package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"github.com/yourname/asynq-task-queue/internal/telemetry"
)

type executionKey struct{}
type execution struct {
	tx pgx.Tx
	id string
}

// RecordEffect must be called by handlers inside the execution transaction.
// Remote providers must instead accept this job ID as their idempotency token.
func RecordEffect(ctx context.Context, kind string, result any) error {
	e, ok := ctx.Value(executionKey{}).(execution)
	if !ok {
		return errors.New("effect attempted outside job transaction")
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = e.tx.Exec(ctx, "INSERT INTO job_effects(job_id,kind,result) VALUES($1,$2,$3)", e.id, kind, data)
	return err
}

func (s *Store) Middleware() asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			id, ok := asynq.GetTaskID(ctx)
			if !ok {
				return fmt.Errorf("missing task ID: %w", asynq.SkipRetry)
			}
			if _, err := uuid.Parse(id); err != nil {
				return fmt.Errorf("invalid task ID: %w", asynq.SkipRetry)
			}
			host, _ := os.Hostname()
			started := time.Now()
			telemetry.InFlight.Inc()
			defer telemetry.InFlight.Dec()
			skipped, err := s.Execute(ctx, id, host, t.Type(), t.Payload(), func(runCtx context.Context) error { return next.ProcessTask(runCtx, t) })
			outcome := "completed"
			if skipped {
				outcome = "skipped"
			}
			if err != nil {
				outcome = "failed"
			}
			telemetry.Executions.WithLabelValues(t.Type(), outcome).Inc()
			telemetry.ExecutionDuration.WithLabelValues(t.Type()).Observe(time.Since(started).Seconds())
			return err
		})
	}
}

// Execute holds a row lock while running a bounded handler. All database effects
// and completion commit together. A dead connection rolls everything back.
func (s *Store) Execute(ctx context.Context, id, worker, kind string, payload []byte, run func(context.Context) error) (skipped bool, err error) {
	var attempt int64
	err = s.Pool.QueryRow(ctx, `INSERT INTO job_attempts(job_id,worker_id)
 SELECT id,$2 FROM jobs WHERE id=$1 AND task_type=$3 AND payload=$4::jsonb RETURNING id`, id, worker, kind, payload).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("unknown job or payload mismatch: %w", asynq.SkipRetry)
	}
	if err != nil {
		return false, err
	}
	// Failures are recorded after rollback using a separate, bounded context.
	defer func() {
		if err != nil {
			failCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, e := s.Pool.Begin(failCtx)
			if e != nil {
				return
			}
			defer tx.Rollback(context.Background())
			retry, _ := asynq.GetRetryCount(ctx)
			maxRetry, _ := asynq.GetMaxRetry(ctx)
			status := "retry"
			if retry >= maxRetry || errors.Is(err, asynq.SkipRetry) {
				status = "archived"
			}
			// Lock jobs first, consistently with the execution path. Never overwrite a
			// concurrently committed completion or cancellation.
			if _, e = tx.Exec(failCtx, "UPDATE jobs SET status=$2,last_error=$3 WHERE id=$1 AND status NOT IN ('completed','canceled')", id, status, err.Error()); e != nil {
				return
			}
			if _, e = tx.Exec(failCtx, "UPDATE job_attempts SET status='failed',error=$2,finished_at=clock_timestamp() WHERE id=$1", attempt, err.Error()); e != nil {
				return
			}
			if e = tx.Commit(failCtx); e != nil {
				err = fmt.Errorf("%w (history write failed: %v)", err, e)
			}
		}
	}()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var status string
	if err = tx.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&status); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='interrupted',finished_at=clock_timestamp(),error='worker stopped before committing' WHERE job_id=$1 AND id<$2 AND status='running'", id, attempt); err != nil {
		return false, err
	}
	if status == "completed" || status == "canceled" {
		if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='skipped',finished_at=clock_timestamp() WHERE id=$1", attempt); err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	if s.hook != nil {
		s.hook("before_effect")
	}
	runCtx := context.WithValue(ctx, executionKey{}, execution{tx: tx, id: id})
	if err = run(runCtx); err != nil {
		return false, err
	}
	if s.hook != nil {
		s.hook("after_effect")
	}
	// Require exactly one observable effect; a handler cannot accidentally mark a
	// job complete without producing its result.
	var count int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM job_effects WHERE job_id=$1", id).Scan(&count); err != nil {
		return false, err
	}
	if count != 1 {
		return false, errors.New("handler did not record an effect")
	}
	if _, err = tx.Exec(ctx, "UPDATE jobs SET status='completed',completed_at=clock_timestamp(),last_error='' WHERE id=$1", id); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='completed',finished_at=clock_timestamp() WHERE id=$1", attempt); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	if s.hook != nil {
		s.hook("after_commit")
	}
	return false, nil
}
