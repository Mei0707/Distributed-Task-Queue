package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Mei0707/Distributed-Task-Queue/internal/telemetry"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
)

var ErrLeaseHeld = errors.New("job has a live execution lease")
var ErrLeaseLost = errors.New("execution lease lost or superseded")
var ErrArchived = fmt.Errorf("job retry budget exhausted: %w", asynq.SkipRetry)

type executionKey struct{}
type execution struct {
	id, owner      string
	fence, attempt int64
	mu             sync.Mutex
	effectKind     string
	result         []byte
}

// RecordEffect stages one result in memory. Only fenced finalization persists it.
// It performs no database or remote-provider operation during the expensive work.
func RecordEffect(ctx context.Context, kind string, result any) error {
	e, ok := ctx.Value(executionKey{}).(*execution)
	if !ok {
		return errors.New("effect attempted outside job execution")
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.result != nil {
		return errors.New("handler already staged an effect")
	}
	e.effectKind, e.result = kind, data
	return nil
}

// FencingToken is monotonic per job. External receivers must enforce it or
// provide durable idempotency keyed by JobID; SQL fencing cannot undo a send.
func FencingToken(ctx context.Context) int64 {
	e, _ := ctx.Value(executionKey{}).(*execution)
	if e == nil {
		return 0
	}
	return e.fence
}
func JobID(ctx context.Context) string {
	e, _ := ctx.Value(executionKey{}).(*execution)
	if e == nil {
		return ""
	}
	return e.id
}
func (s *Store) SetLeaseDuration(d time.Duration) error {
	if d < 3*time.Second {
		return errors.New("execution lease must be at least 3 seconds")
	}
	s.leaseDuration = d
	return nil
}

func (s *Store) Middleware() asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			taskID, ok := asynq.GetTaskID(ctx)
			if !ok {
				return fmt.Errorf("missing task ID: %w", asynq.SkipRetry)
			}
			host, _ := os.Hostname()
			started := time.Now()
			telemetry.InFlight.Inc()
			defer telemetry.InFlight.Dec()
			skipped, err := s.Execute(ctx, taskID, host, t.Type(), t.Payload(), func(runCtx context.Context) error { return next.ProcessTask(runCtx, t) })
			outcome := "completed"
			if skipped {
				outcome = "skipped"
			}
			if err != nil {
				outcome = "failed"
			}
			if errors.Is(err, ErrLeaseHeld) {
				outcome = "busy"
			}
			telemetry.Executions.WithLabelValues(t.Type(), outcome).Inc()
			telemetry.ExecutionDuration.WithLabelValues(t.Type()).Observe(time.Since(started).Seconds())
			return err
		})
	}
}

func (s *Store) claim(ctx context.Context, taskID, worker, kind string, payload []byte) (*execution, bool, error) {
	id, generation, err := ParseTaskID(taskID)
	if err != nil {
		return nil, false, fmt.Errorf("invalid task ID: %w", asynq.SkipRetry)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(context.Background())
	var status string
	var live, hasOwner, matches bool
	var failures, maxRetry int
	err = tx.QueryRow(ctx, `SELECT status,COALESCE(lease_expires_at>clock_timestamp(),false),lease_owner IS NOT NULL,
 failure_count,max_retry,(task_type=$2 AND payload=$3::jsonb) FROM jobs WHERE id=$1 FOR UPDATE`, id, kind, payload).Scan(&status, &live, &hasOwner, &failures, &maxRetry, &matches)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !matches) {
		return nil, false, fmt.Errorf("unknown job or payload mismatch: %w", asynq.SkipRetry)
	}
	if err != nil {
		return nil, false, err
	}
	var current int64
	if err = tx.QueryRow(ctx, "SELECT generation FROM job_outbox WHERE job_id=$1", id).Scan(&current); err != nil {
		return nil, false, err
	}
	if generation != current {
		return nil, true, tx.Commit(ctx)
	}
	if status == "completed" || status == "canceled" || status == "archived" {
		if _, err = tx.Exec(ctx, "INSERT INTO job_attempts(job_id,worker_id,status,finished_at) VALUES($1,$2,'skipped',clock_timestamp())", id, worker); err != nil {
			return nil, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		if status == "archived" {
			return nil, true, ErrArchived
		}
		return nil, true, nil
	}
	if live {
		return nil, false, ErrLeaseHeld
	}
	if hasOwner {
		failures++
		if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='interrupted',finished_at=clock_timestamp(),error='execution lease expired' WHERE job_id=$1 AND status='running'", id); err != nil {
			return nil, false, err
		}
	}
	if failures > maxRetry {
		if _, err = tx.Exec(ctx, "UPDATE jobs SET status='archived',failure_count=$2,fence=fence+1,lease_owner=NULL,lease_expires_at=NULL,last_error='execution lease expired; retry budget exhausted' WHERE id=$1", id, failures); err != nil {
			return nil, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return nil, true, ErrArchived
	}
	e := &execution{id: id, owner: uuid.NewString()}
	err = tx.QueryRow(ctx, `UPDATE jobs SET fence=fence+1,lease_owner=$2,
 lease_expires_at=clock_timestamp()+$3*interval '1 second',failure_count=$4 WHERE id=$1 RETURNING fence`, id, e.owner, s.leaseDuration.Seconds(), failures).Scan(&e.fence)
	if err != nil {
		return nil, false, err
	}
	err = tx.QueryRow(ctx, "INSERT INTO job_attempts(job_id,worker_id,fence) VALUES($1,$2,$3) RETURNING id", id, worker, e.fence).Scan(&e.attempt)
	if err != nil {
		return nil, false, err
	}
	return e, false, tx.Commit(ctx)
}

func (s *Store) renew(ctx context.Context, e *execution) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()+$4*interval '1 second'
 WHERE id=$1 AND lease_owner=$2 AND fence=$3 AND lease_expires_at>clock_timestamp()
 AND status IN ('accepted','retry')`, e.id, e.owner, e.fence, s.leaseDuration.Seconds())
	if err != nil {
		return fmt.Errorf("%w: heartbeat: %v", ErrLeaseLost, err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}
func (s *Store) heartbeat(ctx context.Context, e *execution, cancel context.CancelCauseFunc, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.leaseDuration / 3)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(ctx, s.leaseDuration/3)
			err := s.renew(renewCtx, e)
			renewCancel()
			if err != nil {
				telemetry.LeaseLosses.Inc()
				cancel(err)
				return
			}
		}
	}
}

// Execute uses short claim/heartbeat/finalization transactions. No SQL connection
// or row lock is held while the handler prepares its result.
func (s *Store) Execute(ctx context.Context, taskID, worker, kind string, payload []byte, run func(context.Context) error) (bool, error) {
	e, skipped, err := s.claim(ctx, taskID, worker, kind, payload)
	if err != nil || skipped {
		return skipped, err
	}
	runCtx, cancel := context.WithCancelCause(context.WithValue(ctx, executionKey{}, e))
	defer cancel(nil)
	stop, done := make(chan struct{}), make(chan struct{})
	go s.heartbeat(runCtx, e, cancel, stop, done)
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("handler panic: %v", p)
			}
		}()
		if s.hook != nil {
			s.hook("before_effect")
		}
		err = run(runCtx)
		if err == nil && s.hook != nil {
			s.hook("after_effect")
		}
	}()
	close(stop)
	<-done
	if runCtx.Err() != nil {
		err = context.Cause(runCtx)
	}
	if err == nil {
		err = s.finalize(ctx, e)
	}
	if err != nil {
		failureCtx, failureCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer failureCancel()
		if historyErr := s.fail(failureCtx, e, err); historyErr != nil {
			return false, fmt.Errorf("%w (history write failed: %v)", err, historyErr)
		}
	}
	return false, err
}

func (s *Store) finalize(ctx context.Context, e *execution) error {
	e.mu.Lock()
	kind, data := e.effectKind, append([]byte(nil), e.result...)
	e.mu.Unlock()
	if len(data) == 0 {
		return errors.New("handler did not stage an effect")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status='completed',completed_at=clock_timestamp(),last_error='',lease_owner=NULL,lease_expires_at=NULL
 WHERE id=$1 AND lease_owner=$2 AND fence=$3 AND lease_expires_at>clock_timestamp() AND status IN ('accepted','retry')`, e.id, e.owner, e.fence)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err = tx.Exec(ctx, "INSERT INTO job_effects(job_id,kind,result) VALUES($1,$2,$3)", e.id, kind, data); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='completed',finished_at=clock_timestamp() WHERE id=$1", e.attempt); err != nil {
		return err
	}
	if s.hook != nil {
		s.hook("before_commit")
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if s.hook != nil {
		s.hook("after_commit")
	}
	return nil
}

func (s *Store) fail(ctx context.Context, e *execution, cause error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	terminal := errors.Is(cause, asynq.SkipRetry)
	tag, err := tx.Exec(ctx, `UPDATE jobs SET failure_count=failure_count+1,
 status=CASE WHEN $5 OR failure_count+1>max_retry THEN 'archived' ELSE 'retry' END,
 last_error=$4,lease_owner=NULL,lease_expires_at=NULL
 WHERE id=$1 AND lease_owner=$2 AND fence=$3 AND lease_expires_at>clock_timestamp() AND status IN ('accepted','retry')`, e.id, e.owner, e.fence, cause.Error(), terminal)
	if err != nil {
		return err
	}
	status := "failed"
	if tag.RowsAffected() == 0 {
		status = "interrupted"
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status=$2,error=$3,finished_at=clock_timestamp() WHERE id=$1 AND status='running'", e.attempt, status, cause.Error()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
