package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/Mei0707/Distributed-Task-Queue/internal/telemetry"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

type QueueInspector interface {
	GetTaskInfo(string, string) (*asynq.TaskInfo, error)
}
type candidate struct {
	id, queue  string
	generation int64
}

// Reconcile releases SQL locks before inspecting Redis, then rechecks the
// observed generation under lock before applying a repair.
func (s *Store) Reconcile(ctx context.Context, inspector QueueInspector, batch int, interval time.Duration) error {
	if batch < 1 || batch > 1000 || interval <= 0 {
		return errors.New("invalid reconciliation batch or interval")
	}
	rows, err := s.Pool.Query(ctx, `WITH candidates AS (
 SELECT o.job_id FROM job_outbox o JOIN jobs j ON j.id=o.job_id
 WHERE o.delivered_at IS NOT NULL AND o.reconcile_at<=clock_timestamp() AND j.status IN ('accepted','retry')
 ORDER BY o.reconcile_at,o.job_id FOR UPDATE OF o SKIP LOCKED LIMIT $1)
 UPDATE job_outbox o SET reconcile_at=clock_timestamp()+$2*interval '1 second'
 FROM candidates c,jobs j WHERE o.job_id=c.job_id AND j.id=o.job_id
 RETURNING o.job_id::text,j.queue,o.generation`, batch, interval.Seconds())
	if err != nil {
		return err
	}
	candidates := make([]candidate, 0, batch)
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.id, &c.queue, &c.generation); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, c)
	}
	rowErr := rows.Err()
	rows.Close()
	if rowErr != nil {
		return rowErr
	}
	var firstErr error
	for _, c := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, inspectErr := inspector.GetTaskInfo(c.queue, TransportID(c.id, c.generation))
		missing := errors.Is(inspectErr, asynq.ErrTaskNotFound) || errors.Is(inspectErr, asynq.ErrQueueNotFound)
		if inspectErr != nil && !missing {
			telemetry.Reconciliations.WithLabelValues("error").Inc()
			if firstErr == nil {
				firstErr = inspectErr
			}
			continue
		}
		if !missing && info.State != asynq.TaskStateArchived && info.State != asynq.TaskStateCompleted {
			telemetry.Reconciliations.WithLabelValues("present").Inc()
			continue
		}
		archived := !missing && info.State == asynq.TaskStateArchived
		outcome, err := s.repair(ctx, c, archived)
		if err != nil {
			telemetry.Reconciliations.WithLabelValues("error").Inc()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		telemetry.Reconciliations.WithLabelValues(outcome).Inc()
	}
	return firstErr
}

func (s *Store) repair(ctx context.Context, c candidate, archived bool) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.Background())
	var status string
	var live, hasOwner bool
	var failures, maxRetry int
	err = tx.QueryRow(ctx, `SELECT status,COALESCE(lease_expires_at>clock_timestamp(),false),lease_owner IS NOT NULL,failure_count,max_retry
 FROM jobs WHERE id=$1 FOR UPDATE`, c.id).Scan(&status, &live, &hasOwner, &failures, &maxRetry)
	if err != nil {
		return "", err
	}
	var generation int64
	var delivered bool
	if err = tx.QueryRow(ctx, "SELECT generation,delivered_at IS NOT NULL FROM job_outbox WHERE job_id=$1 FOR UPDATE", c.id).Scan(&generation, &delivered); err != nil {
		return "", err
	}
	if generation != c.generation || !delivered || status == "completed" || status == "canceled" || status == "archived" {
		return "unchanged", tx.Commit(ctx)
	}
	if live {
		return "live_lease", tx.Commit(ctx)
	}
	if hasOwner {
		failures++
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='interrupted',finished_at=clock_timestamp(),error='execution lease expired during reconciliation' WHERE job_id=$1 AND status='running'", c.id); err != nil {
		return "", err
	}
	state, message, outcome := "retry", "Redis message missing or prematurely completed; replay requested", "replayed"
	if archived || failures > maxRetry {
		state, message, outcome = "archived", "transport archived or durable retry budget exhausted", "archived"
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET status=$2,last_error=$3,failure_count=$4,fence=fence+1,
 lease_owner=NULL,lease_expires_at=NULL WHERE id=$1`, c.id, state, message, failures); err != nil {
		return "", err
	}
	if state != "archived" {
		if _, err = tx.Exec(ctx, `UPDATE job_outbox SET generation=generation+1,delivered_at=NULL,
  available_at=clock_timestamp(),attempts=0,last_error=$2 WHERE job_id=$1`, c.id, message); err != nil {
			return "", err
		}
	}
	return outcome, tx.Commit(ctx)
}

func (s *Store) RunReconciler(ctx context.Context, inspector QueueInspector, batch int, interval time.Duration, log *zap.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		cycleCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := s.Reconcile(cycleCtx, inspector, batch, interval)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Error("Queue reconciliation failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
