package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Mei0707/Distributed-Task-Queue/internal/telemetry"
)

type Enqueuer interface {
	EnqueueContext(context.Context, *asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error)
}

// DispatchOne keeps the outbox row locked until enqueueing is acknowledged.
// Crashing after Redis accepts but before COMMIT safely repeats the same task ID.
func (s *Store) DispatchOne(ctx context.Context, client Enqueuer) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var id, kind, queue string
	var payload []byte
	var retry, timeout, retention, attempts int
	var processAt time.Time
	var generation int64
	err = tx.QueryRow(ctx, `SELECT j.id::text,j.task_type,j.payload,j.queue,j.max_retry,j.timeout_seconds,j.retention_seconds,
 j.process_at,o.attempts,o.generation
 FROM job_outbox o JOIN jobs j ON j.id=o.job_id
 WHERE o.delivered_at IS NULL AND o.available_at<=now()
 ORDER BY o.available_at FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&id, &kind, &payload, &queue, &retry, &timeout, &retention, &processAt, &attempts, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	enqueueCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, enqueueErr := client.EnqueueContext(enqueueCtx, asynq.NewTask(kind, payload), asynq.TaskID(TransportID(id, generation)), asynq.Queue(queue), asynq.ProcessAt(processAt), asynq.MaxRetry(retry), asynq.Timeout(time.Duration(timeout)*time.Second), asynq.Retention(time.Duration(retention)*time.Second))
	if enqueueErr == nil || errors.Is(enqueueErr, asynq.ErrTaskIDConflict) {
		if s.hook != nil {
			s.hook("after_enqueue")
		}
		_, err = tx.Exec(ctx, "UPDATE job_outbox SET delivered_at=now(),attempts=attempts+1,last_error='' WHERE job_id=$1", id)
	} else {
		telemetry.OutboxFailures.Inc()
		delay := time.Second * time.Duration(1<<uint(minInt(attempts, 6)))
		_, err = tx.Exec(ctx, "UPDATE job_outbox SET attempts=attempts+1,last_error=$2,available_at=now()+$3*interval '1 second' WHERE job_id=$1", id, enqueueErr.Error(), delay.Seconds())
	}
	if err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *Store) RunDispatcher(ctx context.Context, client Enqueuer, log *zap.Logger) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for i := 0; i < 100; i++ {
			ok, err := s.DispatchOne(ctx, client)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("Outbox dispatch failed", zap.Error(err))
				}
				break
			}
			if !ok {
				break
			}
		}
	}
}
