package jobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type inspectFunc func(string, string) (*asynq.TaskInfo, error)

func (f inspectFunc) GetTaskInfo(q, id string) (*asynq.TaskInfo, error) { return f(q, id) }

func TestRedisDataLossReplaysOnlyUnfinishedJobs(t *testing.T) {
	s := testStore(t)
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR")
	}
	ctx := context.Background()
	// DB 13 is exclusively reserved for this suite. Refuse to flush existing data.
	redisClient := redis.NewClient(&redis.Options{Addr: addr, DB: 13})
	t.Cleanup(func() { redisClient.Close() })
	size, err := redisClient.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatal("Redis test DB 13 must be empty; refusing destructive data-loss test")
	}
	t.Cleanup(func() {
		if err := redisClient.FlushDB(context.Background()).Err(); err != nil {
			t.Error(err)
		}
	})
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr, DB: 13})
	defer client.Close()
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr, DB: 13})
	defer inspector.Close()
	lost, completed, canceled, live, expired := accept(t, s), accept(t, s), accept(t, s), accept(t, s), accept(t, s)
	in := submission(uuid.NewString())
	in.DelaySeconds = 600
	scheduled, _, err := s.Accept(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	in = submission(uuid.NewString())
	in.MaxRetry = 0
	archived, _, err := s.Accept(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	for {
		ok, err := s.DispatchOne(ctx, client)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	_, err = s.Execute(ctx, completed.ID, "completed", completed.Type, completed.Payload, func(ctx context.Context) error { return RecordEffect(ctx, "test", true) })
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Cancel(ctx, canceled.ID); err != nil {
		t.Fatal(err)
	}
	_, err = s.Execute(ctx, archived.ID, "failed", archived.Type, archived.Payload, func(context.Context) error { return errors.New("permanent failure") })
	if err == nil {
		t.Fatal("expected failed job")
	}
	holder, _, err := s.claim(ctx, live.ID, "live", live.Type, live.Payload)
	if err != nil {
		t.Fatal(err)
	}
	stale, _, err := s.claim(ctx, expired.ID, "dead", expired.Type, expired.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", expired.ID); err != nil {
		t.Fatal(err)
	}
	if err = redisClient.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err = s.Reconcile(ctx, inspector, 100, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{lost.ID, scheduled.ID, expired.ID} {
		var generation int64
		var delivered bool
		if err = s.Pool.QueryRow(ctx, "SELECT generation,delivered_at IS NOT NULL FROM job_outbox WHERE job_id=$1", id).Scan(&generation, &delivered); err != nil || generation != 2 || delivered {
			t.Fatalf("lost job not rearmed: %s gen=%d delivered=%v %v", id, generation, delivered, err)
		}
	}
	var liveGeneration int64
	s.Pool.QueryRow(ctx, "SELECT generation FROM job_outbox WHERE job_id=$1", live.ID).Scan(&liveGeneration)
	if liveGeneration != 1 {
		t.Fatal("live execution was replayed")
	}
	dispatched := 0
	for {
		ok, err := s.DispatchOne(ctx, client)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		dispatched++
	}
	if dispatched != 3 {
		t.Fatalf("replayed %d jobs, want 3", dispatched)
	}
	info, err := inspector.GetTaskInfo("default", TransportID(scheduled.ID, 2))
	if err != nil || info.State != asynq.TaskStateScheduled {
		t.Fatalf("schedule lost: %+v %v", info, err)
	}
	var original time.Time
	s.Pool.QueryRow(ctx, "SELECT process_at FROM jobs WHERE id=$1", scheduled.ID).Scan(&original)
	if info.NextProcessAt.Sub(original).Abs() > time.Second {
		t.Fatal("scheduled execution time changed during replay")
	}
	for _, j := range []Job{completed, canceled, archived} {
		for _, gen := range []int64{1, 2} {
			if _, err = inspector.GetTaskInfo(j.Queue, TransportID(j.ID, gen)); !errors.Is(err, asynq.ErrTaskNotFound) {
				t.Fatalf("terminal job resurrected: %s %v", j.ID, err)
			}
		}
	}
	// A healthy computation can finish even though its Redis receipt was lost.
	if err = RecordEffect(context.WithValue(ctx, executionKey{}, holder), "live", true); err != nil {
		t.Fatal(err)
	}
	if err = s.finalize(ctx, holder); err != nil {
		t.Fatal(err)
	}
	if err = RecordEffect(context.WithValue(ctx, executionKey{}, stale), "stale", true); err != nil {
		t.Fatal(err)
	}
	if err = s.finalize(ctx, stale); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired owner finalized: %v", err)
	}
	calls := 0
	skipped, err := s.Execute(ctx, expired.ID, "old-transport", expired.Type, expired.Payload, func(context.Context) error { calls++; return nil })
	if !skipped || err != nil || calls != 0 {
		t.Fatal("obsolete transport executed after Redis recovery")
	}
	_, err = s.Execute(ctx, TransportID(expired.ID, 2), "replacement", expired.Type, expired.Payload, func(ctx context.Context) error { return RecordEffect(ctx, "replacement", true) })
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Execute(ctx, TransportID(lost.ID, 2), "replayed", lost.Type, lost.Payload, func(ctx context.Context) error { return RecordEffect(ctx, "replayed", true) })
	if err != nil {
		t.Fatal(err)
	}
	if effects(t, s, expired.ID) != 1 || effects(t, s, lost.ID) != 1 || effects(t, s, live.ID) != 1 || effects(t, s, completed.ID) != 1 {
		t.Fatal("recovery effect counts incorrect")
	}
	t.Log("Redis FLUSHDB: 3 unfinished jobs replayed, live lease preserved, terminal jobs excluded; one effect per completed job")
}

func TestReconciliationErrorsAndStaleObservations(t *testing.T) {
	s := testStore(t)
	j := accept(t, s)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, "UPDATE job_outbox SET delivered_at=now() WHERE job_id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	outage := inspectFunc(func(string, string) (*asynq.TaskInfo, error) { return nil, errors.New("Redis connection refused") })
	if err := s.Reconcile(ctx, outage, 10, time.Nanosecond); err == nil {
		t.Fatal("Redis outage silently treated as missing")
	}
	var gen int64
	var delivered bool
	s.Pool.QueryRow(ctx, "SELECT generation,delivered_at IS NOT NULL FROM job_outbox WHERE job_id=$1", j.ID).Scan(&gen, &delivered)
	if gen != 1 || !delivered {
		t.Fatal("connection error caused replay")
	}
	// Completion races with an authoritative missing-task observation. No SQL
	// connection may remain held while the Redis adapter is called.
	raced := inspectFunc(func(string, string) (*asynq.TaskInfo, error) {
		if s.Pool.Stat().AcquiredConns() != 0 {
			t.Error("SQL connection held during Redis inspection")
		}
		_, err := s.Execute(ctx, j.ID, "winner", j.Type, j.Payload, func(ctx context.Context) error { return RecordEffect(ctx, "winner", true) })
		if err != nil {
			t.Error(err)
		}
		return nil, asynq.ErrTaskNotFound
	})
	if err := s.Reconcile(ctx, raced, 10, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	s.Pool.QueryRow(ctx, "SELECT generation,delivered_at IS NOT NULL FROM job_outbox WHERE job_id=$1", j.ID).Scan(&gen, &delivered)
	if gen != 1 || !delivered {
		t.Fatal("stale observation resurrected completed job")
	}
}

func TestConcurrentReconciliationIsBoundedAndIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		j := accept(t, s)
		if _, err := s.Pool.Exec(ctx, "UPDATE job_outbox SET delivered_at=now() WHERE job_id=$1", j.ID); err != nil {
			t.Fatal(err)
		}
	}
	absent := inspectFunc(func(string, string) (*asynq.TaskInfo, error) { return nil, asynq.ErrQueueNotFound })
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Reconcile(ctx, absent, 2, time.Hour) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var replayed int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM job_outbox WHERE generation=2 AND delivered_at IS NULL").Scan(&replayed); err != nil || replayed != 7 {
		t.Fatalf("replica reconciliation lost/doubled work: %d %v", replayed, err)
	}
	var bad int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM job_outbox WHERE generation<>2").Scan(&bad)
	if bad != 0 {
		t.Fatal("generation advanced more than once")
	}
}

func TestFinalCrashArchivalIsReconciledAndNotResurrected(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	in := submission(uuid.NewString())
	in.MaxRetry = 0
	j, _, err := s.Accept(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.claim(ctx, j.ID, "crashed", j.Type, j.Payload); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE job_outbox SET delivered_at=now() WHERE job_id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	archived := inspectFunc(func(string, string) (*asynq.TaskInfo, error) {
		return &asynq.TaskInfo{State: asynq.TaskStateArchived}, nil
	})
	if err = s.Reconcile(ctx, archived, 10, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil || got.Status != "archived" || got.Failures != 1 {
		t.Fatalf("terminal crash missing from history: %+v %v", got, err)
	}
	attempts, err := s.Attempts(ctx, j.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Status != "interrupted" {
		t.Fatalf("interrupted history: %+v %v", attempts, err)
	}
	if err = s.Reconcile(ctx, inspectFunc(func(string, string) (*asynq.TaskInfo, error) {
		t.Error("terminal job inspected")
		return nil, fmt.Errorf("%w", asynq.ErrTaskNotFound)
	}), 10, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
}
