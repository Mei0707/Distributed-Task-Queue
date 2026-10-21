package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

func TestLongWorkReleasesConnectionsAndRenewsLease(t *testing.T) {
	s := testStore(t)
	s.leaseDuration = 300 * time.Millisecond
	j := accept(t, s)
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := s.Execute(context.Background(), j.ID, "long-worker", j.Type, j.Payload, func(ctx context.Context) error {
			if FencingToken(ctx) < 1 || JobID(ctx) != j.ID {
				return errors.New("missing execution identity")
			}
			close(entered)
			<-ctx.Done()
			return context.Cause(ctx)
		})
		done <- err
	}()
	<-entered
	// Several complete lease periods pass while the handler does no SQL work.
	time.Sleep(800 * time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for s.Pool.Stat().AcquiredConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Pool.Stat().AcquiredConns() != 0 {
		t.Error("handler holds a database connection")
	}
	_, _, err := s.claim(context.Background(), j.ID, "contender", j.Type, j.Payload)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Errorf("live renewed lease stolen: %v", err)
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err = s.Cancel(cancelCtx, j.ID); err != nil {
		t.Errorf("cancellation blocked on long work: %v", err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("canceled holder: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lease revocation did not cancel work")
	}
	if effects(t, s, j.ID) != 0 {
		t.Fatal("canceled computation committed an effect")
	}
}

func TestExpiredWorkerCannotFinalizeAfterReplacement(t *testing.T) {
	s := testStore(t)
	j := accept(t, s)
	ctx := context.Background()
	old, _, err := s.claim(ctx, j.ID, "old", j.Type, j.Payload)
	if err != nil {
		t.Fatal(err)
	}
	oldCtx := context.WithValue(ctx, executionKey{}, old)
	if err = RecordEffect(oldCtx, "stale", map[string]string{"owner": "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	fresh, _, err := s.claim(ctx, j.ID, "replacement", j.Type, j.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.fence <= old.fence {
		t.Fatal("fence did not advance")
	}
	if err = s.renew(ctx, old); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker renewed: %v", err)
	}
	if err = s.finalize(ctx, old); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale result committed: %v", err)
	}
	if err = s.fail(ctx, old, errors.New("late failure")); err != nil {
		t.Fatal(err)
	}
	var owner string
	if err = s.Pool.QueryRow(ctx, "SELECT lease_owner::text FROM jobs WHERE id=$1", j.ID).Scan(&owner); err != nil || owner != fresh.owner {
		t.Fatalf("stale failure changed ownership: %s %v", owner, err)
	}
	freshCtx := context.WithValue(ctx, executionKey{}, fresh)
	if err = RecordEffect(freshCtx, "fresh", map[string]string{"owner": "replacement"}); err != nil {
		t.Fatal(err)
	}
	if err = s.finalize(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if effects(t, s, j.ID) != 1 {
		t.Fatal("replacement did not produce exactly one effect")
	}
	var kind string
	s.Pool.QueryRow(ctx, "SELECT kind FROM job_effects WHERE job_id=$1", j.ID).Scan(&kind)
	if kind != "fresh" {
		t.Fatal("wrong worker's effect committed")
	}
}

func TestDurableRetryBudgetAndExplicitRequeue(t *testing.T) {
	s := testStore(t)
	in := submission(uuid.NewString())
	in.MaxRetry = 0
	j, _, err := s.Accept(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Execute(context.Background(), j.ID, "first", j.Type, j.Payload, func(context.Context) error { return errors.New("permanent failure") })
	if err == nil {
		t.Fatal("expected failure")
	}
	_, err = s.Execute(context.Background(), j.ID, "duplicate", j.Type, j.Payload, func(context.Context) error { t.Error("archived job executed"); return nil })
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("archival ignored: %v", err)
	}
	if err = s.Requeue(context.Background(), j.ID, j.Queue); err != nil {
		t.Fatal(err)
	}
	skipped, err := s.Execute(context.Background(), j.ID, "stale-delivery", j.Type, j.Payload, func(context.Context) error { t.Error("obsolete generation executed"); return nil })
	if err != nil || !skipped {
		t.Fatalf("obsolete delivery: %v %v", skipped, err)
	}
	_, err = s.Execute(context.Background(), TransportID(j.ID, 2), "operator-retry", j.Type, j.Payload, func(ctx context.Context) error { return RecordEffect(ctx, "retry", true) })
	if err != nil {
		t.Fatal(err)
	}
	if effects(t, s, j.ID) != 1 {
		t.Fatal("operator retry failed")
	}
}

func TestMigrateVersionOnePreservesHistory(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, "DROP TABLE job_effects,job_attempts,job_outbox,jobs; DELETE FROM schema_migrations WHERE version=2"); err != nil {
		t.Fatal(err)
	}
	sql, err := migrations.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	_, err = s.Pool.Exec(ctx, `INSERT INTO jobs(id,task_type,idempotency_key,request_hash,payload,queue,delay_seconds,max_retry,timeout_seconds,retention_seconds)
 VALUES($1,'email:deliver','legacy','hash','{"user_id":42}','default',600,5,30,3600)`, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "INSERT INTO job_outbox(job_id,available_at,delivered_at) VALUES($1,now(),now());", id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "INSERT INTO job_attempts(job_id,worker_id,status) VALUES($1,'old-worker','failed')", id); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := s.Get(ctx, id)
	if err != nil || j.Failures != 1 {
		t.Fatalf("legacy history lost: %+v %v", j, err)
	}
	var delay float64
	var generation int64
	if err = s.Pool.QueryRow(ctx, "SELECT extract(epoch FROM j.process_at-j.created_at),o.generation FROM jobs j JOIN job_outbox o ON o.job_id=j.id WHERE j.id=$1", id).Scan(&delay, &generation); err != nil || delay != 600 || generation != 1 {
		t.Fatalf("legacy schedule/identity changed: %v %v %v", delay, generation, err)
	}
}
