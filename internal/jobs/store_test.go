package jobs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	store, err := Open(ctx, u.String(), 24)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		admin.Pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return store
}

func submission(key string) Submission {
	return Submission{Type: "email:deliver", Key: key, Payload: []byte(`{"user_id":42}`), Queue: "default", MaxRetry: 5, TimeoutSeconds: 2, RetentionSeconds: 3600}
}
func accept(t *testing.T, s *Store) Job {
	t.Helper()
	j, _, err := s.Accept(context.Background(), submission(uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func effects(t *testing.T, s *Store, id string) int {
	t.Helper()
	var count int
	if err := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM job_effects WHERE job_id=$1", id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestConcurrentAcceptanceAndConflict(t *testing.T) {
	s := testStore(t)
	in := submission("same-key")
	ids := make(chan string, 32)
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); j, _, err := s.Accept(context.Background(), in); ids <- j.ID; errs <- err }()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("different jobs for same key: %s %s", first, id)
		}
	}
	var outbox int
	s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM job_outbox").Scan(&outbox)
	if outbox != 1 {
		t.Fatalf("outbox rows=%d", outbox)
	}
	in.Payload = []byte(`{"user_id":99}`)
	if _, _, err := s.Accept(context.Background(), in); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	in = submission("same-key")
	in.DelaySeconds = 1
	if _, _, err := s.Accept(context.Background(), in); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected scheduling conflict, got %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
}

func TestConcurrentExecutionHasOneEffect(t *testing.T) {
	s := testStore(t)
	j := accept(t, s)
	var calls atomic.Int32
	var skipped atomic.Int32
	errs := make(chan error, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			duplicate, err := s.Execute(context.Background(), j.ID, "test", "email:deliver", j.Payload, func(ctx context.Context) error {
				calls.Add(1)
				time.Sleep(10 * time.Millisecond)
				return RecordEffect(ctx, "test", map[string]bool{"ok": true})
			})
			if duplicate {
				skipped.Add(1)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || skipped.Load() != 19 || effects(t, s, j.ID) != 1 {
		t.Fatalf("calls=%d skipped=%d effects=%d", calls.Load(), skipped.Load(), effects(t, s, j.ID))
	}
	if err := s.Cancel(context.Background(), j.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("completed job canceled: %v", err)
	}
}

func TestRollbackRetryAndCancellation(t *testing.T) {
	s := testStore(t)
	j := accept(t, s)
	_, err := s.Execute(context.Background(), j.ID, "test", j.Type, j.Payload, func(ctx context.Context) error {
		if err := RecordEffect(ctx, "test", map[string]int{"value": 1}); err != nil {
			return err
		}
		return errors.New("failure after effect")
	})
	if err == nil || effects(t, s, j.ID) != 0 {
		t.Fatal("failed transaction left an effect")
	}
	_, err = s.Execute(context.Background(), j.ID, "replacement", j.Type, j.Payload, func(ctx context.Context) error { return RecordEffect(ctx, "test", map[string]int{"value": 2}) })
	if err != nil || effects(t, s, j.ID) != 1 {
		t.Fatalf("retry: %v", err)
	}
	attempts, err := s.Attempts(context.Background(), j.ID)
	if err != nil || len(attempts) != 2 || attempts[0].Status != "completed" || attempts[1].Status != "failed" {
		t.Fatalf("attempt history: %+v %v", attempts, err)
	}
	j = accept(t, s)
	if err = s.Cancel(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}
	skipped, err := s.Execute(context.Background(), j.ID, "test", j.Type, j.Payload, func(context.Context) error { t.Error("canceled job executed"); return nil })
	if !skipped || err != nil || effects(t, s, j.ID) != 0 {
		t.Fatalf("cancel skipped=%v err=%v", skipped, err)
	}
	_, err = s.Execute(context.Background(), j.ID, "test", j.Type, []byte(`{"user_id":99}`), func(context.Context) error { return nil })
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("payload mismatch accepted: %v", err)
	}
}

type brokenRedis struct{}

func (brokenRedis) EnqueueContext(context.Context, *asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error) {
	return nil, errors.New("Redis unavailable")
}

func TestOutboxSurvivesRedisFailureAndLostAcknowledgment(t *testing.T) {
	s := testStore(t)
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR")
	}
	ctx := context.Background()
	j := accept(t, s)
	if _, err := s.DispatchOne(ctx, brokenRedis{}); err != nil {
		t.Fatal(err)
	}
	var delivered *time.Time
	var attempts int
	if err := s.Pool.QueryRow(ctx, "SELECT delivered_at,attempts FROM job_outbox WHERE job_id=$1", j.ID).Scan(&delivered, &attempts); err != nil {
		t.Fatal(err)
	}
	if delivered != nil || attempts != 1 {
		t.Fatalf("failed dispatch acknowledged: %v %d", delivered, attempts)
	}
	s.Pool.Exec(ctx, "UPDATE job_outbox SET available_at=now() WHERE job_id=$1", j.ID)
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr, DB: 15})
	defer client.Close()
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr, DB: 15})
	defer inspector.Close()
	defer inspector.DeleteTask("default", j.ID)
	s.hook = func(phase string) {
		if phase == "after_enqueue" {
			panic("crash after enqueue")
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("hook did not execute")
			}
		}()
		s.DispatchOne(ctx, client)
	}()
	s.hook = nil
	if _, err := s.DispatchOne(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool.QueryRow(ctx, "SELECT delivered_at FROM job_outbox WHERE job_id=$1", j.ID).Scan(&delivered); err != nil || delivered == nil {
		t.Fatalf("replay not acknowledged: %v", err)
	}
	info, err := inspector.GetTaskInfo("default", j.ID)
	if err != nil || info.ID != j.ID {
		t.Fatalf("stable task ID lost: %v", err)
	}
}

// The helper runs in a real OS process and a real Asynq worker. The parent kills
// it with SIGKILL; no graceful cleanup runs in the doomed worker.
func TestWorkerProcess(t *testing.T) {
	if os.Getenv("QUEUE_TEST_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	s, err := Open(ctx, os.Getenv("QUEUE_CHILD_DSN"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	phase := os.Getenv("QUEUE_CHILD_PHASE")
	s.hook = func(current string) {
		if current == phase {
			fmt.Println("FAILPOINT_READY")
			select {}
		}
	}
	server := recoveryServer(t, s, os.Getenv("TEST_REDIS_ADDR"), mustInt(os.Getenv("QUEUE_CHILD_DB")))
	if err = server.Start(recoveryHandler(s)); err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown()
	select {}
}

func mustInt(s string) int { var n int; fmt.Sscan(s, &n); return n }
func recoveryHandler(s *Store) asynq.Handler {
	mux := asynq.NewServeMux()
	mux.Use(s.Middleware())
	mux.HandleFunc("email:deliver", func(ctx context.Context, _ *asynq.Task) error {
		return RecordEffect(ctx, "test_delivery", map[string]bool{"ok": true})
	})
	return mux
}
func recoveryServer(t *testing.T, s *Store, addr string, db int) *asynq.Server {
	t.Helper()
	return asynq.NewServer(asynq.RedisClientOpt{Addr: addr, DB: db}, asynq.Config{Concurrency: 2, Queues: map[string]int{"default": 1}, ShutdownTimeout: time.Second, DelayedTaskCheckInterval: 100 * time.Millisecond, RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return 100 * time.Millisecond }})
}

func TestSIGKILLRecovery(t *testing.T) {
	if os.Getenv("TEST_REDIS_ADDR") == "" {
		t.Skip("set TEST_REDIS_ADDR to run crash tests")
	}
	for i, phase := range []string{"before_effect", "after_effect", "after_commit"} {
		i, phase := i, phase
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s := testStore(t)
			ctx := context.Background()
			j := accept(t, s)
			db := 10 + i
			addr := os.Getenv("TEST_REDIS_ADDR")
			opt := asynq.RedisClientOpt{Addr: addr, DB: db}
			client := asynq.NewClient(opt)
			defer client.Close()
			inspector := asynq.NewInspector(opt)
			defer inspector.Close()
			defer inspector.DeleteTask("default", j.ID)
			if _, err := s.DispatchOne(ctx, client); err != nil {
				t.Fatal(err)
			}
			cfg := s.Pool.Config()
			dsn := cfg.ConnString()
			child := exec.Command(os.Args[0], "-test.run=^TestWorkerProcess$", "-test.v")
			child.Env = append(os.Environ(), "QUEUE_TEST_CHILD=1", "QUEUE_CHILD_DSN="+dsn, "QUEUE_CHILD_PHASE="+phase, fmt.Sprintf("QUEUE_CHILD_DB=%d", db))
			stdout, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			child.Stderr = os.Stderr
			if err = child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { child.Process.Kill() }()
			ready := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if strings.Contains(scanner.Text(), "FAILPOINT_READY") {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					t.Fatal("worker exited before reaching failpoint")
				}
			case <-time.After(20 * time.Second):
				t.Fatal("worker did not reach failpoint")
			}
			killedAt := time.Now()
			if err = child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			child.Wait()
			expected := 0
			if phase == "after_commit" {
				expected = 1
			}
			if count := effects(t, s, j.ID); count != expected {
				t.Fatalf("effects immediately after SIGKILL=%d want %d", count, expected)
			}
			replacement := recoveryServer(t, s, addr, db)
			if err = replacement.Start(recoveryHandler(s)); err != nil {
				t.Fatal(err)
			}
			defer replacement.Shutdown()
			deadline := time.Now().Add(180 * time.Second)
			for time.Now().Before(deadline) {
				info, e := inspector.GetTaskInfo("default", j.ID)
				if e == nil && info.State == asynq.TaskStateCompleted {
					break
				}
				time.Sleep(250 * time.Millisecond)
			}
			info, err := inspector.GetTaskInfo("default", j.ID)
			if err != nil || info.State != asynq.TaskStateCompleted {
				t.Fatalf("job did not recover: info=%+v err=%v", info, err)
			}
			if count := effects(t, s, j.ID); count != 1 {
				t.Fatalf("effects after recovery=%d", count)
			}
			got, err := s.Get(ctx, j.ID)
			if err != nil || got.Status != "completed" {
				t.Fatalf("durable job=%+v err=%v", got, err)
			}
			attempts, err := s.Attempts(ctx, j.ID)
			if err != nil || len(attempts) < 2 {
				t.Fatalf("no redelivery history: %+v %v", attempts, err)
			}
			if phase == "after_commit" {
				if attempts[0].Status != "skipped" || attempts[1].Status != "completed" {
					t.Fatalf("ack-loss history: %+v", attempts)
				}
			} else if attempts[0].Status != "completed" || attempts[1].Status != "interrupted" {
				t.Fatalf("rollback history: %+v", attempts)
			}
			t.Logf("SIGKILL phase=%s recovery=%s effects=1 attempts=%d", phase, time.Since(killedAt).Round(time.Millisecond), len(attempts))
		})
	}
}
