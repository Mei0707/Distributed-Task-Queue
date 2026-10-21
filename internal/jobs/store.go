package jobs

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var migrations embed.FS

var ErrConflict = errors.New("idempotency key already used for a different request")
var ErrNotFound = errors.New("job not found")

type Store struct {
	Pool *pgxpool.Pool
	// Tests in this package use hooks to stop a real worker at transaction boundaries.
	hook func(string)
}

func Open(ctx context.Context, dsn string, maxConnections int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConnections
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(739142001)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=1)").Scan(&exists); err != nil {
		return err
	}
	if !exists {
		sql, _ := migrations.ReadFile("schema.sql")
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES(1)"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

type Submission struct {
	Type             string
	Key              string
	Payload          json.RawMessage
	Queue            string
	DelaySeconds     int
	MaxRetry         int
	TimeoutSeconds   int
	RetentionSeconds int
}

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Queue       string          `json:"queue"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	LastError   string          `json:"last_error"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

const jobColumns = "id::text, task_type, queue, status, payload, last_error, created_at, completed_at"

func scanJob(row pgx.Row) (j Job, err error) {
	err = row.Scan(&j.ID, &j.Type, &j.Queue, &j.Status, &j.Payload, &j.LastError, &j.CreatedAt, &j.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return
}

// Accept atomically reserves an idempotency key, records a job, and creates its
// outbox entry. Concurrent identical requests return the same committed job.
func (s *Store) Accept(ctx context.Context, in Submission) (Job, bool, error) {
	if in.Key == "" || len(in.Key) > 200 {
		return Job{}, false, fmt.Errorf("idempotency key must contain 1 to 200 bytes")
	}
	canonical, err := json.Marshal(struct {
		Type    string
		Payload json.RawMessage
		Queue   string
		Delay   int
	}{in.Type, in.Payload, in.Queue, in.DelaySeconds})
	if err != nil {
		return Job{}, false, err
	}
	digest := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(digest[:])
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(context.Background())
	id := uuid.NewString()
	tag, err := tx.Exec(ctx, `INSERT INTO jobs(id,task_type,idempotency_key,request_hash,payload,queue,delay_seconds,max_retry,timeout_seconds,retention_seconds)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(task_type,idempotency_key) DO NOTHING`, id, in.Type, in.Key, fingerprint, []byte(in.Payload), in.Queue, in.DelaySeconds, in.MaxRetry, in.TimeoutSeconds, in.RetentionSeconds)
	if err != nil {
		return Job{}, false, err
	}
	replay := tag.RowsAffected() == 0
	if replay {
		var hash string
		if err = tx.QueryRow(ctx, "SELECT id::text,request_hash FROM jobs WHERE task_type=$1 AND idempotency_key=$2", in.Type, in.Key).Scan(&id, &hash); err != nil {
			return Job{}, false, err
		}
		if hash != fingerprint {
			return Job{}, false, ErrConflict
		}
	} else {
		if _, err = tx.Exec(ctx, "INSERT INTO job_outbox(job_id,available_at) VALUES($1,now())", id); err != nil {
			return Job{}, false, err
		}
	}
	j, err := scanJob(tx.QueryRow(ctx, "SELECT "+jobColumns+" FROM jobs WHERE id=$1", id))
	if err != nil {
		return Job{}, false, err
	}
	return j, replay, tx.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	return scanJob(s.Pool.QueryRow(ctx, "SELECT "+jobColumns+" FROM jobs WHERE id=$1", id))
}

func (s *Store) List(ctx context.Context, limit, offset int) ([]Job, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+jobColumns+" FROM jobs ORDER BY created_at DESC,id DESC LIMIT $1 OFFSET $2", limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Job, 0)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

type Attempt struct {
	ID         int64      `json:"id"`
	Worker     string     `json:"worker_id"`
	Status     string     `json:"status"`
	Error      string     `json:"error"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func (s *Store) Attempts(ctx context.Context, id string) ([]Attempt, error) {
	rows, err := s.Pool.Query(ctx, "SELECT id,worker_id,status,error,started_at,finished_at FROM job_attempts WHERE job_id=$1 ORDER BY id DESC LIMIT 100", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Attempt, 0)
	for rows.Next() {
		var a Attempt
		if err = rows.Scan(&a.ID, &a.Worker, &a.Status, &a.Error, &a.StartedAt, &a.FinishedAt); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

// Cancel serializes against execution. A committed effect always wins over a
// later cancellation; canceled jobs are acknowledged without executing.
func (s *Store) Cancel(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx, "UPDATE jobs SET status='canceled' WHERE id=$1 AND status NOT IN ('completed','canceled')", id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return err
}
