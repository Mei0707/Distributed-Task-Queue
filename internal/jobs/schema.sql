-- Version 1. The migration runner serializes schema changes using an advisory lock.
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    task_type TEXT NOT NULL CHECK (task_type IN ('email:deliver', 'image:resize')),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    queue TEXT NOT NULL CHECK (queue IN ('critical', 'default', 'low')),
    delay_seconds INTEGER NOT NULL CHECK (delay_seconds BETWEEN 0 AND 604800),
    max_retry INTEGER NOT NULL CHECK (max_retry >= 0),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
    retention_seconds INTEGER NOT NULL CHECK (retention_seconds > 0),
    status TEXT NOT NULL DEFAULT 'accepted' CHECK (status IN ('accepted','retry','completed','archived','canceled')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (task_type, idempotency_key)
);
CREATE INDEX jobs_history_idx ON jobs (created_at DESC, id DESC);
CREATE INDEX jobs_status_idx ON jobs (status);

CREATE TABLE job_outbox (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    available_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX job_outbox_ready_idx ON job_outbox (available_at) WHERE delivered_at IS NULL;

CREATE TABLE job_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running','completed','failed','skipped','interrupted')),
    error TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX job_attempts_job_idx ON job_attempts (job_id, id);

-- These are the observable effects of the simulated handlers. The unique key and
-- shared transaction make the effect and job completion atomic.
CREATE TABLE job_effects (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    kind TEXT NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
