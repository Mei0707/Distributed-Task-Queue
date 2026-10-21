-- Preserve version 1 and existing job identity/history during the upgrade.
ALTER TABLE jobs ADD COLUMN fence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN lease_owner UUID;
ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0);
ALTER TABLE jobs ADD COLUMN process_at TIMESTAMPTZ;
UPDATE jobs SET process_at = created_at + delay_seconds * interval '1 second';
ALTER TABLE jobs ALTER COLUMN process_at SET NOT NULL;
ALTER TABLE job_attempts ADD COLUMN fence BIGINT NOT NULL DEFAULT 0;
-- Stop old workers first: unfinished legacy attempts have no renewable lease.
UPDATE job_attempts SET status='interrupted',finished_at=clock_timestamp(),error='legacy execution interrupted during lease upgrade' WHERE status='running';
UPDATE jobs j SET failure_count=(SELECT count(*) FROM job_attempts a WHERE a.job_id=j.id AND a.status IN ('failed','interrupted'));
ALTER TABLE job_outbox ADD COLUMN generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0);
ALTER TABLE job_outbox ADD COLUMN reconcile_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX job_outbox_reconcile_idx ON job_outbox (reconcile_at) WHERE delivered_at IS NOT NULL;
