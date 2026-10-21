# Database design and correctness

## Tables and access patterns

| Table | Identity and constraints | Purpose |
|---|---|---|
| `jobs` | UUID PK; unique `(task_type,idempotency_key)`; checked queue, delay, status | Durable requests, immutable payload/options, current outcome |
| `job_outbox` | Job UUID PK/FK; partial index on undelivered availability | Delivery intents committed with admission |
| `job_attempts` | Sequence PK; job FK; `(job_id,id)` index | Worker execution, skip, failure, and interruption history |
| `job_effects` | Job UUID PK/FK; JSONB result | One simulated email delivery or image artifact per job |
| `schema_migrations` | Integer version PK | Applied schema versions |

`jobs(created_at DESC,id DESC)` supports paginated history, while `jobs(status)` supports durable-state metrics. There are no cascading deletes: removing identity records could permit duplicate effects. Retention of completed Redis tasks does not remove PostgreSQL history or idempotency keys. Production retention/partitioning needs a deliberate key-expiry contract and is outside this lab.

## Admission transaction

1. Validate HTTP fields and normalize an omitted queue to `default`.
2. Hash the typed payload, task type, queue, and relative delay.
3. Insert the job with `ON CONFLICT DO NOTHING` on the scoped idempotency key.
4. On conflict, read the committed job and compare its request hash. An identical request reuses its ID; a different request returns HTTP 409.
5. For a new job, insert its outbox entry and commit both rows together.

PostgreSQL's unique constraint serializes concurrent reservations under the default READ COMMITTED isolation. No check-then-insert race or Redis lock is used for identity. The key is scoped to task type; a multi-tenant extension must include authenticated tenant identity in that constraint and hash scope.

Timeout/retry/retention defaults are snapshotted into each job. They are excluded from the request hash because an operator changing defaults must not turn a previously identical client retry into a conflict. The original job keeps its original options and original schedule.

## Durable outbox

Every API replica runs a dispatcher. `FOR UPDATE OF o SKIP LOCKED` claims one ready outbox row per transaction. The dispatcher enqueues the job using a stable Redis task ID and acknowledges the outbox row only after Redis accepts it. A Redis task-ID conflict also acknowledges it, since the original message already exists. Failures leave the row undelivered with exponential retry capped at 64 seconds.

If a dispatcher dies after Redis accepts the task but before the PostgreSQL acknowledgment, its row lock is released and a later dispatcher safely repeats the stable task ID. The original processing time is based on the job's creation timestamp plus its delay, so retries do not push its schedule further into the future.

This is an at-least-once bridge, not a distributed transaction. It assumes Redis preserves acknowledged writes. AOF with synchronous fsync reduces the data-loss window, but lost Redis volumes or a failover that discards acknowledged writes still require operational reconciliation from PostgreSQL. An acknowledged outbox entry is not automatically replayed after arbitrary Redis data loss.

## Execution transaction

1. Independently insert a `running` attempt, validating the task ID, type, and JSON payload against PostgreSQL.
2. Start a transaction and lock the job row `FOR UPDATE`.
3. Mark earlier unfinished attempts `interrupted` once a replacement has acquired the lock.
4. A completed or canceled job records a `skipped` attempt and returns without calling the handler.
5. Run the context-aware simulated work and insert one `job_effects` row using the same transaction.
6. Set job completion and attempt completion, then commit together.

`clock_timestamp()` records completion time after processing; transaction-start timestamps would understate latency. A callback failure rolls back its effect, then records the failed attempt and retry/archival state in a separate bounded transaction. If PostgreSQL remains unavailable, failure-history writes can also fail; unfinished attempts are reconciled on subsequent execution. Asynq remains responsible for retry scheduling.

An attempt can remain `running` after a hard crash until redelivery acquires the job lock. The durable job remains `accepted` or `retry` until its effect commits; live execution is observable through attempts, Redis active tasks, and worker in-flight metrics. History describes database outcomes; Redis task state describes transport.

If Asynq archives a lease-expired task after its final allowed retry, there is no subsequent handler invocation to reconcile its unfinished SQL attempt. Inspect Redis archived-task state as well as SQL history in that case; an operator rerun can reconcile the attempt. This lab does not run a general Redis-to-PostgreSQL state reconciler.

## Guarantee and boundary

For this implementation, all durable business effects are PostgreSQL rows:

- At most one effect per job follows from the unique effect key plus the serialized completion check.
- Effect and completion are atomic because they share a transaction.
- A crash before commit rolls both back; redelivery can execute again.
- A crash after commit can trigger another delivery, which skips the handler.
- Cancellation cannot interleave between effect and completion because it updates the same locked job row.

This does **not** make arbitrary external operations exactly once. Before implementing real email or storage calls, pass the job UUID to a provider with durable idempotency support, or use a transactional provider outbox and an idempotent receiver. Holding a SQL transaction around an ordinary SMTP send cannot undo a delivered email after a crash.

Holding the row lock and database connection through the simulated work is an intentional, inspectable lab tradeoff. Long-running production work would generally prepare outside the transaction and finalize bounded database effects inside it. It must still preserve the provider's idempotency/fencing boundary.

## References

- [Asynq delivery and recovery model](https://github.com/hibiken/asynq)
- [PostgreSQL row locks](https://www.postgresql.org/docs/17/explicit-locking.html)
- [PostgreSQL INSERT conflict handling](https://www.postgresql.org/docs/17/sql-insert.html)
