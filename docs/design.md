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

## Delivery and reconciliation

Each API replica runs an outbox dispatcher and a reconciler. The dispatcher uses `FOR UPDATE OF o SKIP LOCKED`, enqueues a stable transport ID, and marks delivery only after Redis accepts it. Its Redis request is bounded to two seconds; failures retain the outbox row and back off up to 64 seconds.

Job identity and transport identity are separate. The first generation keeps the original job UUID for compatibility; later generations use `UUID:2`, `UUID:3`, and so on. Repeating an uncertain dispatch uses the same generation and Redis task-ID conflict handling. Replaying a lost delivery advances the generation. An acknowledgment from an old worker then refers to a different Redis ID and cannot remove the replacement message. Workers also reject obsolete generations before running the handler.

The reconciler compares delivered, nonterminal PostgreSQL jobs with their current Redis messages. A short SQL statement claims a fair, bounded batch by advancing `reconcile_at`; it releases its connection before making Redis calls. Each repair locks the job and outbox briefly and rechecks generation, delivery acknowledgment, status, and execution lease. This protects against concurrent dispatchers, reconcilers, completions, cancellations, and operator retries.

- A definitive missing task/queue, or a prematurely completed Redis message, rearms unfinished work in a fresh generation.
- A live SQL execution lease prevents replay even if Redis has lost its receipt: that computation may still finish successfully.
- Missing work with an expired lease is fenced off; its attempt becomes interrupted and consumes one durable failure before replay.
- Redis connection/inspection errors never count as proof that a task is missing.
- PostgreSQL completed, canceled, and archived jobs are excluded and cannot be resurrected by Redis loss.
- An archived Redis message synchronizes unfinished SQL history once no live execution lease remains. This covers a final-retry crash that never calls the handler again.

Replays preserve the absolute `process_at` schedule. An explicit operator Run-now action starts a new generation, moves `process_at` to the current time, and resets the durable retry budget; it cannot override a live lease or a completed/canceled job. Changes to that execution schedule do not change the original request hash or idempotency identity.

The defaults are 50 inspections per one-second loop and a 15-second minimum interval per job. Large backlogs or Redis timeouts increase the time to sweep all candidates. A failed/timed-out cycle leaves candidates eligible again at their next check time. No finite recovery time is promised while PostgreSQL/Redis remain unavailable. Completed transport records are not rebuilt after Redis loss; durable history/results remain available through PostgreSQL.

## Renewable execution leases and fencing

Execution has three phases:

1. **Claim:** in a short transaction, validate the durable job/type/payload and current delivery generation, lock the job, reject a live lease, and allocate a random owner plus a monotonically increasing fence. Record the attempt and commit.
2. **Prepare:** run expensive work without an open SQL transaction, row lock, or reserved pool connection. Heartbeats renew ownership every third of the lease duration (30 seconds by default). `RecordEffect` stages one JSON result in memory. It performs no database or provider side effect.
3. **Finalize:** in a short transaction, conditionally update the job only if owner/fence still match, the lease remains valid, and the status is nonterminal. Insert the effect and complete the attempt in that same transaction. Commit atomically.

Lease comparisons use PostgreSQL `clock_timestamp()` rather than worker clocks or transaction-start timestamps. An expired lease cannot renew itself. Renewal failure cancels the handler context. A handler that ignores cancellation may continue computing, but its expired/superseded fence cannot finalize. Stale failure reporting also cannot overwrite a replacement's ownership or committed completion.

Cancellation revokes the lease and advances the fence immediately; it does not wait for the computation to finish. A result already committed still wins. Busy duplicate deliveries return a temporary error and do not consume Asynq's retry budget; duplicates received after completion are acknowledged without executing.

`failure_count` is durable across Redis generations. Handler failures and abandoned leases consume the same PostgreSQL retry budget, preventing Redis loss from resetting retries indefinitely. SQL archival rejects automatic execution; only an explicit operator requeue resets the budget. Existing version-one failure history is counted during migration.

If an attempt crashes, its lease expires and a later claim or reconciliation marks it interrupted. Live execution is visible through lease expiry/fence fields, attempts, Redis active messages, and worker metrics. The durable job retains `accepted`/`retry` until finalization; a full effect is never visible with an uncommitted completion.

## Guarantees and external effects

For this implementation, durable business effects are PostgreSQL rows:

- The unique effect key and fenced finalization allow at most one committed effect per job.
- Effect, completion, and successful attempt status commit together.
- A crash during computation leaves no committed effect. A crash during finalization rolls the transaction back.
- A crash after commit causes redelivery to skip the handler.
- Redis data loss can recover unfinished jobs without reusing old transport acknowledgments.
- An expired worker cannot commit after a replacement has taken ownership.

A lease permits overlapping computation after a partition or process pause; it does not magically make an external operation exactly once. Real email/storage/model-service integrations must use provider idempotency keyed by `JobID(ctx)`, or a receiver that atomically rejects older `FencingToken(ctx)` values with the effect. Passing a token that the receiver ignores provides no protection. A local SQL fence cannot undo an SMTP send or an already billed model invocation.

Prepared results are in memory until finalization. A crashed worker loses its intermediate computation and must recompute. Long AI pipelines needing checkpoint/resume should add durable, fenced checkpoints or an artifact store; this upgrade provides ownership/finalization primitives rather than checkpointing or a real AI provider.

## Migration and rollout

Migration 1 is unchanged. Migration 2 adds lease/fence/failure fields, delivery generations, reconciliation scheduling, and an absolute process time while preserving job UUIDs, idempotency keys, payloads, schedules, and history. Unfinished legacy attempts are marked interrupted; recorded failures seed the durable budget.

Stop version-one API/worker processes before migrating. Old workers do not understand fences and must not coexist with the new execution protocol. Apply migrations, then start the new binaries. See [deployment](deployment.md) for the coordinated Docker/Kubernetes steps. Subsequent lease-aware replicas can share the reconciler safely.

## References

- [Asynq delivery and recovery model](https://github.com/hibiken/asynq)
- [PostgreSQL row locks](https://www.postgresql.org/docs/17/explicit-locking.html)
- [PostgreSQL INSERT conflict handling](https://www.postgresql.org/docs/17/sql-insert.html)

- [PostgreSQL wall-clock versus transaction timestamps](https://www.postgresql.org/docs/17/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)
