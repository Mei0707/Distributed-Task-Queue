# Failure verification

`internal/jobs/store_test.go` uses real PostgreSQL transactions and Redis, not in-memory replacements for their concurrency behavior. Each test owns a PostgreSQL schema. No tests flush shared Redis databases.

| Failure or race | Test evidence | Required outcome |
|---|---|---|
| 32 simultaneous identical submissions | Concurrent admission test | One job, one outbox row, one shared ID |
| Same key with different payload or delay | Store and HTTP conflict tests | Conflict; original job preserved |
| 20 concurrent deliveries of one job | Concurrent execution test | One handler call, one effect, 19 skips |
| Handler writes an effect, then returns error | Rollback and retry test | Zero committed effects after failure, one after retry |
| Cancellation before execution | Cancellation test | Handler skipped; zero effects |
| Payload does not match durable identity | Payload validation test | Permanent rejection, no effect |
| Redis unavailable during dispatch | Outbox retry test | Job remains durable; outbox stays undelivered |
| Redis accepts, dispatcher fails before SQL acknowledgment | Lost-acknowledgment test | Stable Redis ID reused; outbox eventually acknowledged |
| Worker SIGKILL before preparing effect | Real subprocess + replacement Asynq worker | Rollback; interrupted attempt; one eventual effect |
| Worker SIGKILL after preparing the in-memory effect | Real subprocess + replacement Asynq worker | Prepared result is lost; one eventual effect |
| Worker SIGKILL after effect INSERT inside finalization, before commit | Real subprocess + replacement Asynq worker | SQL transaction rolls back; one eventual effect |
| Worker SIGKILL after commit, before Asynq acknowledgment | Real subprocess + replacement Asynq worker | Original effect preserved; redelivery skips handler |
| PostgreSQL unavailable at admission | HTTP integration test with closed pool | HTTP 503; retry the same key |

The four SIGKILL tests run in parallel against separate Redis databases. A subprocess starts the same execution middleware as the deployed workers, reports that it has reached a transaction boundary, and blocks there. The parent sends a real OS kill signal, inspects committed database effects, starts a replacement Asynq worker, and waits for Redis lease recovery and eventual completion. It does not manually move the task into pending or bypass lease expiry.

The tests deliberately use a two-second task timeout and short retry delay, but Asynq's real recovery scan still takes roughly one minute. They assert the final Redis completion, durable job status, exactly one effect, and attempt states (`interrupted` then `completed`, or `completed` then `skipped`).

## Commands

```sh
docker compose up -d postgres redis
TEST_DATABASE_URL='postgres://queue:queue@localhost:5432/queue?sslmode=disable' \
TEST_REDIS_ADDR=localhost:6379 go test ./... -race -count=1 -v
```

Or run `make integration` for a fully containerized test runner with the race detector and the same subprocess crash tests. Run these suites sequentially, since they share reserved Redis test databases.

Raw output from the verified run is recorded in `docs/results/integration.log`. Performance tests separately verify that every submitted unique ID has a completed job and one SQL effect.

`docs/results/container-integration.log` records the same suite passing inside the Docker test image, including the race detector. The kind experiment in `scripts/kubernetes-crash.py` additionally exercises the deployed production worker binary and Kubernetes container restart under a duplicate-request load; its JSON report and event log are recorded alongside the unit/integration evidence.

## Manual Kubernetes experiment

Submit enough jobs to keep workers busy, then delete a worker pod:

```sh
kubectl -n queue-lab delete pod -l app=queue-worker --grace-period=0 --force
kubectl -n queue-lab rollout status deployment/worker
```

The Deployment replaces workers, and Asynq eventually recovers their expired leases. A force-deleted pod is not a reliable way to target a specific commit boundary; the subprocess suite provides deterministic boundary coverage. Never use force deletion on another workload without understanding its effects.

## Lease and reconciliation upgrade verification

`leases_test.go` verifies renewal across multiple lease periods without a held pool connection, prompt cancellation during computation, expired-owner renewal/finalization rejection, protection against late failure writes, durable retry budgets, explicit operator requeue, and version-one migration compatibility.

`reconcile_test.go` intentionally flushes an empty, dedicated Redis database 13 after successful delivery. It verifies that pending/expired/scheduled jobs are replayed into a fresh generation, original schedules and live leases survive, terminal SQL jobs stay terminal, and completed jobs retain exactly one effect. Additional tests cover concurrent bounded reconcilers, Redis outages, completion racing with inspection, and archival after a final-retry worker crash. It never flushes the application Redis database 0; run suites sequentially on the reserved test databases.

The new host and Docker race-enabled suite outputs are recorded separately as `docs/results/upgrade-integration.log` and `docs/results/upgrade-container-integration.log`. The October 20 performance reports describe the earlier implementation and are retained as historical measurements, not claimed as lease-version benchmarks.
