# Distributed task queue lab

Go, Gin, Asynq, Redis, and PostgreSQL, with durable admission, transactional idempotency, execution history, failure tests, Prometheus/Grafana, and Docker/Kubernetes deployments.

Email delivery and image processing remain **simulations**. Their observable effects are rows in PostgreSQL. The project demonstrates exactly one committed database effect per job under duplicate delivery and worker crashes. It does not claim exactly-once delivery to a remote email or object-storage provider.

## Start the complete stack

Requires Docker Desktop / Docker Engine with Compose. For local Go commands, use Go 1.26.1 (the version used in the Docker build and recorded tests).

For an existing version-one installation, follow the [coordinated upgrade steps](docs/deployment.md#upgrade-from-version-one-execution) first. Old workers must stop before migration 2 because they do not enforce execution fences.

```sh
make up
# equivalent: docker compose up -d --build --scale worker=3
```

- Queue dashboard: http://localhost:8080/dashboard
- Job history: http://localhost:8080/api/v1/jobs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin / queue-local-only)

Compose runs a versioned migration before starting the application. PostgreSQL and Redis use named volumes. Redis uses AOF with `appendfsync always`. `make down` stops the stack and preserves its volumes.

The provided credentials and plaintext connections are for a local learning environment. Public deployment requires authentication, secrets management, TLS, backups, and highly available data services.

## Submit an idempotent job

```sh
curl -i http://localhost:8080/api/v1/tasks/email \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: welcome-user-42-v1' \
  -d '{"user_id":42,"to_address":"user@example.com","template_id":"welcome","subject":"Welcome","queue":"critical"}'
```

The first request returns **202 Accepted**, a stable `task_id`, and a `Location` header for its durable history. It can succeed while Redis is unavailable: the outbox will retry delivery. An identical request with the same key returns **200** with the same job ID and `Idempotency-Replayed: true`. Changing the payload, queue, or delay while reusing the key returns **409**.

Keys are scoped by task type and retained with job history. Supply your own key when retrying an uncertain HTTP outcome. If omitted, the API generates a new key and returns it in a response header; two requests without a key create two jobs.

```sh
curl http://localhost:8080/api/v1/jobs
curl http://localhost:8080/api/v1/jobs/JOB_ID
curl -X POST http://localhost:8080/api/v1/jobs/JOB_ID/cancel
```

The detail endpoint returns the job, its latest 100 attempts, and the simulated effect result. Cancellation serializes against execution; a committed completion wins over a later cancellation. Canceling running work revokes its renewable lease immediately; stale results cannot finalize. Transport messages for canceled jobs drain without producing effects.

Image jobs use `POST /api/v1/tasks/image/resize`:

```json
{"source_url":"https://example.com/photo.jpg","output_bucket":"demo","output_key":"photo.webp","width":800,"height":600,"format":"webp","queue":"low","process_in_seconds":0}
```

Both task types accept queues `critical`, `default`, or `low`, and a delay from 0 to 604800 seconds. Workers schedule queues with weights 6:3:1. All newly admitted jobs snapshot the configured retry, timeout, and retention settings into PostgreSQL.

## Architecture

```text
HTTP -> PostgreSQL transaction: jobs + job_outbox
                           |
                  concurrent outbox dispatchers
                  (FOR UPDATE SKIP LOCKED)
                           |
                  Redis / Asynq generation-specific task ID
                           |
                     multiple workers
                           |
           Short claim -> work + heartbeats -> fenced SQL finalization
```

Redis provides at-least-once transport. PostgreSQL is the authority for job identity, cancellation, and committed effects. See [database design and guarantees](docs/design.md) and the [failure matrix](docs/failures.md).

## Tests and measured performance

```sh
make test                         # unit tests, race detector; integration tests skip without env
make integration                  # real PostgreSQL, Redis, subprocess SIGKILL tests
make benchmark                    # three runs each with one/three workers, plus 50% duplicate requests
```

To run the full suite directly against the local stack:

```sh
TEST_DATABASE_URL='postgres://queue:queue@localhost:5432/queue?sslmode=disable' \
TEST_REDIS_ADDR=localhost:6379 go test ./... -race -count=1 -v
```

Integration tests create and remove isolated PostgreSQL schemas. Crash tests use Redis databases 9–12; outbox tests use database 15. The data-loss test reserves empty database 13 and intentionally flushes only that database, refusing to run if it is already nonempty. Use a dedicated Redis instance and no other test workers on those databases. Recovery tests take about a minute because they wait for Asynq's real lease recovery.

The load generator checks accepted IDs, duplicate responses, job completion, and (when `DATABASE_URL` is supplied) independent SQL effect counts. It exits unsuccessfully on errors, missing completions, or mismatched counts. See [recorded results and limitations](docs/performance.md); raw reports are in [docs/results](docs/results).

## Kubernetes

See [deployment and scaling](docs/deployment.md) for an isolated kind cluster, a two-API / three-worker deployment, health probes, metrics discovery, manual scaling, and optional HPA. The manifests include persistent, single-instance PostgreSQL and Redis for learning; they are not an HA database setup.

## Development

```sh
# With PostgreSQL and Redis running:
make migrate
make run-api
make run-worker
```

Configuration loads from `config.yaml` and environment variables such as `DATABASE_URL`, `DATABASE_MAX_CONNECTIONS`, `REDIS_ADDR`, `WORKER_CONCURRENCY`, `WORKER_TASK_TIMEOUT`, `WORKER_MAX_RETRY`, and `WORKER_RETENTION_TTL`. Environment variables override YAML. Changes to task defaults apply only to newly accepted jobs.

Source layout:

- `cmd/api`: REST endpoints, embedded dashboard, readiness and metrics
- `cmd/worker`: worker lifecycle and metrics/health HTTP server on port 9091
- `cmd/migrate`: serialized, versioned PostgreSQL migrations
- `cmd/loadtest`: HTTP workload, latency measurements, drain and effect verification
- `internal/jobs`: versioned migrations, admission/outbox, Redis reconciliation, execution leases/fences, history, failure tests
- `internal/tasks`: context-aware simulated email/image work and transactional effect recording
- `internal/telemetry`: bounded-label counters and histograms
- `deploy`: Compose monitoring config and Kubernetes Kustomize resources

The module path is `github.com/Mei0707/Distributed-Task-Queue`. Generated `/api` and `/bin/` binaries are excluded from version control; build with `make build` or the Dockerfile. Deleting tracked binaries removes them from the current tree; historical Git commits still contain their original objects.

Recovery controls: `WORKER_LEASE_DURATION` (default `30s`), `RECONCILIATION_INTERVAL` (`15s`), and `RECONCILIATION_BATCH_SIZE` (`50`). Reconciliation runs in every API replica, repairs missing Redis deliveries, preserves active leases/schedules, and exposes `queue_reconciliations_total`. Workers expose `queue_execution_lease_losses_total`.
