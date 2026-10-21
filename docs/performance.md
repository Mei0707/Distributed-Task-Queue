# Recorded performance evidence

Measured on October 20, 2026. The raw JSON reports, environment versions/image identities, integration output, Kubernetes pod list, and scrape-target snapshots are in [results](results/).

## Method

- Docker Desktop on an ARM64 macOS host; Docker VM configured with 10 CPUs and 8,218,034,176 bytes of memory (about 7.65 GiB).
- Go 1.26.1; PostgreSQL 17.6; Redis 7.4.5 with AOF `appendfsync always`.
- One Compose API, PostgreSQL, and Redis; compare one worker with three workers. Each worker has 20 execution slots and a 30-connection maximum PostgreSQL pool.
- Fixed workload: 5,000 unique email jobs, 20 concurrent HTTP clients, default queue, no artificial scheduling delay. The email simulation waits 100 ms, then records a database effect. There is no SMTP or external-network work.
- Warm-up before each replica-count group, then three finite-backlog runs. Each run waits for all jobs to complete before the next begins. Idempotency keys use a unique run UUID, so prior data cannot create false replays.
- The generator measures client-side POST latency separately from queue-to-completion latency. With a database URL, it polls PostgreSQL every 200 ms for completion, avoiding an HTTP history-poll workload, then independently counts effects for exactly the IDs returned by admission.
- A separate 10,000-request run uses 5,000 keys, producing 50% duplicate requests.

The Kubernetes cluster and monitoring ran on the same Docker VM. Cluster setup/smoke work and integration tests overlapped parts of the measurement window. This is a local engineering comparison with shared resources, not an isolated capacity benchmark. The first three-worker admission run shows noticeably more variability than the other two.

## Results

| Workers | Completion throughput across runs (jobs/sec) | Median jobs/sec | Median POST p95 | Median completion p95 |
|---|---|---|---|---|
| 1 | 175.83, 181.07, 182.08 | 181.07 | 6.63 ms | 25.05 s |
| 3 | 373.89, 396.98, 373.98 | 373.98 | 5.37 ms | 11.05 s |

The median completion-rate ratio is **2.07×**. Every run accepted and completed **5,000 jobs**, verified **5,000 effect rows**, and reported **zero errors**. Median admission rates were about 4,485 requests/sec and 5,199 requests/sec respectively; these measure durable API admission, not execution throughput.

The duplicate workload returned **5,000 new admissions and 5,000 replays**, completed **5,000 jobs**, verified **5,000 effects**, and reported **zero errors**. Its end-to-end completion rate was 366.03 jobs/sec and POST p95 was 5.69 ms.

“Completion throughput” is completed jobs divided by time from the first POST until the final drain observation. “Completion p95” uses PostgreSQL's creation/completion timestamps, so it includes backlog wait and simulated execution. These 13–28 second runs do not establish long-term steady-state capacity or tail behavior during a sustained arrival stream. The 200 ms polling interval slightly understates completion rate.

## Reproduce

```sh
make up
make benchmark
```

The script writes fresh raw reports to `artifacts/load/` and exits unsuccessfully if any workload fails verification. To run one scenario manually:

```sh
DATABASE_URL='postgres://queue:queue@localhost:5432/queue?sslmode=disable' \
go run ./cmd/loadtest -requests 5000 -unique 5000 -concurrency 20 -workers 3
```

The `-workers` argument records the intended configuration; it does not scale the deployment. Use Compose scaling first. The generator uses a closed-loop client with fixed concurrency; it does not measure latency under a separately controlled arrival rate. No claim is made about real email-provider performance, image-resizing CPU throughput, HA failover capacity, or HPA behavior.

## Correctness evidence alongside throughput

The race-enabled integration suite passed all concurrent-admission, duplicate-execution, rollback, outbox, and subprocess SIGKILL recovery tests. The recorded SIGKILL cases recovered in roughly one minute and left exactly one effect each. See [failure verification](failures.md) and `results/integration.log`.

The Kubernetes smoke run uses the actual API/worker binaries, two API pods and three worker pods. It submits 200 requests with 100 unique keys, verifies completion and SQL effects, and records per-worker attempt distribution. This is a smoke test through `kubectl port-forward`, which selects one API pod, rather than a Service load-balancing benchmark.

The Kubernetes runtime-SIGKILL experiment submitted **4,000 requests with 2,000 unique keys**. Kubernetes recorded exit code **137** and restarted the killed worker. The final SQL history contained **2,000 completed attempts and 12 interrupted attempts**; all **2,000 jobs** completed with exactly **2,000 effects**, **2,000 HTTP replays**, and **zero errors**. Full drain took **105.60 seconds**, including real lease recovery and default retry delay. Completion p95 was only 3.95 seconds because fewer than 1% of jobs needed recovery; the full-drain time exposes the failure tail that p95 misses. Raw workload, event, and restart evidence is in `results/kubernetes-crash*` and `results/kubernetes-restarts.json`.

## October 21 recovery/lease upgrade checks

The October 20 throughput measurements above are historical and are not used to claim performance for the new lease protocol. The new Compose smoke test submitted 1,000 requests with 500 distinct keys: 500 admissions, 500 replays, 500 completed jobs and SQL effects, and zero errors. This is a correctness smoke check rather than a new scaling benchmark; its raw report is `results/upgrade-compose-smoke.json`.

A live background-reconciliation probe deleted only its own newly accepted job's acknowledged Redis message, leaving PostgreSQL untouched. The running API recovered the job in a new delivery generation (`UUID:2`) and committed exactly one effect, in 4.23 seconds in this run. Its raw evidence is `results/upgrade-runtime-recovery.json`. Unit/integration tests additionally cover complete loss of the reserved Redis test database, scheduling, terminal states, active leases, concurrent reconciliation, and stale result/failure fencing.

New host/container race-enabled suite evidence is in `results/upgrade-integration.log` and `results/upgrade-container-integration.log`; the new Kubernetes duplicate-request smoke and rollout evidence use the `upgrade-kubernetes-*` filenames.
