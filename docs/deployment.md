# Deployment, metrics, and scaling

## Docker Compose

`make up` starts Redis, PostgreSQL, a one-shot migration, an API, three workers, Prometheus, and Grafana. The multi-stage image builds static binaries and runs them as UID 10001. There are no host source mounts for the application.

```sh
docker compose up -d --build --scale worker=3
docker compose ps
docker compose logs -f api worker
docker compose up -d --no-deps --wait --scale worker=1 worker
```

Workers expose metrics internally on port 9091. Compose deliberately does not bind a fixed worker host port, allowing multiple replicas. Prometheus uses DNS service discovery to scrape each replica. Grafana provisions its Prometheus datasource and dashboard from the repository. Prometheus alert rules are loaded; external notification routing/Alertmanager is not configured.

## Isolated local Kubernetes

The checked-in Kustomization deploys two API replicas and three workers. Use a local cluster rather than an unrelated active context:

```sh
kind create cluster --name queue-lab --kubeconfig /tmp/queue-lab-kubeconfig --image kindest/node:v1.32.2
docker build --target runtime -t distributed-queue:local .
kind load docker-image distributed-queue:local --name queue-lab
kubectl --kubeconfig /tmp/queue-lab-kubeconfig apply -k deploy
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab rollout status deployment/api
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab rollout status deployment/worker
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab get pods
```

Every API/worker pod runs the migration as an init container. A transaction-scoped advisory lock serializes concurrent migration runners, and schema versions prevent reapplication. Initial init-container retries while PostgreSQL starts are expected. Resources use an isolated namespace, explicit CPU/memory requests and limits, and persistent volumes for PostgreSQL and Redis. API/worker containers use a non-root UID, read-only filesystem, and no Linux capabilities.

Forward services in separate terminals:

```sh
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab port-forward service/api 18080:8080
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab port-forward service/prometheus 19090:9090
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab port-forward service/grafana 13000:3000
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab port-forward service/postgres 15432:5432
```

`kubectl port-forward service/api` forwards to one selected pod, so this is a correctness/smoke path rather than a benchmark of Service load balancing.

Manually scale workers and observe per-pod throughput:

```sh
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab scale deployment/worker --replicas=1
kubectl --kubeconfig /tmp/queue-lab-kubeconfig -n queue-lab scale deployment/worker --replicas=3
```

An optional `deploy/k8s/hpa.yaml` targets 70% CPU and 3–4 workers. It requires metrics-server, which kind does not install by default. The simulated tasks mostly wait, so CPU is a weak autoscaling signal for this workload; queue-depth/age scaling would require a Prometheus adapter or KEDA. The default deployment uses explicit replicas so scaling experiments remain controlled.

For a reproducible crash experiment on this isolated kind cluster:

```sh
mkdir -p artifacts/load
go build -o artifacts/load/loadtest ./cmd/loadtest
DATABASE_URL='postgres://queue:queue@localhost:15432/queue?sslmode=disable' \
python3 scripts/kubernetes-crash.py --kubeconfig /tmp/queue-lab-kubeconfig
```

The script submits 4,000 requests for 2,000 unique jobs, identifies a busy worker, and sends a real SIGKILL through that kind node's container runtime. Kubernetes restarts the container and Asynq recovers expired leases. It requires verified SQL effects and observed interrupted/skipped attempts; a workload that only drains gracefully does not pass. The script deliberately targets the named local kind node and is not a general remote-cluster fault injector. API/PostgreSQL port-forwards must remain open during the experiment.

PostgreSQL allows 200 connections in the lab. With two APIs and four workers at the default maximum 30 connections per process, the upper bound is 180 plus migrations/operations. Adjust the budget before increasing replicas or concurrency. A single worker's concurrency shares the same SQL pool.

## Observability

| Endpoint or signal | Interpretation |
|---|---|
| `/health` | Process liveness; dependency outages do not cause restart storms |
| `/ready` | PostgreSQL and Redis connectivity |
| API `/metrics` | HTTP latency/counts, admissions/replays/conflicts, outbox failures, durable job counts, Redis queue depths, dependency health |
| Worker `:9091/metrics` | Execution outcomes/durations, active executions, Go/process metrics |
| `/api/v1/jobs/:id` | Durable job, latest 100 attempts, and effect result |

Metric labels use route patterns, task types, and bounded outcomes rather than job IDs or idempotency keys. Durable-state/Redis gauges are repeated by API replicas: aggregate them with `max`, not `sum`. Worker counters are per-process: use `sum(rate(...))`. Counters reset on restart; PostgreSQL history remains durable. Hard-killed workers cannot increment a final outcome counter; recovery is visible in attempts and subsequent skip/completion counts.

Kubernetes Prometheus discovery uses a namespace-scoped read-only Role for pods and scrapes the annotated API/worker endpoints. Kubernetes monitoring storage is ephemeral; Compose monitoring uses named volumes. The sample data services are single replicas and the lab credentials are intentionally explicit; production availability and secret rotation need a separate deployment design.

## Cleanup

```sh
docker compose down                   # keeps database volumes
kind delete cluster --name queue-lab  # destroys this local cluster and its data
```

## References

- [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/)
- [Kubernetes horizontal autoscaling](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
