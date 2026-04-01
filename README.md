# Distributed Task Queue — Go + Asynq + Redis

A production-ready distributed task queue system built with [Asynq](https://github.com/hibiken/asynq), Gin, and Redis. Handles email delivery and image processing tasks with priority queues, delayed scheduling, automatic retries, and a dead letter queue.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Clients                          │
│              (HTTP requests to REST API)                │
└───────────────────────┬─────────────────────────────────┘
                        │  POST /api/v1/tasks/email
                        │  POST /api/v1/tasks/image/resize
                        ▼
┌─────────────────────────────────────────────────────────┐
│                    API Server (:8080)                   │
│              cmd/api  — Gin HTTP server                 │
│   Validates requests → asynq.Client.Enqueue() → Redis  │
└───────────────────────┬─────────────────────────────────┘
                        │  enqueue task
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  Redis Task Queues                      │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  │
│  │   critical   │  │   default    │  │     low     │  │
│  │   weight: 6  │  │   weight: 3  │  │  weight: 1  │  │
│  └──────────────┘  └──────────────┘  └─────────────┘  │
│                                                         │
│  Task States:  pending → active → completed             │
│                           ↓ (on failure)                │
│                        retry → archived (dead letter)   │
└───────────────────────┬─────────────────────────────────┘
                        │  poll & dequeue
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  Worker Process(es)                     │
│         cmd/worker — asynq.Server with goroutine pool  │
│                                                         │
│  ┌─────────────────────┐  ┌──────────────────────────┐ │
│  │  email:deliver      │  │  image:resize            │ │
│  │  EmailHandler       │  │  ImageHandler            │ │
│  └─────────────────────┘  └──────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### Why Two Separate Processes?

| Concern | Benefit |
|---|---|
| **Independent scaling** | Scale workers by queue depth; scale API by RPS |
| **Fault isolation** | Worker OOM/panic doesn't affect API availability |
| **Resource control** | CPU-heavy image tasks won't starve HTTP responses |
| **Kubernetes** | Separate Deployments with separate CPU/memory limits |

---

## Project Structure

```
.
├── cmd/
│   ├── api/                    # HTTP API server entrypoint
│   │   ├── main.go             # Server setup and graceful shutdown
│   │   ├── router.go           # All route registrations
│   │   ├── email_handler.go    # POST /tasks/email
│   │   ├── image_handler.go    # POST /tasks/image/resize
│   │   ├── inspect_handler.go  # Queue inspection and control endpoints
│   │   ├── dashboard_handler.go# Built-in dashboard route
│   │   └── web/                # Embedded dashboard assets
│   └── worker/
│       └── main.go             # Worker entrypoint
│
├── internal/
│   ├── config/
│   │   └── config.go           # Viper config (file + env vars)
│   ├── middleware/
│   │   └── middleware.go       # HTTP middleware (logger, recovery)
│   ├── tasks/
│   │   ├── email/
│   │   │   └── handler.go      # Task payload, creator, handler
│   │   └── image/
│   │       └── handler.go      # Task payload, creator, handler
│   └── worker/
│       └── server.go           # asynq.Server setup + handler registration
│
├── pkg/
│   ├── logger/
│   │   └── logger.go           # Zap structured logger
│   └── response/
│       └── response.go         # Standard JSON response envelope
│
├── config.yaml                 # Default configuration
├── Makefile                    # Dev commands
└── go.mod
```

---

## Getting Started

### Prerequisites

- Go 1.21+
- Redis 7+ (or Docker)

### 1. Start Redis

```bash
make redis-up
# or manually:
docker run -d -p 6379:6379 redis:7-alpine
```

### 2. Install Dependencies

```bash
make tidy
```

### 3. Start the Worker (Terminal 1)

```bash
make run-worker
```

### 4. Start the API Server (Terminal 2)

```bash
make run-api
```

### 5. Open the Built-in Dashboard

```text
http://localhost:8080/dashboard
```

The dashboard is served by your own API server and can be used to:

- Enqueue email tasks from the browser
- Enqueue image resize tasks from the browser
- Inspect queue depth and task states
- Pause and resume queues
- Run scheduled, retry, or archived tasks immediately
- Archive or delete queued tasks

---

## API Reference

### Enqueue Email Delivery Task

```bash
POST /api/v1/tasks/email
```

```json
{
  "user_id": 42,
  "to_address": "user@example.com",
  "template_id": "welcome_email",
  "subject": "Welcome to our platform!",
  "queue": "critical",
  "process_in_seconds": 0
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "task_id": "01HQ5G3YK8ABCDEF",
    "queue": "critical",
    "status": "pending"
  }
}
```

### Enqueue Image Resize Task

```bash
POST /api/v1/tasks/image/resize
```

```json
{
  "source_url": "https://example.com/images/photo.jpg",
  "output_bucket": "my-bucket",
  "output_key": "resized/photo_800x600.webp",
  "width": 800,
  "height": 600,
  "format": "webp",
  "queue": "low",
  "process_in_seconds": 0
}
```

### Monitor Queues

```bash
# Built-in dashboard
GET /dashboard

# List all queues
GET /api/v1/queues

# Queue statistics
GET /api/v1/queues/default/stats

# Active tasks (being processed right now)
GET /api/v1/queues/default/tasks/active

# Pending tasks (waiting for a worker)
GET /api/v1/queues/default/tasks/pending

# Scheduled tasks (future execution)
GET /api/v1/queues/default/tasks/scheduled?page=1&page_size=20

# Retry tasks (waiting for next retry attempt)
GET /api/v1/queues/default/tasks/retry

# Dead letter queue (exhausted all retries)
GET /api/v1/queues/default/tasks/archived

# Pause and resume a queue
POST /api/v1/queues/default/pause
POST /api/v1/queues/default/unpause

# Task control actions
POST   /api/v1/queues/default/tasks/:task_id/run
POST   /api/v1/queues/default/tasks/:task_id/archive
DELETE /api/v1/queues/default/tasks/:task_id
```

---

## Queue Priority

The three queues use **weighted priority**:

| Queue | Weight | ~% of workers |
|---|---|---|
| `critical` | 6 | 60% |
| `default`  | 3 | 30% |
| `low`      | 1 | 10% |

Use `"queue": "critical"` for user-facing real-time tasks, `"low"` for batch jobs.

---

## Task Lifecycle & Retry Logic

```
enqueued
    │
    ▼
pending ──→ active ──→ completed ✓
                │
                ▼ (error returned)
             retry (exponential backoff)
                │
                │ (MaxRetry exceeded)
                ▼
           archived  ← Dead Letter Queue
           (manual intervention needed)
```

**Retry backoff formula** (asynq default):
```
delay = 2^n + jitter   where n = retry attempt number
```

Tasks in the `archived` queue can be:
- Re-run from the built-in dashboard
- Re-run via `asynq` CLI: `asynq task run --queue=default --id=<task_id>`
- Deleted if no longer needed

---

## Configuration

All values can be overridden with environment variables (dot → underscore):

```bash
# config.yaml equivalent
REDIS_ADDR=redis:6379
REDIS_PASSWORD=secret
SERVER_PORT=9090
WORKER_CONCURRENCY=50
```

---

## Web UI

This project now includes its own queue dashboard at:

```text
http://localhost:8080/dashboard
```

It is backed by the same API server and queue inspection endpoints in this repo, so you can test the system end-to-end without running Asynqmon.

Current dashboard capabilities:

- Live queue visibility with auto-refresh
- Browser-based enqueue forms for email and image tasks
- Queue pause/resume controls
- Task actions for run-now, archive, and delete
- Visibility into pending, active, scheduled, retry, and archived tasks

---

## Scaling

To scale processing capacity, run multiple worker instances — they safely compete for tasks via Redis distributed locking:

```bash
# Terminal 1
WORKER_CONCURRENCY=20 go run ./cmd/worker/...

# Terminal 2
WORKER_CONCURRENCY=20 go run ./cmd/worker/...

# Terminal 3
WORKER_CONCURRENCY=20 go run ./cmd/worker/...
```

Total throughput = sum of all worker concurrencies across all instances.
