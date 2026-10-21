.PHONY: help run-api run-worker build tidy test redis-up redis-down up down integration migrate benchmark k8s-render

# Default target
help:
	@echo ""
	@echo "  make run-api       Start the REST API server (port 8080)"
	@echo "  make run-worker    Start the background task worker"
	@echo "  make build         Compile both binaries to ./bin/"
	@echo "  make tidy          Download and tidy Go modules"
	@echo "  make test          Run all tests"
	@echo "  make redis-up      Start Redis in Docker"
	@echo "  make redis-down    Stop Redis Docker container"
	@echo "  make up/down       Start or stop the complete container stack"
	@echo "  make migrate       Apply PostgreSQL migrations"
	@echo "  make integration   Run containerized failure tests"
	@echo "  make benchmark     Record one/three-worker load comparisons"
	@echo "  Dashboard:         http://localhost:8080/dashboard"
	@echo ""

# Run API server (requires Redis running)
run-api:
	go run ./cmd/api/...

# Run worker process (requires Redis running)
run-worker:
	go run ./cmd/worker/...

# Build both binaries
build:
	@mkdir -p bin
	go build -o bin/api    ./cmd/api/...
	go build -o bin/worker ./cmd/worker/...
	go build -o bin/migrate ./cmd/migrate/...
	@echo "Binaries: bin/api, bin/worker"

# Download dependencies
tidy:
	go mod tidy

# Run tests
test:
	go test ./... -v -race

migrate:
	go run ./cmd/migrate

up:
	docker compose up -d --build --scale worker=3

down:
	docker compose down

integration:
	docker compose --profile test run --build --rm integration

benchmark:
	bash scripts/benchmark.sh

k8s-render:
	kubectl kustomize deploy

# Start Redis via Docker
redis-up:
	docker run -d --name asynq-redis -p 6379:6379 redis:7-alpine
	@echo "Redis started on localhost:6379"

# Stop Redis container
redis-down:
	docker stop asynq-redis && docker rm asynq-redis
