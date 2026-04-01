.PHONY: help run-api run-worker build tidy test redis-up redis-down

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
	@echo "Binaries: bin/api, bin/worker"

# Download dependencies
tidy:
	go mod tidy

# Run tests
test:
	go test ./... -v -race

# Start Redis via Docker
redis-up:
	docker run -d --name asynq-redis -p 6379:6379 redis:7-alpine
	@echo "Redis started on localhost:6379"

# Stop Redis container
redis-down:
	docker stop asynq-redis && docker rm asynq-redis
