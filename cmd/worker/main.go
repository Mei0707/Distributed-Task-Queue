package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/worker"
	"github.com/yourname/asynq-task-queue/pkg/logger"
)

// The worker binary is a separate process from the API server.
//
// Why separate processes?
//   - Independent scaling: scale workers based on queue depth, API by RPS
//   - Fault isolation: a worker crash doesn't affect API availability
//   - Resource control: CPU-heavy image processing won't starve HTTP responses
//   - Kubernetes: deploy as separate Deployments with separate resource limits
//
// Run multiple worker instances against the same Redis for horizontal scaling.
// Asynq handles distributed locking so tasks are never double-processed.
func main() {
	// --- Load Config ---
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// --- Initialize Logger ---
	log, err := logger.New("production")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	log.Info("Starting worker process",
		zap.String("redis", cfg.Redis.Addr),
		zap.Int("concurrency", cfg.Worker.Concurrency),
	)

	// --- Create Worker Server ---
	srv, err := worker.New(cfg, log)
	if err != nil {
		log.Fatal("Failed to create worker server", zap.Error(err))
	}

	// --- Start Worker in Background Goroutine ---
	// srv.Start() blocks until srv.Shutdown() is called or Redis connection is lost
	errCh := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errCh <- err
		}
	}()

	// --- Graceful Shutdown ---
	// On SIGINT/SIGTERM: stop accepting new tasks, wait for in-flight tasks to finish
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Info("Shutdown signal received", zap.String("signal", sig.String()))
	case err := <-errCh:
		log.Error("Worker encountered fatal error", zap.Error(err))
	}

	// Asynq performs graceful shutdown: waits for active tasks to complete
	srv.Shutdown()

	log.Info("Worker process stopped cleanly")
}