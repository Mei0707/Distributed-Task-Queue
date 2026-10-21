package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/jobs"
	"github.com/yourname/asynq-task-queue/internal/telemetry"
	"github.com/yourname/asynq-task-queue/internal/worker"
	"github.com/yourname/asynq-task-queue/pkg/logger"
)

// The worker binary is a separate process from the API server.
//   - Independent scaling: scale workers based on queue depth, API by RPS
//   - Fault isolation: a worker crash doesn't affect API availability
//   - Resource control: CPU-heavy image processing won't starve HTTP responses
//   - Kubernetes: deploy as separate Deployments with separate resource limits
//
// Run multiple worker instances against the same Redis for horizontal scaling.
// Asynq delivers at least once; PostgreSQL serializes and deduplicates effects.
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	store, err := jobs.Open(ctx, cfg.Database.URL, cfg.Database.MaxConnections)
	cancel()
	if err != nil {
		log.Fatal("Failed to open job database", zap.Error(err))
	}
	defer store.Close()
	srv, err := worker.New(cfg, log, store)
	if err != nil {
		log.Fatal("Failed to create worker server", zap.Error(err))
	}

	if err = srv.Start(); err != nil {
		log.Fatal("Failed to start worker", zap.Error(err))
	}
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
	defer redisClient.Close()
	mux := http.NewServeMux()
	mux.Handle("/metrics", telemetry.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if store.Pool.Ping(ctx) != nil || redisClient.Ping(ctx).Err() != nil {
			http.Error(w, "dependencies unavailable", 503)
			return
		}
		w.Write([]byte("ready"))
	})
	metricsServer := &http.Server{Addr: ":" + cfg.Worker.MetricsPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	metricsServer.Shutdown(shutdownCtx)

	log.Info("Worker process stopped cleanly")
}
