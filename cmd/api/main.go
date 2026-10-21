package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/Mei0707/Distributed-Task-Queue/internal/config"
	"github.com/Mei0707/Distributed-Task-Queue/internal/jobs"
	"github.com/Mei0707/Distributed-Task-Queue/internal/telemetry"
	"github.com/Mei0707/Distributed-Task-Queue/pkg/logger"
)

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
	defer log.Sync() // flush buffered log entries on exit

	log.Info("Starting API server",
		zap.String("port", cfg.Server.Port),
		zap.String("redis", cfg.Redis.Addr),
	)

	// --- Redis Connection (shared by Client and Inspector) ---
	redisOpt := asynq.RedisClientOpt{
		Addr:        cfg.Redis.Addr,
		Password:    cfg.Redis.Password,
		DB:          cfg.Redis.DB,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
	}

	// asynq.Client: enqueues tasks into Redis queues
	client := asynq.NewClient(redisOpt)
	defer client.Close()

	// asynq.Inspector: reads queue/task state for monitoring endpoints
	inspector := asynq.NewInspector(redisOpt)
	defer inspector.Close()
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	store, err := jobs.Open(startupCtx, cfg.Database.URL, cfg.Database.MaxConnections)
	startupCancel()
	if err != nil {
		log.Fatal("Failed to open job database", zap.Error(err))
	}
	defer store.Close()
	telemetry.Registry.MustRegister(jobs.Collector{Store: store})
	telemetry.Registry.MustRegister(transportCollector{inspector: inspector})
	dispatchCtx, stopDispatch := context.WithCancel(context.Background())
	dispatchDone := make(chan struct{})
	go func() { defer close(dispatchDone); store.RunDispatcher(dispatchCtx, client, log) }()
	defer func() { stopDispatch(); <-dispatchDone }()
	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		store.RunReconciler(dispatchCtx, inspector, cfg.Reconciliation.BatchSize, cfg.Reconciliation.Interval, log)
	}()
	defer func() { stopDispatch(); <-reconcileDone }()

	// --- Build HTTP Router ---
	router := NewRouter(client, inspector, log, store, cfg)

	// --- HTTP Server with graceful shutdown ---
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start server in a goroutine so it doesn't block signal handling
	go func() {
		log.Info("API server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("API server failed", zap.Error(err))
		}
	}()

	// --- Graceful Shutdown ---
	// Wait for SIGINT (Ctrl+C) or SIGTERM (Docker/Kubernetes stop signal)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutdown signal received, draining connections...")

	// Give active requests 30 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("Forced shutdown due to timeout", zap.Error(err))
	}

	log.Info("API server stopped cleanly")
}
