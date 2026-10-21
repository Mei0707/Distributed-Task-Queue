package main

import (
	"context"
	"log"
	"time"

	"github.com/yourname/asynq-task-queue/internal/config"
	"github.com/yourname/asynq-task-queue/internal/jobs"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store, err := jobs.Open(ctx, cfg.Database.URL, cfg.Database.MaxConnections)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	log.Print("database migrations complete")
}
