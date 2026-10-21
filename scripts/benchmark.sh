#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/load
go build -o artifacts/load/loadtest ./cmd/loadtest
export DATABASE_URL="${DATABASE_URL:-postgres://queue:queue@localhost:5432/queue?sslmode=disable}"
for workers in 1 3; do
  docker compose up -d --no-deps --wait --scale "worker=$workers" worker
  artifacts/load/loadtest -requests 200 -unique 100 -concurrency 10 -workers "$workers" > "artifacts/load/warmup-$workers.json"
  for run in 1 2 3; do
    artifacts/load/loadtest -requests 5000 -unique 5000 -concurrency 20 -workers "$workers" > "artifacts/load/workers-$workers-run-$run.json"
  done
done
artifacts/load/loadtest -requests 10000 -unique 5000 -concurrency 20 -workers 3 > artifacts/load/duplicates.json
