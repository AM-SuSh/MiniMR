#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

RPC_PORT=${RPC_PORT:-8080}
HTTP_PORT=${HTTP_PORT:-8081}

mkdir -p bin mr-work-crawl

echo "=== Building ==="
go build -o bin/master ./cmd/master
go build -o bin/worker ./cmd/worker

echo "=== Starting Master ==="
./bin/master -port ":${RPC_PORT}" -http ":${HTTP_PORT}" &
MASTER_PID=$!
sleep 2

cleanup() {
  kill "$MASTER_PID" 2>/dev/null || true
  pkill -f "bin/worker" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Workers ==="
for i in 1 2; do
  ./bin/worker -master "localhost:${RPC_PORT}" -id "worker-$i" &
done
sleep 1

echo "=== Running Crawl Clean Pipeline ==="
python3 bridge/crawler_pipeline.py

echo "=== Done ==="
