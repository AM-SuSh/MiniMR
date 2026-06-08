#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

RPC_PORT=${RPC_PORT:-8080}
HTTP_PORT=${HTTP_PORT:-8081}
N_WORKERS=${N_WORKERS:-3}

mkdir -p bin mr-work

echo "=== Building ==="
go build -o bin/master ./cmd/master
go build -o bin/worker ./cmd/worker
go build -o bin/client ./cmd/client

echo "=== Starting Master ==="
./bin/master -port ":${RPC_PORT}" -http ":${HTTP_PORT}" &
MASTER_PID=$!
sleep 2

cleanup() {
  echo "=== Cleanup ==="
  kill "$MASTER_PID" 2>/dev/null || true
  pkill -f "bin/worker" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Workers ==="
for i in $(seq 1 "$N_WORKERS"); do
  ./bin/worker -master "localhost:${RPC_PORT}" -id "worker-$i" &
done
sleep 1

echo "=== Submitting WordCount Job ==="
./bin/client \
  -master-http "http://localhost:${HTTP_PORT}" \
  -input "testdata/input.txt" \
  -nreduce 3 \
  -map wordcount_map \
  -reduce wordcount_reduce \
  -combine wordcount_combine \
  -workdir mr-work

echo "=== Output ==="
cat mr-work/mr-out-* 2>/dev/null | sort
echo "=== Done ==="
