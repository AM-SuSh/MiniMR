#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-3}
MASTER=${2:-localhost:8080}

echo "Building worker..."
go build -o bin/worker ./cmd/worker

echo "Starting $N workers..."
for i in $(seq 1 "$N"); do
  ./bin/worker -master "$MASTER" -id "worker-$i" &
  echo "  worker-$i started (pid $!)"
done

wait
