#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Building master..."
go build -o bin/master ./cmd/master

echo "Starting master (RPC :8080, HTTP :8081)..."
./bin/master -port :8080 -http :8081
