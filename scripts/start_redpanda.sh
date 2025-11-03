#!/usr/bin/env bash

# Start the Redpanda container and optionally create the ticks topic.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR%/scripts}"

cd "${PROJECT_ROOT}"

echo "Starting Redpanda via docker compose..."
docker compose up -d

# Wait briefly for the broker to come online before calling rpk inside the container.
echo "Waiting for Redpanda to become ready..."
ready=false
for attempt in {1..10}; do
  if docker compose exec -T redpanda rpk cluster health >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done

if [ "${ready}" = true ]; then
  echo "Creating Kafka topic 'ticks' with rpk (if absent)..."
  docker compose exec -T redpanda rpk topic create ticks --replicas 1 --partitions 1 || true
else
  echo "Redpanda did not report healthy in time; the producer will auto-create 'ticks' on first publish."
fi
