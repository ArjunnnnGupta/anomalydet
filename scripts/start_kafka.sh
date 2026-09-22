#!/usr/bin/env bash
# scripts/start_kafka.sh — Start the Kafka broker via Docker Compose.
# Requires Docker Desktop to be running.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$(dirname "$SCRIPT_DIR")/docker/kafka-compose.yml"

echo "==> Starting Kafka (KRaft mode, single broker, heap capped at 512MB) ..."
docker compose -f "$COMPOSE_FILE" up -d

echo "==> Waiting for Kafka to become healthy ..."
for i in $(seq 1 30); do
    if docker inspect kafka --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; then
        echo "==> Kafka is healthy and ready."
        exit 0
    fi
    echo "    ... waiting ($i/30)"
    sleep 2
done

echo "ERROR: Kafka did not become healthy in 60s. Check: docker logs kafka"
exit 1

