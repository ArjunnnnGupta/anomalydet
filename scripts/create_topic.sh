#!/usr/bin/env bash
# scripts/create_topic.sh — Create the Kafka topic 'network-flows' (2 partitions).
# Run after start_kafka.sh confirms Kafka is healthy.

set -e
TOPIC="network-flows"
BOOTSTRAP="localhost:9092"
PARTITIONS=2

echo "==> Creating Kafka topic: $TOPIC (partitions=$PARTITIONS) ..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --bootstrap-server "$BOOTSTRAP" \
    --partitions "$PARTITIONS" \
    --replication-factor 1

echo "==> Topic list:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --list \
    --bootstrap-server "$BOOTSTRAP"

echo "==> Done. Topic '$TOPIC' is ready."

