#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka-0:9092,kafka-1:9092,kafka-2:9092"
TOPIC="practical7-messages"

echo "Waiting for Kafka..."
until docker exec practical7-kafka-0 kafka-broker-api-versions --bootstrap-server kafka-0:9092 >/dev/null 2>&1; do
  sleep 2
done

docker exec practical7-kafka-0 kafka-topics \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000 \
  --config segment.bytes=1073741824 \
  --bootstrap-server "$BOOTSTRAP"

echo "Topic:"
docker exec practical7-kafka-0 kafka-topics \
  --describe --topic "$TOPIC" --bootstrap-server "$BOOTSTRAP"

echo "Schema Registry:"
curl -sS -X POST \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data-binary @practical-7/schema/register-schema.json \
  http://localhost:8081/subjects/practical7-message-value/versions

echo
curl -sS http://localhost:8081/subjects
echo
