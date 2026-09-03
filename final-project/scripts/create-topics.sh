#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose-final-project.yml"
ADMIN="/etc/kafka/client-config/admin.properties"

SECONDARY_BOOTSTRAP="secondary-kafka-0:9092,secondary-kafka-1:9092,secondary-kafka-2:9092"

BOOTSTRAP="kafka-0:9093,kafka-1:9093,kafka-2:9093"

run_checked() {
  local timeout_seconds="$1"
  shift
  "$@" &
  local pid=$!
  local started
  started=$(date +%s)
  while kill -0 "$pid" 2>/dev/null; do
    if [ $(( $(date +%s) - started )) -ge "$timeout_seconds" ]; then
      echo "ERROR: command timed out after ${timeout_seconds}s"
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
  done
  wait "$pid"
}

kafka_exec() {
  $COMPOSE exec -T kafka-0 "$@"
}

secondary_exec() {
  $COMPOSE exec -T secondary-kafka-0 "$@"
}

echo "Waiting for all Kafka brokers..."
for i in $(seq 1 60); do
  if kafka_exec kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" >/dev/null 2>&1; then
    echo "Kafka is ready."
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: Kafka did not become ready within 60 seconds."
    exit 1
  fi
  sleep 2
done

create_topic() {
  local name="$1"
  local partitions="$2"
  shift 2
  echo "Creating topic: $name"
  run_checked 30 kafka_exec kafka-topics --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 3 \
    --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" "$@"
}

create_topic shop-products 3
create_topic forbidden-products 1 --config cleanup.policy=compact
create_topic filtered-products 3
create_topic client-requests 3
create_topic recommendations 3

echo
run_checked 30 kafka_exec kafka-topics --list --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN"

echo
echo "Waiting for all secondary Kafka brokers..."
for i in $(seq 1 60); do
  if secondary_exec kafka-broker-api-versions --bootstrap-server "$SECONDARY_BOOTSTRAP" >/dev/null 2>&1; then
    echo "Secondary Kafka is ready."
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: secondary Kafka did not become ready within 60 seconds."
    exit 1
  fi
  sleep 2
done

run_checked 30 secondary_exec kafka-topics --create --if-not-exists \
  --topic filtered-products --partitions 3 --replication-factor 3 \
  --bootstrap-server "$SECONDARY_BOOTSTRAP"

echo
echo "Topic descriptions:"
for topic in shop-products forbidden-products filtered-products client-requests recommendations; do
  run_checked 30 kafka_exec kafka-topics --describe --topic "$topic" \
    --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN"
done
