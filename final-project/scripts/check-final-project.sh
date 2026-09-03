#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose-final-project.yml"

$COMPOSE ps

echo
printf '%s\n' '=== primary topics ==='
$COMPOSE exec -T kafka-0 kafka-topics --list \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /etc/kafka/client-config/admin.properties

echo
printf '%s\n' '=== secondary brokers ==='
$COMPOSE exec -T secondary-kafka-0 kafka-broker-api-versions \
  --bootstrap-server secondary-kafka-0:9092,secondary-kafka-1:9092,secondary-kafka-2:9092 >/dev/null
printf '%s\n' 'secondary Kafka: OK'

echo
printf '%s\n' '=== Kafka Connect ==='
curl -fsS http://localhost:8084/connectors

echo
printf '%s\n' '=== Prometheus ==='
curl -fsS http://localhost:9090/-/ready

echo
printf '%s\n' '=== Grafana ==='
curl -fsS http://localhost:3000/api/health
