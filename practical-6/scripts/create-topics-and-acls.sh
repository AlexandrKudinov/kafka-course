#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP='kafka-0:9093,kafka-1:9093,kafka-2:9093'
KAFKA='docker exec practical6-kafka-0'

for i in {1..30}; do
  status=$(docker inspect --format='{{.State.Health.Status}}' practical6-kafka-0 2>/dev/null || true)
  if [ "$status" = "healthy" ]; then break; fi
  sleep 2
done
if [ "${status:-}" != "healthy" ]; then
  echo 'Kafka broker is not healthy.'
  exit 1
fi
$KAFKA kafka-topics --create --if-not-exists --topic topic-1 --partitions 3 --replication-factor 3 --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties
$KAFKA kafka-topics --create --if-not-exists --topic topic-2 --partitions 3 --replication-factor 3 --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties
$KAFKA kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation WRITE --operation DESCRIBE --topic topic-1
$KAFKA kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation WRITE --operation DESCRIBE --topic topic-2
$KAFKA kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation READ --operation DESCRIBE --topic topic-1
$KAFKA kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation READ --group practical-6-consumer
$KAFKA kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --list
