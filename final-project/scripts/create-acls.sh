#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose-final-project.yml"
BOOTSTRAP="kafka-0:9093,kafka-1:9093,kafka-2:9093"
ADMIN="/etc/kafka/client-config/admin.properties"

kafka_acl() {
  $COMPOSE exec -T kafka-0 kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" "$@"
}

echo "Waiting for Kafka..."
for i in $(seq 1 30); do
  if kafka_acl --list >/dev/null 2>&1; then break; fi
  if [ "$i" -eq 30 ]; then echo "ERROR: Kafka ACL API is not ready"; exit 1; fi
  sleep 2
done

# SHOP API publishes products.
kafka_acl --add --allow-principal User:shop --operation WRITE --operation DESCRIBE --topic shop-products

# Kafka Streams reads the catalog and the forbidden-product state and writes filtered products.
kafka_acl --add --allow-principal User:streams --operation READ --operation DESCRIBE --topic shop-products
kafka_acl --add --allow-principal User:streams --operation READ --operation WRITE --operation DESCRIBE --topic forbidden-products
kafka_acl --add --allow-principal User:streams --operation WRITE --operation DESCRIBE --topic filtered-products
kafka_acl --add --allow-principal User:streams --operation CREATE --cluster
kafka_acl --add --allow-principal User:streams --operation READ --operation WRITE --operation DESCRIBE --topic final-project-filter --resource-pattern-type prefixed
kafka_acl --add --allow-principal User:streams --operation READ --group final-project-filter

# CLIENT API publishes search events and consumes already prepared recommendations.
kafka_acl --add --allow-principal User:client --operation WRITE --operation DESCRIBE --topic client-requests
kafka_acl --add --allow-principal User:client --operation READ --operation DESCRIBE --topic recommendations
kafka_acl --add --allow-principal User:client --operation READ --group-prefix client-recommendations-

# Analytics consumes search events and publishes recommendations.
kafka_acl --add --allow-principal User:analytics --operation READ --operation DESCRIBE --topic client-requests
kafka_acl --add --allow-principal User:analytics --operation WRITE --operation DESCRIBE --topic recommendations
kafka_acl --add --allow-principal User:analytics --operation READ --group analytics-requests
kafka_acl --add --allow-principal User:analytics --operation READ --group analytics-filtered

# Replication service reads filtered data from the primary cluster.
kafka_acl --add --allow-principal User:streams --operation READ --group final-project-replicator

kafka_acl --list
