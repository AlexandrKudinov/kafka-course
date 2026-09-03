#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose-final-project.yml"
BOOTSTRAP="kafka-0:9093,kafka-1:9093,kafka-2:9093"

producer() {
  local config="$1" topic="$2" value="$3"
  printf '%s\n' "$value" | $COMPOSE exec -T kafka-0 kafka-console-producer \
    --bootstrap-server "$BOOTSTRAP" --topic "$topic" \
    --producer.config "$config"
}

echo "shop -> shop-products: WRITE should succeed"
producer /etc/kafka/client-config/shop.properties shop-products acl-test

echo "client -> shop-products: WRITE should fail"
if producer /etc/kafka/client-config/client.properties shop-products acl-test 2>&1; then
  echo "ERROR: client unexpectedly has WRITE access"
  exit 1
else
  echo "OK: client is denied WRITE access"
fi
