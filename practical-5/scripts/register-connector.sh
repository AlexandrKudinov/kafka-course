#!/bin/sh
set -e
curl -X PUT -H 'Content-Type: application/json' \
  --data @practical-5/debezium-connector.json \
  http://localhost:8083/connectors/postgres-shop-connector/config
echo
curl http://localhost:8083/connectors/postgres-shop-connector/status
echo
