#!/usr/bin/env bash
set -euo pipefail
curl -sS -X PUT -H 'Content-Type: application/json' \
  --data @final-project/connect/file-sink.json \
  http://localhost:8084/connectors/final-file-sink/config
printf '\nStatus:\n'
curl -sS http://localhost:8084/connectors/final-file-sink/status
printf '\n'
