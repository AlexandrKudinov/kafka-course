#!/usr/bin/env bash
set -euo pipefail

BROKER="${1:-kafka-1:19092}"

printf 'user-1:{"userIds":["user-2"]}\n' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server "$BROKER" --topic blocked_users \
  --property parse.key=true --property key.separator=:

printf 'global:{"words":["bad","spam"]}\n' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server "$BROKER" --topic banned_words \
  --property parse.key=true --property key.separator=:

printf 'user-1:{"id":"1","senderId":"user-2","recipientId":"user-1","text":"hello from blocked user"}\n' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server "$BROKER" --topic messages \
  --property parse.key=true --property key.separator=:

printf 'user-1:{"id":"2","senderId":"user-3","recipientId":"user-1","text":"hello bad spam"}\n' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server "$BROKER" --topic messages \
  --property parse.key=true --property key.separator=:
