#!/usr/bin/env bash
set -u
BOOTSTRAP='kafka-0:9093,kafka-1:9093,kafka-2:9093'
KAFKA='docker exec practical6-kafka-0'
printf '\n=== topic-1: producer should be allowed ===\n'
printf 'message-topic-1\n' | $KAFKA kafka-console-producer --bootstrap-server "$BOOTSTRAP" --producer.config /tmp/producer.properties --topic topic-1
printf '\n=== topic-1: consumer should be allowed ===\n'
$KAFKA timeout 5 kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config /tmp/consumer.properties --topic topic-1 --group practical-6-consumer --from-beginning
printf '\n=== topic-2: producer should be allowed ===\n'
printf 'message-topic-2\n' | $KAFKA kafka-console-producer --bootstrap-server "$BOOTSTRAP" --producer.config /tmp/producer.properties --topic topic-2
printf '\n=== topic-2: consumer should be denied ===\n'
set +e
output=$($KAFKA timeout 8 kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --consumer.config /tmp/consumer.properties --topic topic-2 --group practical-6-consumer --from-beginning 2>&1)
status=$?
set -e
printf '%s\n' "$output"
if printf '%s\n' "$output" | grep -Eq 'AuthorizationException|TopicAuthorizationException|GroupAuthorizationException'; then
  echo "Consumer rejected as expected (exit code $status)."
else
  echo 'ERROR: topic-2 consumer was not rejected by ACL.'
  exit 1
fi
