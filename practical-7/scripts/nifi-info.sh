#!/usr/bin/env bash
set -euo pipefail

echo "NiFi URL: https://localhost:8443/nifi"
echo "Username: admin"
echo "Password: admin"
echo
echo "Kafka brokers for NiFi:"
echo "kafka-0:9092,kafka-1:9092,kafka-2:9092"
echo "Topic: practical7-messages"
echo
echo "Configure ConsumeKafka_2_6 -> LogAttribute (or PutFile) in NiFi."
