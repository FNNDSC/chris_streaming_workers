#!/bin/bash
# ============================================================================
# Kafka initialization: create topics and ACLs.
#
# This script runs in a Kafka container after the broker is ready.
# It uses KRaft mode (no ZooKeeper) with SASL/PLAIN authentication.
# Users are defined in the JAAS config file (kafka_server_jaas.conf).
#
# For production: switch to SASL_SSL + SCRAM-SHA-512 (see README.md).
# ============================================================================

set -euo pipefail

BOOTSTRAP="kafka:9092"
KAFKA_BIN="/opt/kafka/bin"

echo "=== Waiting for Kafka broker to be ready ==="
until ${KAFKA_BIN}/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" \
    --command-config /etc/kafka/admin.properties >/dev/null 2>&1; do
    echo "  Kafka not ready, retrying in 3s..."
    sleep 3
done
echo "Kafka broker is ready."

# ----------------------------------------------------------------------------
# Topics
# Partitioned by job_id (Kafka default partitioner: murmur2 hash of key).
# 12 partitions allows up to 12 consumer instances per group.
# Retention: 3 days = 259200000 ms.
# ----------------------------------------------------------------------------
echo "=== Creating topics ==="

create_topic() {
    local topic=$1 partitions=$2 retention=$3
    ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --command-config /etc/kafka/admin.properties \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config "retention.ms=${retention}" \
        --config "cleanup.policy=delete"
    echo "  Created topic: $topic (partitions=$partitions, retention=${retention}ms)"
}

create_topic "job-status-events"     12 259200000   # 3 days
create_topic "job-logs"              12 259200000   # 3 days
create_topic "job-status-events-dlq"  3 604800000   # 7 days
create_topic "job-logs-dlq"           3 604800000   # 7 days

# ----------------------------------------------------------------------------
# ACLs
# Restrict each user to only the operations they need.
# Users are defined in kafka_server_jaas.conf (SASL/PLAIN).
# ----------------------------------------------------------------------------
echo "=== Configuring ACLs ==="

acl() {
    ${KAFKA_BIN}/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" \
        --command-config /etc/kafka/admin.properties \
        --add "$@"
}

# event-forwarder: writes to job-status-events and job-logs (EOS markers)
acl --allow-principal "User:event-forwarder" \
    --operation Write --operation Describe \
    --topic "job-status-events"
acl --allow-principal "User:event-forwarder" \
    --operation Write --operation Describe \
    --topic "job-logs"

# log-producer: writes to job-logs (used by Fluent Bit)
acl --allow-principal "User:log-producer" \
    --operation Write --operation Describe \
    --topic "job-logs"

# status-consumer: reads from job-status-events, writes to DLQ
acl --allow-principal "User:status-consumer" \
    --operation Read --operation Describe \
    --topic "job-status-events"
acl --allow-principal "User:status-consumer" \
    --operation Read \
    --group "status-consumer-group"
acl --allow-principal "User:status-consumer" \
    --operation Write --operation Describe \
    --topic "job-status-events-dlq"

# log-consumer: reads from job-logs, writes to DLQ
acl --allow-principal "User:log-consumer" \
    --operation Read --operation Describe \
    --topic "job-logs"
acl --allow-principal "User:log-consumer" \
    --operation Read \
    --group "log-consumer-group"
acl --allow-principal "User:log-consumer" \
    --operation Write --operation Describe \
    --topic "job-logs-dlq"

echo "=== Kafka initialization complete ==="
