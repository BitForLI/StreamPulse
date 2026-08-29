#!/usr/bin/env sh
set -eu

bootstrap_server="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"

create_topic() {
  topic="$1"
  partitions="$2"
  retention_ms="$3"
  kafka-topics.sh \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "retention.ms=$retention_ms"
}

create_topic cdn.delivery.v1 6 86400000
create_topic cdn.routing.v1 6 86400000
create_topic cdn.player.v1 6 86400000
create_topic cdn.metrics.node.1m.v1 3 604800000
create_topic cdn.metrics.network.1m.v1 3 604800000
create_topic cdn.metrics.content.5m.v1 3 604800000
create_topic cdn.recommendations.v1 3 2592000000
create_topic cdn.dead-letter.v1 3 604800000

kafka-topics.sh --bootstrap-server "$bootstrap_server" --list
