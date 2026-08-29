# Kafka topics and partition contracts

The local MVP starts with three to six partitions per raw topic. Partition
counts are changed only after measuring records/s, broker bytes, consumer lag,
Flink source parallelism, and maximum-to-median key load.

| Topic | Record key | Purpose | Local retention |
|---|---|---|---:|
| `cdn.delivery.v1` | `location|network_id|stable_bucket` | CDN object delivery | 24h |
| `cdn.routing.v1` | `request_id` | routing decision and evidence | 24h |
| `cdn.player.v1` | `session_id_hash` | ordered player QoE events | 24h |
| `cdn.metrics.node.1m.v1` | `window_start|node_id` | node minute metrics | 7d |
| `cdn.metrics.network.1m.v1` | `window_start|location|network_id` | network minute metrics | 7d |
| `cdn.metrics.content.5m.v1` | `window_start|content_id` | content demand metrics | 7d |
| `cdn.recommendations.v1` | `location|network_id` | shadow recommendations | 30d |
| `cdn.dead-letter.v1` | `source_topic` | redacted parse failures | 7d |

## Key choices

Delivery records are not keyed by `content_id`: a viral object would create a
hot partition. The mature delivery key adds a deterministic session-derived
bucket while preserving location/network locality. Player events remain keyed
by `session_id_hash` because per-session order is required.

## Delivery semantics

The pipeline is at-least-once. Each raw event has an `event_id`; consumers use
that identity plus versioned window revisions for application-level deduplication
and idempotent ClickHouse reads. A breaking schema change requires a new major
schema and normally a new topic.

## Dead-letter records

DLQ records contain source topic, partition, offset, observation time, bounded
error code/message, payload SHA-256, and a redacted preview. Raw payloads are not
stored because they may contain sensitive data.
