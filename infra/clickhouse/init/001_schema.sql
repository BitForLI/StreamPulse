CREATE DATABASE IF NOT EXISTS streampulse;

CREATE TABLE IF NOT EXISTS streampulse.raw_delivery
(
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    event_id String,
    request_id String,
    session_id_hash String,
    content_id String,
    object_type LowCardinality(String),
    node_id LowCardinality(String),
    location LowCardinality(String),
    network_id LowCardinality(String),
    cache_status LowCardinality(String),
    http_status UInt16,
    bytes_sent UInt64,
    ttfb_ms Float32,
    transfer_ms Float32,
    origin_ms Float32,
    segment_duration_ms UInt32,
    bitrate_bps UInt32,
    synthetic_node_rps UInt32,
    synthetic_capacity_rps UInt32,
    synthetic_labels Array(String),
    stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(stored_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (location, node_id, event_time, event_id)
TTL event_time + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.node_metrics_1m
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    revision UInt32,
    location LowCardinality(String),
    network_id LowCardinality(String),
    node_id LowCardinality(String),
    requests UInt64,
    error_5xx_rate Float64,
    cache_hit_ratio Float64,
    bytes_sent UInt64,
    origin_ms_total Float64,
    ttfb_p50_ms Float64,
    ttfb_p95_ms Float64,
    ttfb_p99_ms Float64
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (location, network_id, node_id, window_start)
TTL window_start + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.network_metrics_1m
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    revision UInt32,
    location LowCardinality(String),
    network_id LowCardinality(String),
    requests UInt64,
    error_5xx_rate Float64,
    cache_hit_ratio Float64,
    bytes_sent UInt64,
    origin_ms_total Float64,
    ttfb_p50_ms Float64,
    ttfb_p95_ms Float64,
    ttfb_p99_ms Float64
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (location, network_id, window_start)
TTL window_start + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.content_metrics_5m
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    revision UInt32,
    content_id String,
    requests UInt64,
    cache_hit_ratio Float64,
    bytes_sent UInt64,
    origin_ms_total Float64
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (content_id, window_start)
TTL window_start + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.raw_routing
(
    event_time DateTime64(3, 'UTC'),
    event_id String,
    request_id String,
    selected_node LowCardinality(String),
    policy LowCardinality(String),
    quality_snapshot_age_ms UInt32,
    fallback_level UInt8,
    reason_codes Array(String),
    stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(stored_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, request_id, event_id)
TTL event_time + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.raw_player
(
    event_time DateTime64(3, 'UTC'),
    event_id String,
    session_id_hash String,
    content_id String,
    event_type LowCardinality(String),
    segment_sequence UInt32,
    selected_bitrate_bps UInt32,
    download_ms Float32,
    buffer_ms Float32,
    rebuffer_ms Nullable(Float32),
    stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(stored_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, session_id_hash, segment_sequence, event_id)
TTL event_time + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.recommendations
(
    recommendation_id String,
    created_at DateTime64(3, 'UTC'),
    valid_until DateTime64(3, 'UTC'),
    revision UInt32,
    mode LowCardinality(String),
    location LowCardinality(String),
    network_id LowCardinality(String),
    action LowCardinality(String),
    current_json String,
    proposed_json String,
    evidence_window LowCardinality(String),
    reason_codes Array(String),
    expected_p95_ttfb_delta_ms Float64,
    expected_error_rate_delta Float64,
    expected_cost_units_delta Float64,
    confidence Float64,
    model_version LowCardinality(String)
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(created_at)
ORDER BY (location, network_id, created_at, recommendation_id)
TTL created_at + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS streampulse.dead_letters
(
    observed_at DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    error_code LowCardinality(String),
    error_message String,
    payload_sha256 FixedString(64),
    payload_preview_redacted String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(observed_at)
ORDER BY (source_topic, error_code, observed_at, payload_sha256)
TTL observed_at + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS streampulse.kafka_delivery (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.delivery.v1',
         kafka_group_name = 'streampulse-clickhouse-delivery-v1', kafka_format = 'JSONAsString',
         kafka_num_consumers = 1, kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_delivery TO streampulse.raw_delivery AS
SELECT
    parseDateTime64BestEffort(JSONExtractString(raw, 'event_time'), 3, 'UTC') AS event_time,
    parseDateTime64BestEffort(JSONExtractString(raw, 'ingest_time'), 3, 'UTC') AS ingest_time,
    JSONExtractString(raw, 'event_id') AS event_id,
    JSONExtractString(raw, 'request_id') AS request_id,
    JSONExtractString(raw, 'session_id_hash') AS session_id_hash,
    JSONExtractString(raw, 'content_id') AS content_id,
    JSONExtractString(raw, 'object_type') AS object_type,
    JSONExtractString(raw, 'node_id') AS node_id,
    JSONExtractString(raw, 'location') AS location,
    JSONExtractString(raw, 'network_id') AS network_id,
    JSONExtractString(raw, 'cache_status') AS cache_status,
    toUInt16(JSONExtractUInt(raw, 'http_status')) AS http_status,
    JSONExtractUInt(raw, 'bytes_sent') AS bytes_sent,
    toFloat32(JSONExtractFloat(raw, 'ttfb_ms')) AS ttfb_ms,
    toFloat32(JSONExtractFloat(raw, 'transfer_ms')) AS transfer_ms,
    toFloat32(JSONExtractFloat(raw, 'origin_ms')) AS origin_ms,
    toUInt32(JSONExtractUInt(raw, 'segment_duration_ms')) AS segment_duration_ms,
    toUInt32(JSONExtractUInt(raw, 'bitrate_bps')) AS bitrate_bps,
    toUInt32(JSONExtractUInt(raw, 'synthetic_node_rps')) AS synthetic_node_rps,
    toUInt32(JSONExtractUInt(raw, 'synthetic_capacity_rps')) AS synthetic_capacity_rps,
    JSONExtract(raw, 'synthetic_labels', 'Array(String)') AS synthetic_labels
FROM streampulse.kafka_delivery
WHERE JSONExtractUInt(raw, 'schema_version') = 1
  AND event_id != '' AND request_id != '' AND node_id != '' AND location != '' AND network_id != ''
  AND cache_status IN ('HIT', 'MISS', 'BYPASS', 'EXPIRED', 'STALE', 'UPDATING', 'REVALIDATED', 'UNKNOWN')
  AND http_status BETWEEN 100 AND 599;

CREATE TABLE IF NOT EXISTS streampulse.kafka_node_metrics (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.metrics.node.1m.v1',
         kafka_group_name = 'streampulse-clickhouse-node-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_node_metrics TO streampulse.node_metrics_1m AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw, 'window_start'), 3, 'UTC') AS window_start,
       parseDateTime64BestEffort(JSONExtractString(raw, 'window_end'), 3, 'UTC') AS window_end,
       toUInt32(JSONExtractUInt(raw, 'revision')) AS revision,
       JSONExtractString(raw, 'location') AS location, JSONExtractString(raw, 'network_id') AS network_id,
       JSONExtractString(raw, 'node_id') AS node_id, JSONExtractUInt(raw, 'requests') AS requests,
       JSONExtractFloat(raw, 'error_5xx_rate') AS error_5xx_rate,
       JSONExtractFloat(raw, 'cache_hit_ratio') AS cache_hit_ratio,
       JSONExtractUInt(raw, 'bytes_sent') AS bytes_sent,
       JSONExtractFloat(raw, 'origin_ms_total') AS origin_ms_total,
       JSONExtractFloat(raw, 'ttfb_p50_ms') AS ttfb_p50_ms,
       JSONExtractFloat(raw, 'ttfb_p95_ms') AS ttfb_p95_ms,
       JSONExtractFloat(raw, 'ttfb_p99_ms') AS ttfb_p99_ms
FROM streampulse.kafka_node_metrics WHERE JSONExtractUInt(raw, 'schema_version') = 1;

CREATE TABLE IF NOT EXISTS streampulse.kafka_network_metrics (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.metrics.network.1m.v1',
         kafka_group_name = 'streampulse-clickhouse-network-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_network_metrics TO streampulse.network_metrics_1m AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw, 'window_start'), 3, 'UTC') AS window_start,
       parseDateTime64BestEffort(JSONExtractString(raw, 'window_end'), 3, 'UTC') AS window_end,
       toUInt32(JSONExtractUInt(raw, 'revision')) AS revision,
       JSONExtractString(raw, 'location') AS location, JSONExtractString(raw, 'network_id') AS network_id,
       JSONExtractUInt(raw, 'requests') AS requests,
       JSONExtractFloat(raw, 'error_5xx_rate') AS error_5xx_rate,
       JSONExtractFloat(raw, 'cache_hit_ratio') AS cache_hit_ratio,
       JSONExtractUInt(raw, 'bytes_sent') AS bytes_sent,
       JSONExtractFloat(raw, 'origin_ms_total') AS origin_ms_total,
       JSONExtractFloat(raw, 'ttfb_p50_ms') AS ttfb_p50_ms,
       JSONExtractFloat(raw, 'ttfb_p95_ms') AS ttfb_p95_ms,
       JSONExtractFloat(raw, 'ttfb_p99_ms') AS ttfb_p99_ms
FROM streampulse.kafka_network_metrics WHERE JSONExtractUInt(raw, 'schema_version') = 1;

CREATE TABLE IF NOT EXISTS streampulse.kafka_content_metrics (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.metrics.content.5m.v1',
         kafka_group_name = 'streampulse-clickhouse-content-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_content_metrics TO streampulse.content_metrics_5m AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw, 'window_start'), 3, 'UTC') AS window_start,
       parseDateTime64BestEffort(JSONExtractString(raw, 'window_end'), 3, 'UTC') AS window_end,
       toUInt32(JSONExtractUInt(raw, 'revision')) AS revision,
       JSONExtractString(raw, 'content_id') AS content_id, JSONExtractUInt(raw, 'requests') AS requests,
       JSONExtractFloat(raw, 'cache_hit_ratio') AS cache_hit_ratio,
       JSONExtractUInt(raw, 'bytes_sent') AS bytes_sent,
       JSONExtractFloat(raw, 'origin_ms_total') AS origin_ms_total
FROM streampulse.kafka_content_metrics WHERE JSONExtractUInt(raw, 'schema_version') = 1;

CREATE TABLE IF NOT EXISTS streampulse.kafka_routing (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.routing.v1',
         kafka_group_name = 'streampulse-clickhouse-routing-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_routing TO streampulse.raw_routing AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw, 'event_time'), 3, 'UTC') AS event_time,
       JSONExtractString(raw, 'event_id') AS event_id, JSONExtractString(raw, 'request_id') AS request_id,
       JSONExtractString(raw, 'selected_node') AS selected_node, JSONExtractString(raw, 'policy') AS policy,
       toUInt32(JSONExtractUInt(raw, 'quality_snapshot_age_ms')) AS quality_snapshot_age_ms,
       toUInt8(JSONExtractUInt(raw, 'fallback_level')) AS fallback_level,
       JSONExtract(raw, 'reason_codes', 'Array(String)') AS reason_codes
FROM streampulse.kafka_routing WHERE JSONExtractUInt(raw, 'schema_version') = 1;

CREATE TABLE IF NOT EXISTS streampulse.kafka_player (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.player.v1',
         kafka_group_name = 'streampulse-clickhouse-player-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_player TO streampulse.raw_player AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw, 'event_time'), 3, 'UTC') AS event_time,
       JSONExtractString(raw, 'event_id') AS event_id, JSONExtractString(raw, 'session_id_hash') AS session_id_hash,
       JSONExtractString(raw, 'content_id') AS content_id, JSONExtractString(raw, 'event_type') AS event_type,
       toUInt32(JSONExtractUInt(raw, 'segment_sequence')) AS segment_sequence,
       toUInt32(JSONExtractUInt(raw, 'selected_bitrate_bps')) AS selected_bitrate_bps,
       toFloat32(JSONExtractFloat(raw, 'download_ms')) AS download_ms,
       toFloat32(JSONExtractFloat(raw, 'buffer_ms')) AS buffer_ms,
       if(JSONHas(raw, 'rebuffer_ms'), toNullable(toFloat32(JSONExtractFloat(raw, 'rebuffer_ms'))), NULL) AS rebuffer_ms
FROM streampulse.kafka_player WHERE JSONExtractUInt(raw, 'schema_version') = 1;

CREATE TABLE IF NOT EXISTS streampulse.kafka_recommendations (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.recommendations.v1',
         kafka_group_name = 'streampulse-clickhouse-recommendations-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_recommendations TO streampulse.recommendations AS
SELECT JSONExtractString(raw, 'recommendation_id') AS recommendation_id,
       parseDateTime64BestEffort(JSONExtractString(raw, 'created_at'), 3, 'UTC') AS created_at,
       parseDateTime64BestEffort(JSONExtractString(raw, 'valid_until'), 3, 'UTC') AS valid_until,
       toUInt32(JSONExtractUInt(raw, 'revision')) AS revision,
       JSONExtractString(raw, 'mode') AS mode,
       JSONExtractString(JSONExtractRaw(raw, 'scope'), 'location') AS location,
       JSONExtractString(JSONExtractRaw(raw, 'scope'), 'network_id') AS network_id,
       JSONExtractString(raw, 'action') AS action,
       JSONExtractRaw(raw, 'current') AS current_json, JSONExtractRaw(raw, 'proposed') AS proposed_json,
       JSONExtractString(raw, 'evidence_window') AS evidence_window,
       JSONExtract(raw, 'reason_codes', 'Array(String)') AS reason_codes,
       JSONExtractFloat(JSONExtractRaw(raw, 'expected'), 'p95_ttfb_delta_ms') AS expected_p95_ttfb_delta_ms,
       JSONExtractFloat(JSONExtractRaw(raw, 'expected'), 'error_rate_delta') AS expected_error_rate_delta,
       JSONExtractFloat(JSONExtractRaw(raw, 'expected'), 'cost_units_delta') AS expected_cost_units_delta,
       JSONExtractFloat(raw, 'confidence') AS confidence,
       JSONExtractString(raw, 'model_version') AS model_version
FROM streampulse.kafka_recommendations
WHERE JSONExtractUInt(raw, 'schema_version') = 1 AND JSONExtractString(raw, 'mode') = 'shadow';

CREATE TABLE IF NOT EXISTS streampulse.kafka_dead_letters (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'cdn.dead-letter.v1',
         kafka_group_name = 'streampulse-clickhouse-dlq-v1', kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS streampulse.mv_dead_letters TO streampulse.dead_letters AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw, 'observed_at'), 3, 'UTC') AS observed_at,
       JSONExtractString(raw, 'source_topic') AS source_topic,
       JSONExtractString(raw, 'error_code') AS error_code,
       JSONExtractString(raw, 'error_message') AS error_message,
       toFixedString(JSONExtractString(raw, 'payload_sha256'), 64) AS payload_sha256,
       JSONExtractString(raw, 'payload_preview_redacted') AS payload_preview_redacted
FROM streampulse.kafka_dead_letters
WHERE length(JSONExtractString(raw, 'payload_sha256')) = 64;
