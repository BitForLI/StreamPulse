ALTER TABLE streampulse.recommendations
    ADD COLUMN IF NOT EXISTS input_window_start DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC') AFTER model_version;
ALTER TABLE streampulse.recommendations
    ADD COLUMN IF NOT EXISTS input_window_end DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC') AFTER input_window_start;
ALTER TABLE streampulse.recommendations
    ADD COLUMN IF NOT EXISTS query_version LowCardinality(String) DEFAULT '' AFTER input_window_end;
ALTER TABLE streampulse.recommendations
    ADD COLUMN IF NOT EXISTS config_hash String DEFAULT '' AFTER query_version;
ALTER TABLE streampulse.recommendations
    ADD COLUMN IF NOT EXISTS evidence_json String DEFAULT '{}' AFTER config_hash;

DROP VIEW IF EXISTS streampulse.mv_recommendations;

CREATE MATERIALIZED VIEW streampulse.mv_recommendations TO streampulse.recommendations AS
SELECT JSONExtractString(raw, 'recommendation_id') AS recommendation_id,
       parseDateTime64BestEffort(JSONExtractString(raw, 'created_at'), 3, 'UTC') AS created_at,
       parseDateTime64BestEffort(JSONExtractString(raw, 'valid_until'), 3, 'UTC') AS valid_until,
       toUInt32(JSONExtractUInt(raw, 'revision')) AS revision,
       JSONExtractString(raw, 'mode') AS mode,
       JSONExtractString(JSONExtractRaw(raw, 'scope'), 'location') AS location,
       JSONExtractString(JSONExtractRaw(raw, 'scope'), 'network_id') AS network_id,
       JSONExtractString(raw, 'action') AS action,
       JSONExtractRaw(raw, 'current') AS current_json,
       JSONExtractRaw(raw, 'proposed') AS proposed_json,
       JSONExtractString(raw, 'evidence_window') AS evidence_window,
       JSONExtract(raw, 'reason_codes', 'Array(String)') AS reason_codes,
       JSONExtractFloat(JSONExtractRaw(raw, 'expected'), 'p95_ttfb_delta_ms') AS expected_p95_ttfb_delta_ms,
       JSONExtractFloat(JSONExtractRaw(raw, 'expected'), 'error_rate_delta') AS expected_error_rate_delta,
       JSONExtractFloat(JSONExtractRaw(raw, 'expected'), 'cost_units_delta') AS expected_cost_units_delta,
       JSONExtractFloat(raw, 'confidence') AS confidence,
       JSONExtractString(raw, 'model_version') AS model_version,
       parseDateTime64BestEffort(JSONExtractString(raw, 'input_window_start'), 3, 'UTC') AS input_window_start,
       parseDateTime64BestEffort(JSONExtractString(raw, 'input_window_end'), 3, 'UTC') AS input_window_end,
       JSONExtractString(raw, 'query_version') AS query_version,
       JSONExtractString(raw, 'config_hash') AS config_hash,
       JSONExtractRaw(raw, 'evidence') AS evidence_json
FROM streampulse.kafka_recommendations
WHERE JSONExtractUInt(raw, 'schema_version') = 1 AND JSONExtractString(raw, 'mode') = 'shadow';

CREATE TABLE IF NOT EXISTS streampulse.recommendation_acknowledgements
(
    recommendation_id String,
    actor LowCardinality(String),
    status LowCardinality(String),
    observed_at DateTime64(3, 'UTC'),
    stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (recommendation_id, observed_at, actor)
TTL observed_at + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS streampulse.recommendation_outcomes
(
    recommendation_id String,
    observed_at DateTime64(3, 'UTC'),
    observed_p95_ttfb_delta_ms Float64,
    observed_error_rate_delta Float64,
    observed_cost_units_delta Float64,
    notes String,
    stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (recommendation_id, observed_at)
TTL observed_at + INTERVAL 30 DAY;
