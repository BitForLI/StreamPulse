# StreamPulse v1 data dictionary

All durations are milliseconds unless the field name states otherwise. All
timestamps are RFC 3339 UTC values. Public fixtures are synthetic.

## Shared identifiers

| Field | Meaning | Governance |
|---|---|---|
| `schema_version` | Contract major version; v1 is the integer `1` | Required |
| `event_id` | Unique producer event identifier | Synthetic; required for input events |
| `event_time` | Time the observed event occurred | Drives Flink business windows |
| `ingest_time` | Time the delivery event entered collection | Used for collection delay |
| `request_id` | Correlates routing and delivery records | Synthetic opaque value |
| `session_id_hash` | One-way session correlation identifier | `sha256:` plus 64 lowercase hex characters |
| `trace_id` | Optional distributed trace correlation value | Synthetic/opaque; never a user identifier |

## DeliveryEvent

One event per CDN object request.

| Field | Type/unit | Meaning |
|---|---|---|
| `content_id` | string | Synthetic or hashed content identity |
| `object_type` | enum | `manifest`, `segment`, `image`, `other`, or `UNKNOWN` |
| `path_template` | string | Query-free low-cardinality path template |
| `node_id` | string | Edge cache node identity |
| `location` | string | Logical region/location identity |
| `network_id` | string | Synthetic/anonymous network identity; never a full IP |
| `cache_status` | enum | Normalized cache outcome including `HIT`, `MISS`, and `UNKNOWN` |
| `http_status` | integer | HTTP status in the range 100--599 |
| `bytes_sent` | integer/bytes | Non-negative response bytes |
| `ttfb_ms` | number/ms | Time to first response byte |
| `transfer_ms` | number/ms | Response transfer duration |
| `origin_ms` | number/ms | Optional origin fetch duration |
| `segment_duration_ms` | integer/ms | Optional media duration represented by a segment |
| `bitrate_bps` | integer/bits per second | Optional selected media bitrate |
| `synthetic_node_rps` | integer/requests per second | Generator-only node load label |
| `synthetic_capacity_rps` | integer/requests per second | Generator-only configured node capacity |

## RoutingEvent

One event per EdgeRoute decision.

| Field | Type/unit | Meaning |
|---|---|---|
| `candidate_nodes` | unique string array | Nodes eligible before final selection |
| `selected_node` | string | Node selected by the routing policy |
| `policy` | enum | Versioned routing policy identity |
| `quality_snapshot_age_ms` | integer/ms | Age of the quality snapshot used by the decision |
| `selected_weight` | number | Effective selected-node weight in `[0,1]` |
| `fallback_level` | integer | Zero for primary scope; larger values are deeper fallback |
| `reason_codes` | unique string array | Bounded auditable decision reasons |

## PlayerEvent

Synthetic player/k6 events are the only contract that directly represents
client playback experience.

| Field | Type/unit | Meaning |
|---|---|---|
| `event_type` | enum | Player lifecycle or error event |
| `segment_sequence` | integer | Optional non-negative media sequence |
| `selected_bitrate_bps` | integer/bits per second | Optional selected rendition bitrate |
| `download_ms` | number/ms | Optional object download duration |
| `buffer_ms` | number/ms | Optional buffer remaining after the event |
| `startup_ms` | number/ms or null | Startup delay when applicable |
| `rebuffer_ms` | number/ms or null | Rebuffer duration when applicable |

## RecommendationEvent

Auditable shadow output; it is not a direct DNS request-path dependency.

| Field | Type/unit | Meaning |
|---|---|---|
| `recommendation_id` | string | Unique recommendation identifier |
| `created_at`, `valid_until` | timestamp | Creation and strict expiry; expiry must be later |
| `mode` | enum | MVP permits only `shadow` |
| `scope` | object | Location and synthetic/anonymous network scope |
| `action` | enum | Weight adjustment, prefetch, or no-op action |
| `current`, `proposed` | node-to-number map | Current and proposed bounded node weights |
| `evidence_window` | duration string | Completed metric window such as `5m` |
| `reason_codes` | unique string array | Bounded evidence/guardrail reasons |
| `expected` | object | Estimated QoE proxy, error, and cost-unit deltas |
| `confidence` | number | Evidence confidence in `[0,1]`; not causal proof |
| `model_version` | string | Versioned rule/statistical/model identity |
