# Event-time semantics

- `event_time` is when the observable CDN/player event occurred and drives
  business windows.
- `ingest_time` is collector arrival time and must not precede `event_time` in
  synthetic DeliveryEvent fixtures.
- Kafka record timestamps are transport metadata, not the business-time source
  of truth.
- Producers emit UTC RFC 3339 timestamps.
- Out-of-order events inside the configured watermark bound update open
  windows. Later records follow an explicit late-data policy and are measured.
- Aggregate records carry deterministic window identity and revision so replay
  can be reconciled instead of silently double-counted.

The exact watermark delay and allowed lateness will be fixed together with the
Flink job and tested against hand-calculated fixtures; they are intentionally
not claimed before that implementation exists.
