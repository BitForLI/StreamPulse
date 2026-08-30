# Event-time semantics

- `event_time` is when the observable CDN/player event occurred and drives
  business windows.
- `ingest_time` is collector arrival time and must not precede `event_time` in
  synthetic DeliveryEvent fixtures.
- Kafka record timestamps are transport metadata, not the business-time source
  of truth.
- Producers emit UTC RFC 3339 timestamps.
- The delivery stream uses a 10-second bounded-out-of-orderness watermark and
  marks a source partition idle after 30 seconds.
- Events arriving after the watermark but within five seconds of allowed
  lateness correct the aggregate and increment `revision`. Events beyond that
  boundary enter the redacted `TOO_LATE` audit path and do not change the
  aggregate.
- Aggregate records carry deterministic window identity and revision so replay
  can be reconciled instead of silently double-counted.

The isolated two-partition integration result is recorded in
`experiments/results/watermark-lateness/`; one partition was intentionally
empty, so the test also covers idle-partition watermark progress.
