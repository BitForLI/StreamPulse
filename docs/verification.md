# Verification status

Last updated: 2026-08-29 on Windows 11.

## Passed

- JSON Schema Draft 2020-12 validation: 7/7 contract tests.
- Privacy, semantic, bad-fixture, and optional-field compatibility checks.
- Root Compose overlay configuration parsing with Docker Compose
  `v2.40.3-desktop.1` (Docker config emitted a host permission warning).
- Generator compilation and Go tests, including deterministic sequence,
  controlled duplicates/schema errors, all five fault labels, and key spread.
- Full 3,000,000-request generator run repeated with identical manifest SHA-256
  `ff5b6b60361ea1885215ac838bcbeb6eb56d25ebcd7e6faf306e7983bc9b4792`.
- 684-record generator JSONL smoke checked against input schemas and privacy.
- Flink 1.16 job compilation and 5/5 hand-calculated Java tests.
- Upstream Flink Operations Playground runtime through the documented official
  Kafka-image substitution: Kafka healthy, one TaskManager registered, job
  `RUNNING`, and real output windows consumed.
- Upstream TaskManager recovery: all four vertices returned to `RUNNING` in 6
  seconds with the same Job ID and increasing Kafka offsets.
- Eight StreamPulse topics created on the real broker with the documented
  partition counts and retention; all six delivery partitions received data.
- Generator -> Kafka -> StreamPulse Flink -> node/network/content/DLQ topics:
  source lag reached zero, all six vertices stayed `RUNNING`, 19/19 observed
  checkpoints completed, and the DLQ delta exactly matched 219 injected schema
  errors.
- Generator -> Kafka -> Flink -> ClickHouse materialized views: all six Flink
  vertices stayed `RUNNING`, delivery source lag reached zero, 21/21 observed
  checkpoints completed, and all eight ClickHouse Kafka consumers reported no
  exceptions.
- Query-visible ClickHouse rows after the fixed-seed run: 22,584 delivery,
  22,800 routing, 5,700 player, 84 node-minute, 28 network-minute, 250
  content-five-minute, and exactly 219 dead-letter records.
- Grafana `13.1.3` health, provisioned ClickHouse datasource health, and the
  provisioned 20-panel dashboard API passed. All 20 panel SQL statements were
  executed directly against ClickHouse: 20 passed, 0 failed.
- Three required ClickHouse query baselines completed with one cold and 20 hot
  iterations each. Hot P95 was 27.857 ms for node quality, 12.170 ms for
  location/network anomalies, and 23.337 ms for content across nodes on this
  local synthetic dataset; rows and bytes are preserved in the Day 4 report.
- Kafka generator sink batching tests and runtime smoke: bounded 500-record
  synchronous batches, final partial flush, and the 684-record smoke completed
  in 3.421 seconds after removing per-record broker round trips.
- Recommendation API module verification, all Go package tests, `go vet`, and
  Windows binary build passed. Tests cover rule and past-window EWMA/MAD
  detection, normal-window suppression, stale data, minimum samples, capacity
  filtering, two-candidate minimum, maximum 0.20 weight step, two-minute TTL,
  one-minute dwell, failed-publish behavior, parameterized ClickHouse queries,
  and HTTP 400/503 behavior.
- Recommendation API read-only smoke against the real local ClickHouse passed:
  liveness `ok`, readiness `ready`, three scope nodes returned, and the latest
  recovered normal window returned `generated=false` with `NO_ANOMALY`.

## Not yet passed

- Checkpoint restore, duplicate behavior across restart, idle partition, and
  watermark/allowed-lateness integration tests.
- ClickHouse pause/materialized-view catch-up (scenario implemented but not yet
  executed), injected-fault recommendation publication through Kafka,
  recommendation ack/outcome durability, model comparison, and EdgeRoute
  shadow integration.

The upstream runtime pass does not substitute for the remaining StreamPulse
integration gates.
