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
- Fixed-threshold, combined-rule, and past-only EWMA/MAD detector comparison
  completed across three independently seeded real Kafka -> Flink -> ClickHouse
  runs. Each run saved 156 complete node-minute rows, a generator manifest,
  SHA-256 identifiers, and raw per-window predictions.
- Detector results were retained without selection: fixed and EWMA/MAD mean F1
  were both `1.0`; the combined rule mean F1 was `0.136508`. All strategies had
  zero false alerts/hour in this small synthetic dataset, and completed-window
  P95 detection delay was 30 seconds where a fault was detected.
- Detector evaluator tests pass, including compound Go-duration parsing,
  manifest-ground-truth use, past-only EWMA/MAD history, raw evidence output,
  and retention of a weak rule result.
- Future-dated synthetic windows exposed a stale-data bypass boundary. The
  ClickHouse history query now excludes metrics/events over five seconds ahead
  of server time, and the scorer independently rejects an all-future set. A
  direct read showed unguarded latest `2026-09-01 00:13:00` versus guarded
  latest `2026-08-28 00:07:00` at server time `2026-08-29 11:16:19`; repository
  and scorer regression tests pass.
- Controlled ClickHouse pause/catch-up passed without truncating tables:
  ClickHouse consumer lag reached `6,134` while stopped and returned to zero
  `20.373` seconds after restart. The isolated range changed from zero rows to
  2,572 delivery, 2,582 routing, 645 player, 12 node-minute, and 4
  network-minute rows; ClickHouse returned healthy after the run.

## Not yet passed

- Checkpoint restore, duplicate behavior across restart, idle partition, and
  watermark/allowed-lateness integration tests.
- Injected-fault recommendation publication through Kafka, recommendation
  ack/outcome durability, and EdgeRoute shadow integration.

The upstream runtime pass does not substitute for the remaining StreamPulse
integration gates.
