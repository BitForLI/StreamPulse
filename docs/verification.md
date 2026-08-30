# Verification status

Last updated: 2026-08-30 on Windows 11.

## Passed

- JSON Schema Draft 2020-12 validation: 8/8 contract tests, including the paired
  RoutingEvent/RecommendationEvent shadow-boundary fixture.
- Privacy, semantic, bad-fixture, and optional-field compatibility checks.
- Root Compose overlay configuration parsing with Docker Compose
  `v2.40.3-desktop.1` (Docker config emitted a host permission warning).
- Generator compilation and Go tests, including deterministic sequence,
  controlled duplicates/schema errors, all five fault labels, and key spread.
- Full 3,000,000-request generator run repeated with identical manifest SHA-256
  `ff5b6b60361ea1885215ac838bcbeb6eb56d25ebcd7e6faf306e7983bc9b4792`.
- 684-record generator JSONL smoke checked against input schemas and privacy.
- Flink 1.16 job compilation and 8/8 Java tests, including hand-calculated
  aggregate fixtures and job-configuration checks.
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
- A `1800x1900` PNG was rendered from the live provisioned dashboard with the
  official Grafana Image Renderer service and visually inspected. The final
  render produced no Grafana query errors. Its freshness query ignores
  future-dated watermark-test windows, and recommendation table timestamps are
  rendered as UTC strings so descending audit results do not violate the
  ClickHouse datasource's time-series ordering requirement.
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
- Injected-fault recommendation E2E passed through generator -> Kafka -> isolated
  Flink consumer -> ClickHouse -> Go detector/scorer -> Kafka -> ClickHouse. The
  successful run advanced recommendation offsets `2 -> 3`, produced schema-v1
  shadow ID `rec-b4861107ffbf3c4cef004a0d`, retained three node evidence records
  plus input/query/config audit fields, enforced a 120-second TTL and 0.20
  maximum weight step, and persisted one acknowledgement and one non-causal
  synthetic outcome under the same ID.
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
- StreamPulse TaskManager checkpoint recovery passed with the same Job ID:
  vertices changed `6 -> 0 -> 6`, Flink reported checkpoint 15 as restored,
  checkpoint 16 completed afterward, observed source lag changed `2,626 -> 0`,
  and all vertices were RUNNING 12.779 seconds after TaskManager start.
- Exact duplicate replay used byte-identical manifests. For the checked
  completed minute, `FINAL` unique raw delivery, node request sum, and network
  request sum were all `1,198`; this establishes no missing/double-counted
  request for that scoped local window.
- The failed first restart attempt is retained: `fixedDelayRestart(3, 5s)`
  exhausted its budget while the only TaskManager was absent, and its ordinary
  checkpoint was cleaned up after terminal failure. The fix uses a bounded
  10-failures/5-minute strategy and externalized checkpoints. Raw REST
  exception/job payloads are saved with the successful evidence.
- Idle-partition/watermark integration passed with Flink parallelism 2 and an
  isolated two-partition delivery topic containing 1,201 records on partition
  0 and zero on partition 1. The target window first appeared with 600 requests
  at revision 0, then an event inside allowed lateness changed it to 601 at
  revision 1. A later event beyond the boundary produced exactly one redacted
  `TOO_LATE` DLQ record and left the aggregate at 601/revision 1.
- The isolated watermark job was cancelled in cleanup, TaskManager count was
  restored, and the main StreamPulse and upstream ClickCount jobs were both
  confirmed RUNNING afterward. Machine-readable evidence and all five generator
  manifests are retained under `experiments/results/watermark-lateness/`.
- `scripts/demo.ps1` passed a fresh full run after detecting and fixing the
  official Apache Kafka image's `/opt/kafka/bin` CLI path. It emitted one new
  shadow recommendation, advanced the recommendation topic by one record,
  persisted three-node evidence plus one acknowledgement/outcome, cancelled the
  isolated demo job, and restored the upstream ClickCount job. The main
  StreamPulse job remained RUNNING 6/6 and the API returned live/ready.
- GitHub-hosted CI run `33289296683` passed on commit `8ae9362`. It executed
  Python schema and detector tests, both Go service test suites, Go vet, the
  Maven/Flink test suite, and Docker Compose configuration validation. The two
  preceding failed runs are retained as evidence of fixes for the pip cache
  dependency path and the undeclared `PyYAML` test dependency.

## Not yet passed

- EdgeRoute shadow adapter consumption remains a separate cross-project gate.
- A narrated five-minute recording has not yet been produced; the static
  dashboard screenshot and executable demo path are verified locally.
- A `v0.1.0` release has not been created; release publication is intentionally
  separate from the completed fork, push, and CI verification.

The upstream runtime pass does not substitute for the remaining StreamPulse
integration gates.
