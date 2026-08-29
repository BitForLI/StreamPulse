# StreamPulse

StreamPulse is a CDN data application that turns synthetic delivery, routing,
and player events into event-time metrics and auditable **shadow** routing
recommendations. It is built to demonstrate correctness under out-of-order
events, replay, component failure, and stale data—not merely to launch a
dashboard.

> Runtime scaffold derived from Apache Flink Playgrounds; CDN schemas,
> analytics jobs, ClickHouse pipeline, recommendation service, experiments and
> documentation are implemented in this repository.

## Current status

Day 1 contracts, the Day 2 deterministic generator, the Day 3 core Flink
analytics job, and the Day 4 ClickHouse/Grafana path are implemented and
broker-integrated. Day 5 recommendation API source, explainable detectors,
scorer, and guardrails are implemented and locally tested; their fault-to-Kafka
runtime gate remains pending. The
repository contains four v1 event schemas, compatibility/privacy enforcement,
topic design, fixed-seed CDN fault scenarios, Kafka and JSONL sinks, manifest
generation, event-time/watermark handling, deduplication, DLQ/late outputs, and
node/network/content window aggregation. Kafka Engine tables and materialized
views persist raw and aggregate data with explicit TTLs; a provisioned Grafana
dashboard exposes 20 verified queries. The upstream Apache Kafka/Flink operations
example is pinned at commit
`6115f8e6d083b1b69f7c82b19d5723a90aed95a1`.

The upstream job is runtime-verified on Docker Desktop through a documented
official Kafka-image substitution: the original unavailable
`bitnami/kafka:3.9.0` reference is overlaid with `apache/kafka:3.9.0`. Kafka was
healthy, the job produced output, and all four vertices recovered in 6 seconds
after the only TaskManager was restarted. See
[`docs/upstream-baseline.md`](docs/upstream-baseline.md).

The StreamPulse integration run sent 22,800 fixed-seed requests through real
Kafka topics. Flink reached zero source lag, emitted all three aggregate types,
completed 21/21 observed checkpoints in the ClickHouse-integrated run, and produced a 219-record DLQ for
exactly 219 injected contract errors. See
[`experiments/reports/flink-integration/report.md`](experiments/reports/flink-integration/report.md)
and
[`experiments/reports/clickhouse-grafana/report.md`](experiments/reports/clickhouse-grafana/report.md).

## Architecture

```text
synthetic CDN/player events
          |
          v
       Kafka raw topics ---> dead-letter topic
          |
          v
  Flink event-time jobs
          |
          +----> Kafka aggregate topics ----> ClickHouse ----> Grafana/API
          |
          +----> completed-window features ----> shadow recommendations
```

The DNS request path never waits for StreamPulse. Recommendations carry an
expiry, evidence window, confidence, reason codes, and bounded proposed weights;
EdgeRoute integration remains an explicit later-stage contract.

The recommendation API follows `HTTP handler -> use case -> query repository ->
detector/scorer -> guardrail -> Kafka publisher`. It combines a fixed rule with
past-window EWMA and rolling median/MAD evidence, filters stale/unhealthy/
capacity-constrained nodes, preserves at least two candidates, limits each
absolute weight step to 0.20, enforces a one-minute dwell, and emits only
two-minute `mode=shadow` events. Current code and endpoint semantics are in
[`services/recommendation-api/README.md`](services/recommendation-api/README.md).

## Contract quick start

Requires Python 3 and the pinned test dependency.

```powershell
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests/schema -v
```

Expected result: seven contract tests pass. They cover minimal/full valid
events, a forward-compatible optional field, missing required fields, unknown
enums, invalid numeric bounds, expired recommendations, and forbidden privacy
data.

## Generator quick start

The public workload is explicitly synthetic. A small offline smoke run requires
Go and Python but no Kafka:

```powershell
go -C services/event-generator test ./...
go -C services/event-generator run ./cmd/generator `
  -config ../../experiments/scenarios/smoke.yaml `
  -output jsonl `
  -output-file ../../.tmp/generator-smoke.jsonl `
  -manifest ../../.tmp/generator-smoke-manifest.json
python scripts/validate-generated-events.py .tmp/generator-smoke.jsonl
```

The full fixed-seed baseline generated 3,000,000 base requests twice. Both run
manifests had SHA-256
`ff5b6b60361ea1885215ac838bcbeb6eb56d25ebcd7e6faf306e7983bc9b4792`;
the maximum/median delivery-key load ratio was `1.0081`. This validates
determinism and key distribution only, not Kafka/Flink throughput. See
[`experiments/reports/generator-baseline/report.md`](experiments/reports/generator-baseline/report.md).

## Upstream playground gate

The root Compose file includes the original operations playground without
editing it:

```powershell
docker compose -f compose.yaml config
docker compose -f compose.yaml up --build -d
```

The original dependency no longer resolves; use the declared replacement layer
for the reproducible baseline command:

```powershell
docker compose -f operations-playground/docker-compose.yaml `
  -f experiments/reports/upstream-baseline/kafka-image-override.yaml `
  up --build -d
```

The upstream acceptance gate passed on 2026-08-29. This is separate from the
StreamPulse generator-to-aggregate integration gate, which also passed using
the fixed-seed integration scenario.

## Flink analytics test

```powershell
mvn -f jobs/cdn-analytics/pom.xml test
```

The Java tests use hand-calculated fixtures for exact MVP percentiles, 5xx
count, cache hits, bytes, and origin time. Compilation also verifies the Kafka
source/sinks, 10-second bounded-out-of-orderness watermark, idle-partition
handling, 5-second allowed lateness, per-window revision, and 25-hour event-ID
deduplication APIs against Flink `1.16.0`.

## ClickHouse and Grafana gate

Start the root Compose stack, create the topics, submit the analytics JAR, and
run the fixed-seed integration generator. The provisioned services are exposed
locally at ClickHouse `http://localhost:8123`, Grafana
`http://localhost:3000`, and Flink `http://localhost:8081`.

```powershell
docker compose -f compose.yaml up -d
make clickhouse-benchmark
```

The verified run populated raw delivery/routing/player tables, all three
aggregate tables, and 219 dead letters. Grafana datasource health returned
`OK`; all 20 dashboard SQL statements passed direct execution. The three fixed
query baselines record cold latency, 20-iteration hot P50/P95, rows, and bytes
in the Day 4 report. These are local synthetic measurements, not production
throughput claims.

## Repository map

- `schemas/` — versioned Delivery, Routing, Player, and Recommendation schemas.
- `contracts/` — topics, field semantics, compatibility, and privacy rules.
- `tests/schema/` — schema, semantic, compatibility, and privacy tests.
- `operations-playground/` and `docker/` — preserved Apache upstream runtime.
- `infra/clickhouse/` and `infra/grafana/` — version-pinned storage and dashboard provisioning.
- `services/recommendation-api/` — Go query, detection, scoring, guardrail, and shadow-publish service.
- `docs/` — architecture and exact upstream provenance/verification status.
- `experiments/` — reproducible evidence; raw large outputs are not committed.

## Scope and limitations

- Public fixtures are synthetic and contain no complete IPs, credentials,
  signed URLs, raw user agents, accounts, device IDs, email, or phone data.
- V1 normal synthetic traffic is unsampled to keep count checks exact.
- Recommendation output is shadow-only and cannot mutate production routing.
- ClickHouse storage and Grafana dashboard queries are locally integrated.
  Recommendation API code is locally tested, but its injected-fault-to-Kafka
  container integration is not yet claimed complete. Model comparison,
  EdgeRoute shadow integration, and StreamPulse-specific runtime failure
  experiments are subsequent stages. Checkpoint restore, ClickHouse
  pause/catch-up, and restart aggregate comparison remain unverified.

See [`THIRD_PARTY.md`](THIRD_PARTY.md) for provenance and license boundaries.
