# Architecture and ownership boundaries

## Data plane

Synthetic generators and, later, Vector-normalized edge logs publish versioned
events to Kafka. Flink owns parsing, event-time/watermark handling, deduplication,
late-event policy, keyed state, windows, and aggregate emission. ClickHouse owns
analytical storage and query acceleration; Grafana and the API only read the
resulting tables.

## Recommendation plane

Only completed aggregate windows become recommendation features. Statistical
or mature ML components may score anomalies, while repository-owned policy code
combines latency, error, cache, origin-cost, capacity, and freshness evidence.
Every output is shadow-only, time-bounded, explainable, and constrained before
it can be offered to EdgeRoute.

```text
HTTP handler
  -> evaluation use case
     -> parameterized ClickHouse repository
     -> fixed rule + past-window EWMA/median/MAD
     -> normalized multi-objective node scorer
     -> stale/future/capacity/candidate/step/TTL/dwell guardrails
     -> synchronous Kafka publisher
```

The liveness endpoint is process-only; readiness checks ClickHouse. A failed
query or Kafka publish returns 503 and does not start the minimum-dwell timer.
Normal windows return `NO_ANOMALY` without publishing. Expected deltas remain
model estimates; the separate outcome endpoint is the only place for observed
deltas.

## Reliability boundaries

- Kafka absorbs producer/consumer rate mismatch and short processing outages.
- Flink checkpoints state; replay and duplicate delivery are expected.
- A dead-letter topic isolates malformed records without leaking raw payloads.
- ClickHouse is not placed in the DNS request path.
- Missing, stale, or future-dated analytics data produces no unsafe routing mutation.

## Reused versus implemented

Apache Kafka, Flink, ClickHouse, Grafana, Vector, and mature statistical models
remain third-party components. StreamPulse implements the CDN contracts,
partitioning decisions, Flink business logic, storage model, recommendation
guardrails, adapters, tests, and reproducible experiments.

## Evidence stages

1. Contract evidence: schema, semantic, compatibility, and privacy tests.
2. Runtime evidence: pinned upstream job and failure recovery.
3. Processing evidence: hand-calculated event-time/window fixtures.
4. Integration evidence: generator-to-dashboard and DLQ behavior.
5. Experiment evidence: fixed-seed normal/fault scenarios and honest reports.

Passing an earlier stage never substitutes for a later one.
