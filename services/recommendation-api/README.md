# Recommendation API

This service reads recent completed node windows from ClickHouse, combines an
explainable fixed rule with EWMA/rolling-median/MAD signals, scores eligible
nodes, applies safety guardrails, and publishes schema-v1 recommendations to
Kafka in shadow mode.

It deliberately reuses ClickHouse and `segmentio/kafka-go`; it does not
reimplement storage, message delivery, EWMA, median, MAD, or rendezvous routing.
The repository-specific code defines CDN features, detector composition,
guardrails, audit fields, error semantics, and API boundaries.

## Endpoints

- `GET /healthz` — process liveness; independent of ClickHouse.
- `GET /readyz` — dependency readiness; returns 503 when ClickHouse is unavailable.
- `GET /v1/scopes/{location}/{network}/metrics`
- `GET /v1/scopes/{location}/{network}/recommendations/latest`
- `POST /v1/recommendations/evaluate`
- `POST /v1/recommendations/{id}/ack`
- `POST /v1/recommendations/{id}/outcome`

Example evaluation body:

```json
{
  "scope": {
    "location": "au-sydney",
    "network_id": "as-synthetic-1221"
  },
  "current_weights": {
    "edge-syd-a": 0.5,
    "edge-syd-b": 0.5
  }
}
```

Normal scopes return `generated: false` with `NO_ANOMALY`. Dependency failures
return 503 and never publish. Generated events always use `mode=shadow`, a
two-minute TTL, a deterministic evidence-window ID, and a maximum absolute
per-node step of 0.20. At least two non-stale, healthy, unsaturated nodes must
remain, and recommendations observe a one-minute minimum dwell time.

Acknowledgements and outcomes are currently retained in the process-local
audit store; recommendation events themselves are durable through Kafka and
ClickHouse. Durable ack/outcome persistence is a documented later hardening
item.

## Test

```powershell
go test ./...
go vet ./...
```
