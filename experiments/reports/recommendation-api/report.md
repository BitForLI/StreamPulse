# Recommendation API verification report

## Passed locally

- Go module checksums verified; all packages pass `go test ./...` and `go vet ./...`.
- Rule baseline requires minimum samples plus both elevated error rate and P95
  above twice the peer median.
- EWMA/MAD uses only past windows, freezes large historical outliers, handles
  MAD=0, and explains latency/error/cache-miss signals separately.
- Normal histories do not emit signals or publish recommendations.
- Scoring filters stale, unhealthy, low-sample, and saturated nodes; at least
  two candidates must remain.
- Progressive interpolation preserves a weight sum of one while limiting every
  node to an absolute 0.20 step.
- Application tests verify two-minute TTL, one-minute dwell, deterministic ID,
  audit fields, publish-before-dwell ordering, and 503 dependency behavior.
- ClickHouse repository tests verify typed response parsing and parameter
  binding rather than string interpolation.
- Real local ClickHouse read smoke returned `health=ok`, `ready=ready`, three
  nodes for the tested scope, and `NO_ANOMALY` for the latest recovered normal
  window.

## Pending container gate

`scripts/recommendation-e2e.ps1` builds the container and Linux generator, runs
the final-window fault scenario with a manifest-recorded current start-time
override, evaluates the scope, verifies shadow mode/TTL/weight step, and waits
for the exact recommendation ID to reach ClickHouse. It has not run because the
Docker action approval was unavailable. No fault-detection precision or runtime
publication claim is made yet.

Acknowledgement and outcome endpoints currently use a process-local audit store;
durable ack/outcome tables remain a hardening item.
