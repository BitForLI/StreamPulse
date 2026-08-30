# Five-minute StreamPulse demo

Run `scripts/demo.ps1` before presenting. It generates fresh synthetic data and
stores the machine-readable result under `.tmp/demo/`; formal experiment
evidence under `experiments/` is not overwritten.

## 0:00–0:40 — architecture and safety boundary

Show the README architecture. EdgeRoute is the online routing system;
StreamPulse is an asynchronous data application. Point out that events are
synthetic/anonymous and recommendations are shadow-only, expiring, and bounded.

## 0:40–1:20 — normal pipeline

Open Flink at `http://localhost:8081` and Grafana at
`http://localhost:3000/d/streampulse-overview`. Explain the path from Kafka raw
events through event-time aggregation to ClickHouse and the dashboard.

## 1:20–2:10 — injected degradation

Use the demo command output to identify the fresh scenario. Show the affected
Sydney network/node window in Grafana and explain that service-side delivery
latency/error metrics are QoE proxies, not direct rebuffer measurements.

## 2:10–3:00 — detection and recommendation

Show the recommendation ID, reason codes, node evidence, 120-second TTL, and
maximum 0.20 absolute weight step in `.tmp/demo/e2e-result-*.json`. Emphasize
that publication to Kafka and persistence in ClickHouse are verified, but no
online route is mutated.

## 3:00–3:50 — correctness under event time

Open `experiments/results/watermark-lateness/result.json`: one input partition
is empty, the window moves from 600/revision 0 to 601/revision 1 for an
allowed-late event, and a too-late event goes to the redacted DLQ without
changing the aggregate.

## 3:50–4:30 — failure recovery

Open `experiments/results/flink-restart/report.md`: the same job restores
checkpoint 15, returns from 0 to 6 running vertices in 12.779 seconds, drains
lag to zero, and preserves an exact 1,198-request scoped replay result. Also
show the retained failed first attempt.

## 4:30–5:00 — honest experiment conclusion

Open `experiments/results/detector-comparison/report.md`. Fixed threshold and
EWMA/MAD tie at mean F1 1.0 on this small synthetic dataset; the combined rule
is worse at 0.136508. Close with the main limitation: the EdgeRoute shadow
adapter and real player-session QoE remain future cross-project work.
