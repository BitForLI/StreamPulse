# Recommendation API end-to-end report

Measured at `2026-08-29T12:54:16Z` on the local Docker Compose stack.

## Result

- Isolated Flink job: `54e1e61d745eac06f76969ca9077df60`
- Consumer group: `streampulse-recommendation-e2e-v1`
- Starting offsets: `latest`
- Kafka recommendation total offset: `2 -> 3`
- Recommendation ID: `rec-b4861107ffbf3c4cef004a0d`
- Contract/mode: schema v1 / `shadow`
- TTL: 120 seconds
- Maximum absolute node weight step: 0.20
- ClickHouse audit evidence: 3 node records, input window, query version, config hash
- Durable acknowledgement rows for this ID: 1
- Durable outcome rows for this ID: 1

The fault combined elevated latency and 5xx rate on `edge-syd-a`. The rule,
EWMA error-rate, and EWMA/MAD latency detectors all fired. The proposed weights
reduced `edge-syd-a` from 0.3333 to 0.1988 and raised `edge-syd-b` from 0.3333 to
0.5333 while preserving three eligible nodes.

## Boundaries

The generator uses synthetic locations, networks, nodes, and traffic. Expected
QoE/cost deltas are model estimates, not observed production savings. The saved
outcome contains zero deltas and explicitly states that it is only a persistence
check; it is not causal evidence. The test never applies weights to EdgeRoute.

Machine-readable evidence and the exact generator manifest are stored beside
this report in `e2e-result.json` and `fault-manifest.json`.
