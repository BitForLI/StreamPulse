# Kafka/Flink integration report

Date: 2026-08-29
Platform: Windows 11, Docker Desktop Linux engine 29.0.1
Scenario: `experiments/scenarios/integration.yaml`
Seed: `20260828`

## Input

The generator represented 6 minutes 20 seconds of event time and completed in
105.018 seconds of wall time on the local single-broker stack.

| Item | Count |
|---|---:|
| Base requests | 22,800 |
| Delivery records | 23,226 |
| Routing records | 22,800 |
| Player records | 5,700 |
| Deliberate duplicates | 426 |
| Deliberate schema errors | 219 |

The batch sink used bounded 500-record synchronous `RequireAll` writes. This
timing is an integration observation, not a throughput benchmark.

## Real broker distribution

Delivery-topic partition deltas for this run were:

| Partition | Records |
|---:|---:|
| 0 | 3,302 |
| 1 | 3,982 |
| 2 | 3,432 |
| 3 | 3,576 |
| 4 | 4,935 |
| 5 | 3,999 |

All six partitions received data and the deltas sum to the 23,226-record
manifest count. The maximum/median partition-load ratio was 1.306. This is a
measured consequence of hashing 64 locality-preserving stable keys to six local
partitions; it is retained rather than presented as perfectly uniform.

## Flink result

Job `a47f4af5802a649b73122931d5076771` remained `RUNNING` with all six vertices
`RUNNING`. The source reached zero lag on all six delivery partitions. At the
observation point, checkpoints were 19 completed, zero failed.

Kafka output counts were:

| Topic | Total records |
|---|---:|
| `cdn.metrics.node.1m.v1` | 92 |
| `cdn.metrics.network.1m.v1` | 32 |
| `cdn.metrics.content.5m.v1` | 324 |
| `cdn.dead-letter.v1` | 227 |

The DLQ contained 8 records from the preceding smoke replay. Its integration
delta was therefore exactly 219, matching the manifest's 219 invalid records.
Samples used `INVALID_CACHE_STATUS`, retained only a payload SHA-256 and
`redacted:length=...`, and did not copy the raw payload.

The aggregate topics contain node/location/network percentiles, 5xx rate,
cache-hit ratio, bytes and origin time, plus network-minute and content-five-
minute records. Counts include the earlier smoke window because the replacement
job intentionally started at the topic's earliest offsets. This report proves
the live data path and contract/DLQ count; hand-calculated metric correctness is
covered separately by the five Java tests.

## Not proved by this run

- production-scale throughput or latency;
- exactly-once end-to-end semantics;
- StreamPulse checkpoint restore or aggregate equality across a restart;
- idle-partition watermark behavior at runtime; or
- ClickHouse/Grafana ingestion.
