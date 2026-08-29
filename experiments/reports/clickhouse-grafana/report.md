# ClickHouse and Grafana integration report

Measured on 2026-08-29 on a local Windows 11 Docker Desktop host. The workload
is synthetic; this report is correctness and local-query evidence, not a
production capacity claim.

## Runtime and data path

- ClickHouse `26.7.3.19`, Grafana `13.1.3`, and Grafana ClickHouse datasource
  `4.20.0` were pinned in the repository.
- Eight ClickHouse Kafka Engine tables continuously consumed the three raw,
  three aggregate, recommendation, and dead-letter topics. Their materialized
  views performed JSON extraction and wrote TTL-governed MergeTree or
  ReplacingMergeTree targets.
- The fixed-seed run contained 22,800 base requests, 23,226 delivery records,
  22,800 routing records, 5,700 player records, 426 duplicate delivery records,
  and 219 injected contract errors.
- Flink consumed all six delivery partitions to zero lag. All six vertices
  remained `RUNNING`, and 21/21 checkpoints completed with zero failures during
  this run.
- Every ClickHouse Kafka consumer reported an empty exception list. The target
  row counts after catch-up were:

| Table | Query-visible rows |
|---|---:|
| `raw_delivery FINAL` | 22,584 |
| `raw_routing FINAL` | 22,800 |
| `raw_player FINAL` | 5,700 |
| `node_metrics_1m FINAL` | 84 |
| `network_metrics_1m FINAL` | 28 |
| `content_metrics_5m FINAL` | 250 |
| `dead_letters` | 219 |

The 23,226 physical delivery messages become 22,584 query-visible records after
application-level replacement of duplicate event IDs and rejection of invalid
events. Duplicate and schema-error annotations can overlap, so their manifest
totals must not be subtracted independently. The honest delivery guarantee is
at-least-once plus application-level event-ID replacement/revision, not
end-to-end exactly once.

## Grafana gate

- `/api/health` returned database `ok` for Grafana `13.1.3`.
- Provisioned datasource `streampulse-clickhouse` returned `OK` and
  `Data source is working`.
- Dashboard `streampulse-overview` loaded with 20 non-row panels across user
  impact, CDN efficiency, network/node quality, data-platform health, and
  shadow recommendations.
- All 20 panel SQL statements were executed directly against ClickHouse after
  expanding their time macros: 20 passed and 0 failed.
- The ClickHouse datasource uses the read-only `grafana_reader` account. Its
  grants are limited to `SELECT` on `streampulse.*` and
  `system.kafka_consumers`.

## Query baseline

The reproducible script resets the supported ClickHouse mark, uncompressed,
query-condition, and filesystem caches, executes one cold query, then runs 20
hot iterations. Nearest-rank percentiles are reported. The time includes local
HTTP client overhead, and the OS page cache is not forcibly cleared.

| Query | Cold | Hot P50 | Hot P95 | Read rows | Read bytes |
|---|---:|---:|---:|---:|---:|
| Node P95/error/hit ratio, latest 15 minutes | 12.746 ms | 9.830 ms | 27.857 ms | 168 | 5,202 |
| Location/network anomalous intervals, latest 24 hours | 13.032 ms | 8.881 ms | 12.170 ms | 56 | 1,832 |
| Most popular content across nodes | 21.256 ms | 16.967 ms | 23.337 ms | 45,168 | 3,501,400 |

Raw evidence is in `query-benchmark.json`; the executable benchmark is
`scripts/benchmark-clickhouse.ps1`.

## Remaining failure gate

The ClickHouse pause/materialized-view catch-up scenario is implemented as
`experiments/scenarios/clickhouse-catchup.yaml`, but it has not been executed.
The local Docker approval was rejected before `docker compose stop clickhouse`
ran, so no catch-up time is claimed.
