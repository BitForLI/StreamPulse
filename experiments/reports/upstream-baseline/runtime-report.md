# Upstream runtime report

Date: 2026-08-29
Platform: Windows 11, Docker Desktop 4.53.0, Linux engine 29.0.1
Baseline: Apache Flink Playgrounds commit
`6115f8e6d083b1b69f7c82b19d5723a90aed95a1`

## Dependency result

The unmodified Compose startup failed before container creation because Docker
Hub no longer resolved `bitnami/kafka:3.9.0`. The successful run used the
declared overlay `kafka-image-override.yaml`, which substitutes the official
`apache/kafka:3.9.0` image and single-node KRaft environment only. The upstream
Compose and ClickCount source were not edited.

## Healthy baseline

```text
Kafka:       Up (healthy), apache/kafka:3.9.0
JobManager:  Up, REST http://localhost:8081
TaskManager: 1 registered, 2 total slots
Topics:      input, output
Job ID:      7e10cffb737c4c6dda344a33dad34f8f
Job state:   RUNNING
Vertices:    4/4 RUNNING
```

The output topic returned real 15-second ClickCount windows, including:

```json
{"windowStart":"01-01-1970 12:00:15:000","windowEnd":"01-01-1970 12:00:30:000","page":"/jobs","count":1000}
```

## TaskManager recovery

Under steady producer load, the only TaskManager was stopped for three seconds
and restarted.

```text
Before:  job RUNNING, 4/4 vertices RUNNING
During:  same Job ID, job RUNNING, 4/4 vertices CREATED
Poll 1:  0/4 vertices RUNNING
Poll 6:  4/4 vertices RUNNING
After:   same Job ID, job RUNNING, 4/4 vertices RUNNING
```

Kafka offsets observed before and five seconds after recovery:

| Topic | Before (p0/p1) | After (p0/p1) |
|---|---:|---:|
| `input` | 23605 / 23596 | 28135 / 28124 |
| `output` | 18 / 24 | 26 / 34 |

Recovery observation: 6 seconds from TaskManager restart to all vertices
`RUNNING`. Offset growth shows that production and aggregation resumed. This is
an upstream-scaffold recovery gate, not a throughput benchmark or an
exactly-once claim.
