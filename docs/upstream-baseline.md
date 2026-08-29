# Apache Flink Playgrounds baseline

- Repository: <https://github.com/apache/flink-playgrounds>
- Commit: `6115f8e6d083b1b69f7c82b19d5723a90aed95a1`
- Commit date: `2025-02-14T00:08:41+08:00`
- Commit subject: `update docker setup`
- Local tag: `upstream-baseline-6115f8e`
- License: Apache License 2.0

## Source inspection

The pinned operations playground declares:

- Apache Flink `1.16.0`, Scala `2.12`, Java runtime `11`;
- Bitnami Kafka `3.9.0` in KRaft mode;
- Maven builder image `3.8-eclipse-temurin-17`;
- one JobManager, one TaskManager, the ClickCount example producer, and the
  checkpointed event-time ClickCount job.

## Runtime verification

The first unmodified startup attempt on 2026-08-29 failed before containers
started because Docker Hub no longer resolved `bitnami/kafka:3.9.0`. Docker
Desktop itself was healthy (`29.0.1`, Linux engine). The failure was retained as
an upstream reproducibility result rather than silently changing the original
file.

`experiments/reports/upstream-baseline/kafka-image-override.yaml` replaces only
that unavailable image with the Apache official `apache/kafka:3.9.0` image and
its documented single-node KRaft variables. The upstream Compose and example
job remain unchanged. Any successful run through this overlay is therefore a
declared source-image substitution, not a byte-for-byte execution of the
original Compose dependency.

The declared-substitution run then passed the baseline gate on 2026-08-29:

- Kafka was healthy and exposed the `input` and `output` topics;
- one TaskManager registered with two slots;
- the producer advanced both `input` partitions;
- Flink job `7e10cffb737c4c6dda344a33dad34f8f` ran all four vertices;
- the output topic contained 15-second page-count windows;
- stopping the TaskManager moved all four vertices to `CREATED`;
- after restart, all four vertices returned to `RUNNING` in 6 seconds without a
  new Job ID; and
- offsets continued from input `23605/23596` to `28135/28124` and output
  `18/24` to `26/34`.

The exact concise command evidence is retained in
`experiments/reports/upstream-baseline/runtime-report.md`. This proves recovery
for the upstream ClickCount example under this local single-TaskManager setup;
it does not yet prove StreamPulse aggregate correctness across a restart.
