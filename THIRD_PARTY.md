# Third-party software and provenance

## Apache Flink Playgrounds

- Upstream: <https://github.com/apache/flink-playgrounds>
- Baseline commit: `6115f8e6d083b1b69f7c82b19d5723a90aed95a1`
- Local baseline tag: `upstream-baseline-6115f8e`
- License: Apache License 2.0 (`LICENSE`)

The upstream repository supplies the Docker Compose playgrounds, the original
Kafka/Flink operations example, and the original example jobs. StreamPulse does
not claim those components as original work.

StreamPulse-specific work begins with the versioned CDN event contracts,
privacy rules, streaming analytics, ClickHouse model, recommendation service,
fault experiments, and project documentation added after the baseline tag.

Additional container and code dependencies will be recorded here when their
exact versions are introduced and verified.

The upstream Compose originally references `bitnami/kafka:3.9.0`; that tag was
unavailable during baseline replay. The documented local fallback uses Apache's
official `apache/kafka:3.9.0` image under Apache-2.0 and changes only container
configuration, not Kafka or Flink source code.

## Event generator dependencies

| Dependency | Pinned version | Use | Modified |
|---|---:|---|---:|
| `github.com/segmentio/kafka-go` | `v0.4.49` | Kafka producer client | No |
| `gopkg.in/yaml.v3` | `v3.0.1` | Scenario configuration parser | No |

The generator's CDN event model, scenario application, deterministic IDs,
partition keys, manifests, privacy validator, and tests are implemented here.

## Data application dependencies

| Dependency | Pinned version | Use | Modified |
|---|---:|---|---:|
| ClickHouse server | `26.7.3.19` | Kafka Engine ingestion and analytical storage | No |
| Grafana | `13.1.3` | Provisioned operational/business dashboard | No |
| Grafana ClickHouse datasource | `4.20.0` | Read-only dashboard queries | No |
| `github.com/segmentio/kafka-go` | `v0.4.49` | Recommendation Kafka publisher | No |

StreamPulse implements the DDL/materialized views, dashboard queries,
ClickHouse repository adapter, detector composition, scoring/guardrail policy,
API behavior, tests, and experiments. It does not claim the database,
visualization platform, Kafka client, or statistical formulae as original work.
