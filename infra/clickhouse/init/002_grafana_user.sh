#!/usr/bin/env bash
set -eu

grafana_password="${GRAFANA_CH_PASSWORD:-grafana-local}"

clickhouse-client --multiquery <<SQL
CREATE USER IF NOT EXISTS grafana_reader IDENTIFIED WITH plaintext_password BY '${grafana_password}';
GRANT SELECT ON streampulse.* TO grafana_reader;
GRANT SELECT ON system.kafka_consumers TO grafana_reader;
SQL
