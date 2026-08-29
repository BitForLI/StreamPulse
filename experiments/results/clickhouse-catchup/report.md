# ClickHouse pause and catch-up

ClickHouse was stopped before the workload and restarted after Flink input lag reached zero.

- Observed ClickHouse consumer lag while paused: 6134
- Final ClickHouse consumer lag: 0
- Intentional pause before restart: 26.472 seconds
- Stop-to-healthy duration: 29.579 seconds
- Restart-to-zero-lag catch-up: 20.373 seconds
- Rows after catch-up: delivery=2572, routing=2582, player=645, node=12, network=4

This local synthetic result does not establish production recovery capacity.
