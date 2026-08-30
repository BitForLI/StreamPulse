# Evidence-backed resume bullets

**StreamPulse — Real-Time CDN Telemetry Analytics and QoE/Cost Optimisation**

*Java, Apache Flink, Kafka, ClickHouse, Go, Grafana, Docker Compose*

- Built an event-time CDN telemetry pipeline over six Kafka delivery
  partitions, processing a fixed-seed 22,800-request run into revisioned
  node/network/content aggregates in ClickHouse while matching all 219 injected
  contract errors to a redacted DLQ.
- Implemented watermark/idleness, allowed-late correction, event-ID
  deduplication and checkpoint recovery; restored the same Flink job from
  checkpoint 15 in 12.779 seconds, drained 2,626 records of observed lag to
  zero, and preserved an exact 1,198-request scoped replay result.
- Developed fixed-rule and past-only EWMA/MAD detection with TTL/staleness,
  capacity, two-candidate and 0.20 weight-step guardrails, publishing an
  auditable two-minute shadow recommendation through Kafka and ClickHouse; both
  detectors achieved mean F1 1.0 across three small synthetic runs, reported as
  a tie rather than a production claim.

Every number above is traceable to the reports linked from `README.md`. Do not
claim production scale, causal QoE improvement, exactly-once delivery, or a live
EdgeRoute adapter from this evidence.
