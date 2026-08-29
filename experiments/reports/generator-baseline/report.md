# Deterministic generator baseline

Date: 2026-08-29
Configuration: `experiments/scenarios/baseline.yaml`
Output sink: Windows `NUL` device (records were generated and encoded but not
published to Kafka)
Source state: uncommitted StreamPulse working tree on upstream baseline
`6115f8e6d083b1b69f7c82b19d5723a90aed95a1`

## Reproducibility result

The 10-minute logical workload at 5,000 base requests/s was executed twice with
seed `20260827`. Both generated manifests had the same SHA-256:

```text
ff5b6b60361ea1885215ac838bcbeb6eb56d25ebcd7e6faf306e7983bc9b4792
```

| Measure | Result |
|---|---:|
| Base requests | 3,000,000 |
| Routing records | 3,000,000 |
| Delivery records | 3,003,054 |
| Player records | 750,000 |
| Exact duplicate records | 3,054 |
| Controlled schema-error records | 1,533 |
| Delivery partition keys | 64 |
| Maximum/median delivery-key count | 1.0081 |
| Maximum/mean delivery-key count | 1.0084 |

Expected label windows for node latency, node 5xx, ISP-to-node degradation,
content popularity shift, and capacity pressure are stored in
`run-manifest.json`.

## Contract smoke validation

A separate 300-request smoke workload wrote JSONL and was checked against all
three input schemas plus privacy, selected-node, and exact-duplicate rules. It
validated 684 records: 300 routing, 309 delivery, and 75 player. The run
included nine exact duplicates and eight deliberately invalid schema records;
every unmarked record was accepted and every marked invalid record was rejected.

## Interpretation and limitations

This is evidence of deterministic generation, controlled corruption, contract
coverage, and balanced keys. It is not a Kafka throughput benchmark, does not
prove partition placement inside a broker, and does not validate Flink recovery.
Those claims remain behind separate Docker/Kafka/Flink gates.
