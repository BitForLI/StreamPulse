# Detector comparison

Synthetic, fixed-seed local evidence. Missing or weak results are retained.

Runs: **3**; three-run gate: **True**; manifest evidence complete: **True**.

| Strategy | Precision mean/median/range | Recall mean/median/range | F1 mean/median/range | False alerts/hour mean | Detection P95 mean (s) |
|---|---:|---:|---:|---:|---:|
| `fixed` | 1.000000/1.000000/[1.000000, 1.000000] | 1.000000/1.000000/[1.000000, 1.000000] | 1.000000/1.000000/[1.000000, 1.000000] | 0.000000 | 30.0 |
| `rule` | 0.666667/1.000000/[0.000000, 1.000000] | 0.076923/0.076923/[0.000000, 0.153846] | 0.136508/0.142857/[0.000000, 0.266667] | 0.000000 | 30.0 |
| `ewma_mad` | 1.000000/1.000000/[1.000000, 1.000000] | 1.000000/1.000000/[1.000000, 1.000000] | 1.000000/1.000000/[1.000000, 1.000000] | 0.000000 | 30.0 |

## Per-run results

| Run | Strategy | Precision | Recall | F1 | Missed faults |
|---|---|---:|---:|---:|---:|
| `run-01-seed-20260831` | `fixed` | 1.000000 | 1.000000 | 1.000000 | 0 |
| `run-01-seed-20260831` | `rule` | 1.000000 | 0.153846 | 0.266667 | 2 |
| `run-01-seed-20260831` | `ewma_mad` | 1.000000 | 1.000000 | 1.000000 | 0 |
| `run-02-seed-20260901` | `fixed` | 1.000000 | 1.000000 | 1.000000 | 0 |
| `run-02-seed-20260901` | `rule` | 1.000000 | 0.076923 | 0.142857 | 3 |
| `run-02-seed-20260901` | `ewma_mad` | 1.000000 | 1.000000 | 1.000000 | 0 |
| `run-03-seed-20260902` | `fixed` | 1.000000 | 1.000000 | 1.000000 | 0 |
| `run-03-seed-20260902` | `rule` | 0.000000 | 0.000000 | 0.000000 | 4 |
| `run-03-seed-20260902` | `ewma_mad` | 1.000000 | 1.000000 | 1.000000 | 0 |

## Interpretation

The fixed threshold and EWMA/MAD baseline tied on these deliberately strong synthetic faults; this does not show that either generalizes to changing production traffic.
The combined rule was retained despite weak recall: requiring latency and error evidence together missed latency-only and error-only windows.
Each raw node-metric file and manifest is SHA-256 identified in `summary.json`; per-window labels, predictions, metrics, and reason codes are stored in the three `*-predictions.jsonl` files.

Detection uses completed-window timestamps. EWMA/MAD sees only past windows; no future rows are included.
This report does not establish causal QoE improvement or production-scale performance.
