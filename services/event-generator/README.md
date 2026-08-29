# Deterministic synthetic event generator

This service emits v1 DeliveryEvent, RoutingEvent, and PlayerEvent records to
Kafka or a local JSONL envelope. It uses an explicit seed and simulation start
time, injects bounded event-time disorder, duplicates, schema errors, and the
five required CDN fault scenarios, then writes a ground-truth manifest.

```powershell
go run ./cmd/generator `
  -config ../../experiments/scenarios/baseline.yaml `
  -output jsonl `
  -output-file ../../experiments/raw/baseline.jsonl `
  -manifest ../../experiments/reports/generator-baseline/run-manifest.json
```

Use `-output kafka -brokers localhost:9092` only after the topics exist. JSONL
is the deterministic offline validation path; each line contains topic, key,
value, and explicit duplicate/schema-error annotations. All values are
synthetic.

For stale-data-sensitive recommendation experiments, pass an explicit RFC3339
`-start-time`. Repeated experiments may also pass an integer `-seed`. Both
overrides are validated and recorded in the manifest; all relative fault
windows remain reproducible. The API clock is never altered to make stale data
appear fresh.

The Kafka sink keeps a bounded 500-record buffer, writes each batch
synchronously with `RequireAll`, and flushes the final partial batch on close.
This avoids one broker round trip per event while still returning broker and
final-flush errors to the run.
