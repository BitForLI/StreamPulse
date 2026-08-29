# CDN analytics Flink job

The single MVP job reads `cdn.delivery.v1`, validates and deduplicates events,
assigns event-time watermarks, and branches into:

- node 1-minute metrics keyed by location/network/node;
- network 1-minute metrics keyed by location/network;
- content 5-minute metrics keyed by content;
- redacted parse and too-late records in `cdn.dead-letter.v1`.

The watermark starts at 10 seconds bounded out-of-orderness with 30-second idle
partition detection. Windows allow five more seconds for revisions; records
beyond that boundary are audited once through the node branch's late side
output. Event IDs are deduplicated with 25-hour state TTL.

MVP percentiles keep exact window samples so correctness can be compared with
hand calculations. This is deliberately bounded to local scale; a mergeable
t-digest/HDR implementation and measured approximation error are required
before claiming high-scale percentile support.

```powershell
mvn test
mvn package
```

The Kafka sinks are at-least-once. Aggregate keys include deterministic window
identity and `revision`; downstream storage must select the latest revision.
