# Event contract compatibility

All StreamPulse event contracts carry an integer `schema_version`. Version 1
consumers follow these rules:

1. Producers may add optional fields without changing the major version.
2. Consumers must ignore unknown optional fields and preserve known semantics.
3. Required fields cannot be removed or renamed in the same major version.
4. A removed field name must never be reused with a different meaning.
5. Enum producers may emit only documented values. Consumers must implement an
   explicit `UNKNOWN` branch before an enum is extended.
6. Type, unit, timestamp, or identifier meaning changes require a new major
   schema version and normally a new Kafka topic.
7. Historical replay and Flink state/savepoint compatibility must be assessed
   before a breaking contract is deployed.

The JSON Schemas therefore allow additional optional properties, while privacy
tests independently reject prohibited field names and unsafe URL values.
