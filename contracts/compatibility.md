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

## Routing/recommendation boundary

The paired v1 fixtures additionally enforce the cross-project shadow contract:

- `current` and `proposed` contain the same node IDs;
- every recommended node is present in the routing candidate set;
- proposed weights sum to 1.0;
- StreamPulse only emits `mode=shadow` and `action=adjust_node_weights`.

This contract makes a recommendation auditable by EdgeRoute without granting
StreamPulse permission to mutate the online routing path. Contract validity is
necessary for a future adapter, but does not claim that adapter is implemented.
