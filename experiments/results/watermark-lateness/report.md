# Idle partition and lateness integration report

Measured on 2026-08-29 on the local Docker Desktop stack. This is a scoped
correctness experiment, not a throughput benchmark.

## Setup

- Isolated Flink job ID: `3aed3edbcf68d8428e210b19063ea218`.
- Flink parallelism: 2.
- Delivery topic: two partitions, keyed so partition 0 received 1,201 records
  and partition 1 remained empty.
- Bounded out-of-orderness: 10 seconds.
- Idle partition timeout: 30 seconds.
- Allowed lateness: 5 seconds.

## Result

| Check | Observed result |
|---|---|
| Idle partition progress | Target window became query-visible despite one empty partition |
| Initial aggregate | 600 requests, revision 0 |
| Allowed-late correction | 601 requests, revision 1 |
| Too-late behavior | Aggregate stayed at 601/revision 1 |
| Late audit | Exactly one redacted `TOO_LATE` DLQ record |
| Cleanup | Isolated job cancelled; original TaskManager count restored |

The first visible result arrived 37.712 seconds after the flush event. That
measurement includes the 30-second idle timeout and local polling; it is not
presented as general pipeline freshness.

The DLQ record retains the source topic, observation time, error code, error
message, payload SHA-256, and a redacted event ID preview. It does not retain the
raw event payload.

## Evidence

- `result.json`: assertions, offsets, revisions, job identity, and cleanup
  context.
- `too-late-dlq.json`: the persisted redacted late-event record.
- `watermark-*-manifest.json`: fixed scenario/configuration manifests for the
  base, flush, allowed-late, and too-late injections.

Re-run with `make watermark-lateness-test`. The script creates uniquely named
24-hour-retention test topics, uses an isolated consumer group, and restores the
original TaskManager count in `finally`.
