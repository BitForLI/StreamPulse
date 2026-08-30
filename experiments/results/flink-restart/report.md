# Flink TaskManager restart and exact replay

The only TaskManager was stopped after a completed checkpoint. The exact same generator run was replayed while it was down, then the TaskManager was restarted.

- Job ID remained: 344360c66b87dc6e5ceba0a5bdf84b71
- Vertices: 6 running -> 0 during outage -> 6 running
- Checkpoint: 15 -> 16; post-restore checkpoint state size 7999960 bytes
- Flink reported restored checkpoint: 15
- Peak observed delivery-source lag: 2626; final lag: 0
- TaskManager start to 6/6 vertices RUNNING: 12.779 seconds
- Exact replay manifests SHA-256 matched: True
- Completed minute unique delivery/node/network requests: 1198/1198/1198

The equality is scoped to a local synthetic minute and is not a production availability claim.

## Failed first attempt retained

The first attempt used `fixedDelayRestart(3, 5s)`. With the only TaskManager
offline for longer than the three retry intervals, Flink exhausted its restart
budget and marked the job `FAILED` with `NoResourceAvailableException`. Its
ordinary checkpoint was then cleaned up, so a manual restore was impossible.

The implementation was changed to a bounded failure-rate strategy (at most 10
failures in five minutes, five-second delay) and externalized checkpoints with
`RETAIN_ON_CANCELLATION`. The successful run above used the revised policy.
The original REST job and exception payloads are preserved as
`failed-attempt-job.json` and `failed-attempt-exceptions.json`; the failed run
is not counted as a successful recovery.
