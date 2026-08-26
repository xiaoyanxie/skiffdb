# SkiffDB benchmark summary

Commit: `5c89f47b8f02250e100a604edefdfca2ebd35823` (dirty: `true`)  
Profile: `smoke`  
Completed: `true`

| Deployment | Workload | Value | Throughput ops/s | GET p99 µs | SET p99 µs | Errors |
|---|---|---:|---:|---:|---:|---:|
| in-memory-single | get95-set5 | 64 B | 150433.37 | 56.42 | 52.75 | 0 |
| durable-single-voter | get95-set5 | 64 B | 3033.30 | 15574.17 | 24316.42 | 0 |
| durable-three-voter | get95-set5 | 64 B | 1130.71 | 34620.04 | 45893.92 | 0 |
| durable-three-voter | snapshot-under-load-get95-set5 | 1024 B | 1371.27 | 33597.54 | 43295.33 | 0 |

## Recovery and snapshot scenarios

- Snapshot under load: 64.94 ms, 266385 bytes, throughput change -32.13%, max p99 38873.58 µs (success: true)
- Follower catch-up: 179.32 ms (success: true)
- Leader failover: 2579.25 ms (success: true)
- Snapshot + log replay: 1283.78 ms (success: true)
- Full-cluster restart: 1283.78 ms (success: true)

## Missing internal metrics

- Raft commit/apply latency: server instrumentation is not available
- Raft follower lag: server instrumentation is not available

Do not compare deployment modes as equivalent: in-memory acknowledgements and durable Raft acknowledgements provide different durability guarantees. Compare historical runs only when workload parameters and host metadata match.
