<p align="center">
  <img width="661" height="279" alt="image"
       src="https://github.com/user-attachments/assets/27133b61-56af-452a-86ec-f6100157d11d" />
</p>


------
Skiffdb is a lightweight, Redis-protocol (RESP) key-value store designed as a **secondary-tier cache** with **optional persistence** and **high availability (HA)**. Point existing Redis clients at it for fast reads, consistent writes, and warm restarts when you want durability.
> Status: Experimental / alpha. Core cache commands work; HA scaffolding exists; persistence is WIP.  

# Intro
## Why Skiffdb?
- Cache-first: predictable latency and memory controls (TTL & eviction) for hot paths.
- Redis-compatible subset: speak RESP so your existing clients/tools “just work.”
- Optional persistence: warm-start via snapshots and (optionally) AOF.
- HA mode: leader-based replication so writes are consistent; stale follower reads are possible for cache workloads.

## Current highlights
- RESP server with common commands (`GET`/`SET`/`DEL`/`EXPIRE`/`TTL`/`EXISTS`/`INCR`/`DECR`, simple Lists/Hashes, `PING`).
- TTLs with lazy expiration (active sweeps on the roadmap).
- Basic HA scaffolding (Raft-based) with in-memory snapshotting.
- Simple in-memory engine (map + locks) — easy to hack, easy to profile.

## Getting Started
> You can run a single node (fastest way to try it) or a small HA cluster. Flags may evolve while in alpha.
### 1) Single node
```bash
# Download a release binary for your platform (example path shown)
# chmod +x ./skiffdb-server

./skiffdb-server \
  --addr=:6380 \
  --data-dir=./data \
  --maxmemory=1GB
```

Smoke test with redis-cli:
```bash
redis-cli -p 6380 PING
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
redis-cli -p 6380 EXPIRE hello 5
redis-cli -p 6380 TTL hello
```

Optional health/metrics (if enabled in your build):
curl -s localhost:9090/healthz
curl -s localhost:9090/metrics | head

### 2) 3-node HA cluster (alpha)
> Basic leader/follower replication; single Raft group. Start three processes with unique IDs and addresses. Bootstrap once.
Terminal A:
```bash
./skiffdb-server \
  --addr=:6381 \
  --raft-id=node-a \
  --raft-addr=:7001 \
  --data-dir=./data-a \
  --bootstrap
```

Terminal B:
```bash
./skiffdb-server \
  --addr=:6382 \
  --raft-id=node-b \
  --raft-addr=:7002 \
  --data-dir=./data-b \
  --join=127.0.0.1:7001
```

Terminal C:
```bash
./skiffdb-server \
  --addr=:6383 \
  --raft-id=node-c \
  --raft-addr=:7003 \
  --data-dir=./data-c \
  --join=127.0.0.1:7001
```

Point clients at the leader (Skiffdb will log which node is leader) for linearizable writes. Stale reads from followers may be allowed for cache workloads (depending on config).

### 3) Run With Config file (TOML)
You can also run with a config file:
```toml
# skiffdb.toml
[server]
bind = "0.0.0.0"
port = 6380
maxmemory = "1GB"                # enforce memory cap
eviction_policy = "allkeys-lru"  # planned: lru|lfu|ttl

[persistence]
snapshot = true                   # enable periodic RDB-like snapshot
aof = false                       # append-only file (everysec) — optional

[cluster]
enabled = false
node_id = "node-a"
raft_addr = ":7001"
bootstrap = true
join = ""                         # e.g. "127.0.0.1:7001" on non-bootstrap nodes
data_dir = "./data"
```

Run:
```bash
./skiffdb-server --config=./skiffdb.toml
```

## Build from source
### Prereqs
- Go 1.22+
- (Optional) make, redis-cli, Prometheus/Grafana for metrics

### Build
```
git clone https://github.com/fanyi-zhao/skiffdb.git
cd skiffdb

# Build server
go build -o skiffdb-server

# Run tests
go test ./...
```

## Roadmap
A pragmatic path to a cache-first KV with opt-in persistence and trustworthy HA.
### P0 — Make it dependable (MVP you can deploy)
#### Correctness & Ops
- [ ] TTL/Expiry engine: lazy + active sweeps; min-heap or timing-wheel.
- [ ] Memory limits & eviction: maxmemory + policies (allkeys-lru, allkeys-lfu w/ sampling).
- [ ] Command set for cache: MGET/MSET, SETEX/PEXPIRE/EXPIREAT/PTTL, variadic DEL, SCAN (cursor), INFO, ECHO.
- [ ] Networking hygiene: pipelining, backpressure, slow client handling, sane conn limits.
- [ ] Observability: Prometheus metrics (ops/latency by command, mem by type, expirations/evictions, replication lag), SLOWLOG, LATENCY LATEST, INFO.
- [ ] Security: AUTH and TLS listener.

#### Persistence (cache-friendly)
- [ ] RDB-style snapshots for fast warm start (non-blocking).
- [ ] Optional AOF (always|everysec|no) + lightweight rewrite/compaction.

#### Performance hygiene
- [ ] Sharded hashmap (e.g., 256 shards) to reduce RWMutex contention.
- [ ] Buffer reuse; avoid hot-path allocations.
- [ ] Batch log/AOF application.

### P1 — HA that operators trust
#### Replication model
- [ ] Single Raft group with durable log + stable store on disk (e.g., Pebble/Bolt).
- [ ] Leader reads (simple) + option for linearizable reads via barrier.
- [ ] Bootstrap/recovery docs; snapshot restore; node replacement.

#### Persistence experience
- [ ] Sub-minute warm restart for ~GB snapshots; bounded AOF replay (rewrite on ratio/size).
- [ ] Write-through/-behind hooks (interface only at first) for “secondary-tier” patterns.

#### Operational surface
- [ ] Config file (TOML/YAML), Docker image, Helm chart.
- [ ] Health endpoints (/healthz, /readyz), /metrics.
- [ ] Admin: CONFIG GET/SET (safe subset), SAVE, BGSAVE, guarded FLUSHDB.

#### Target SLOs (guidance)
- Single node: >200–300k GET/s and >100–150k SET/s @ 1 KiB values; p99 < 5–10 ms @ ~70% CPU on a modern box.

### P2 — Scale-out
#### A) Client-side sharding (faster to ship)
- [ ] Consistent hashing (ketama) across many 3-node shards.
- [ ] Lightweight membership/gossip and client library/sidecar.

#### B) Server-managed sharding (Redis-Cluster-like)
- [ ] 16384 slots + CLUSTER SLOTS, MOVED/ASK redirects.
- [ ] Metadata Raft group for slot→replica mapping.
- [ ] Resharding & rebalancing with online key migration.

### P3 — Differentiators
- [ ] Better hit-rates: TinyLFU/ARC; pluggable policies per namespace.
- [ ] Multi-tenant namespaces: per-tenant quotas, metrics, ACL/RBAC.
- [ ] S3/R2 snapshot sink for rapid fleet warm-ups.
- [ ] Probabilistic DS ops: Bloom/Count-Min; HLL; built-in rate limiters.
- [ ] RESP3, streams, pub/sub — only if demanded.
