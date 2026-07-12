# pg-ha

PostgreSQL High Availability with built-in Raft consensus and load balancing.

## Overview

pg-ha is a Rust-based PostgreSQL HA solution providing complete feature parity with [Patroni](https://github.com/patroni/patroni) while eliminating external dependencies:

- **No etcd/ZooKeeper/Consul** — embedded Raft consensus via [openraft](https://github.com/databendlabs/openraft)
- **No HAProxy** — built-in TCP proxy with active health checking
- **Single binary deployment** — one process per node, zero external services required
- **TLS encrypted** — Raft RPC communication secured with TLS/mTLS
- **Graceful shutdown** — CHECKPOINT + leader lock release before exit for fast failover

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/pg-ha-architecture.md) | System design, HA decision loop, Raft consensus, TLS, auth, graceful shutdown |
| [Deployment Guide](docs/deployment-guide.md) | Quick start, production setup, config reference, TLS certificates, Docker/systemd |
| [Operations Guide](docs/operations-guide.md) | Cluster management, switchover/failover, disaster recovery, debugging, monitoring |

## Architecture

Each node runs a single `pg-ha` agent process containing:

1. **HA Engine** — periodic decision loop managing failover and promotion
2. **Raft DCS** — embedded consensus cluster for leader election and state storage
3. **REST API** — health checks (compatible with HAProxy) and management endpoints
4. **TCP Proxy** — read/write splitting across PostgreSQL backends

## Quick Start

```bash
# Clone and start a 3-node cluster
git clone https://github.com/seraphico/pg-ha.git && cd pg-ha
make up

# Verify cluster
curl -s http://localhost:8008/cluster | jq .

# Connect via proxy (RW port)
psql -h localhost -p 6432 -U postgres

# Stop cluster
make down
```

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `pg-ha` | Main binary (composes all subsystems) |
| `pg-ha-core` | HA logic, PG lifecycle, cluster types, configuration |
| `pg-ha-dcs` | Raft state machine, network transport, DCS trait impl |
| `pg-ha-proxy` | TCP proxy for PostgreSQL connections with health checking |
| `pg-ha-api` | axum REST API for health checks and management |
| `pg-ha-ctl` | CLI tool for cluster administration |


## License

Apache-2.0
