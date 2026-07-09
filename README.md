# pg-ha

PostgreSQL High Availability with built-in Raft consensus and load balancing.

## Overview

pg-ha is a Rust-based PostgreSQL HA solution providing complete feature parity with [Patroni](https://github.com/patroni/patroni) while eliminating external dependencies:

- **No etcd/ZooKeeper/Consul** — embedded Raft consensus via [openraft](https://github.com/databendlabs/openraft)
- **No HAProxy** — built-in TCP proxy via [Pingora](https://github.com/cloudflare/pingora)
- **Single binary deployment** — one process per node, zero external services required

## Architecture

Each node runs a single `pg-ha` agent process containing:

1. **HA Engine** — periodic decision loop managing failover and promotion
2. **Raft DCS** — embedded consensus cluster for leader election and state storage
3. **REST API** — health checks (compatible with HAProxy) and management endpoints
4. **TCP Proxy** — read/write splitting across PostgreSQL backends

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `pg-ha` | Main binary (composes all subsystems) |
| `pg-ha-core` | HA logic, PG lifecycle, cluster types, configuration |
| `pg-ha-dcs` | Raft state machine, network transport, DCS trait impl |
| `pg-ha-proxy` | Pingora TCP proxy for PostgreSQL connections |
| `pg-ha-api` | axum REST API for health checks and management |
| `pg-ha-ctl` | CLI tool for cluster administration |


## License

Apache-2.0
