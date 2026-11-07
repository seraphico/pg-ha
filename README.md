# pg-ha

[中文](README.zh.md)

PostgreSQL High Availability with built-in Raft consensus and load balancing.

## Overview

pg-ha is a Rust-based PostgreSQL HA solution aimed at the same job as [Patroni](https://github.com/patroni/patroni): automatic failover and synchronous replication, without etcd, ZooKeeper, Consul, or HAProxy. It does not implement every Patroni switch. Open gaps are listed in the TODO.

- **No etcd/ZooKeeper/Consul** — embedded Raft consensus via [openraft](https://github.com/databendlabs/openraft)
- **No HAProxy** — built-in TCP proxy with active health checking
- **Single binary deployment** — one process per node, zero external services required
- **TLS encrypted** — Raft RPC and the REST management API can both be secured with TLS. Set `raft.tls` / `restapi.tls` `cert` + `key` for server-auth HTTPS; add `ca_cert` + `require_client_cert: true` to enforce mutual TLS (client certificates verified against `ca_cert`). Omitting the `tls` block leaves that plane as plaintext (default). Basic Auth credentials on the REST API are only protected once REST TLS is enabled.
- **Graceful shutdown** — drain (renewing the leader lock), CHECKPOINT + stop bounded by the leader lease, lock release only after PostgreSQL stopped

## Documentation

| Document | 中文 | English |
|----------|------|---------|
| Architecture | [架构](docs/pg-ha-architecture.md) | [Architecture](docs/en/pg-ha-architecture.md) |
| Deployment | [部署指南](docs/deployment-guide.md) | [Deployment Guide](docs/en/deployment-guide.md) |
| Operations | [运维指南](docs/operations-guide.md) | [Operations Guide](docs/en/operations-guide.md) |
| Invariants | [不变量](docs/invariants.md) | [Invariants](docs/en/invariants.md) |
| TODO | [待办](docs/TODO.md) | [TODO](docs/en/TODO.md) |

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
