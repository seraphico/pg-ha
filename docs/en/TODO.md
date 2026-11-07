# pg-ha TODO — production-readiness roadmap

[中文](../TODO.md)

Checked against the code in November 2026, tag `v0.1.23`. Finished items are marked done. Open items describe the gap that is actually in the code.

## Overview

pg-ha is a PostgreSQL HA system written in Rust. It aims at the same job as Patroni (automatic failover, synchronous replication, health checks) without etcd, ZooKeeper, Consul, or HAProxy. It does not copy every Patroni dependency or switch.

**Process:** one `pg-ha` binary per node, containing:

- **HA Engine** — periodic decision loop for failover and promotion
- **Raft DCS** — embedded consensus on openraft 0.9, instead of etcd
- **REST API** — axum health checks and management (port 8008, plaintext HTTP by default; `restapi.tls` switches it to HTTPS, optionally mTLS)
- **TCP Proxy** — read/write split (RW 6432, RO 6433) with active health checks, fall/rise, and per-backend `maxconn`. Probes use `http://` when REST is plaintext, `https://` when `restapi.tls` is set, and this node's server certificate under mTLS.

**Version:** 0.1.23 (matches git tag `v0.1.23`)
**Layout:** Cargo workspace, `edition = "2024"`. Crates: `pg-ha-core` (HA), `pg-ha-dcs` (Raft), `pg-ha-proxy`, `pg-ha-api` (REST), `pg-ha-ctl` (CLI).

**Shipped:**

- Automatic failover and manual switchover / failover (with synchronous replication on, the candidate must be in DCS `/sync`)
- Raft RPC TLS (`raft.tls`; the dev `docker-compose.yml` mounts certificates). `require_client_cert: true` verifies client certificates against `ca_cert`. Default `false` is server-auth TLS only
- REST TLS (`restapi.tls`, SB-03): `cert` + `key` enables HTTPS; adding `ca_cert` and `require_client_cert: true` enables mTLS (fail-closed). Omitting the block leaves plaintext HTTP. Same server mTLS machinery as Raft
- Append-only Raft WAL (`raft_log.wal`, compacted above 4 MiB) plus `hard_state.json`, `state_machine.json`, `membership.json`
- Watchdog (Linux `/dev/watchdog`; pet only a writable primary with a valid lease; timeout = TTL − `safety_margin`; default `mode: off`)
- Graceful shutdown: wait `shutdown.drain_timeout_secs` while renewing the lock, then CHECKPOINT and stop on the leader (`-m fast`, then `-m immediate`), bounded by the leader lease, then release the lock only after PostgreSQL has stopped. If it cannot be stopped, the process does not exit
- Prometheus metrics, text or JSON logs (`log_format` or `PG_HA_LOG_FORMAT`)
- Docker Compose 3-node dev cluster, `make up` / `make test`
- Dynamic synchronous replication (Priority mode), `/sync` and `/async` health checks, cascading replication (`tags.replicatefrom`)
- Standby cluster: dynamic `standby_cluster` follows a remote primary. `Ha::cascade_promote` has no REST or CLI entry

**Not on the running path:** Quorum sync mode. (`GET /history` is wired by SB-09: DCS `/history`, appended best-effort on a later Hold cycle after promote. Raft snapshots are on disk as `snapshot.json` and reloaded on restart, SB-06, item 14. A non-empty `/failover` intent expires with the cluster `ttl`, SB-05, item 13. Member `conn_url` no longer contains the password, SB-07 Option A. Failsafe was decided NO-GO and the surface was removed, SB-04, item 4.) `maximum_lag_on_syncnode`, `maximum_lag_on_failover` (SB-11), and REST `replication_lag` (SB-10) are wired.

---

## P0 — blocks production

Items 13 and 14 in the old numbering are closed. Item 15 (password in `conn_url`) is closed by SB-07 Option A. Item 1 (REST TLS, SB-03) and item 2 are done; item 2 stays here because Quorum mode is still listed under it.

### 1. REST API TLS — done (SB-03)

`src/main.rs` mirrors the Raft listener. No `restapi.tls` keeps plaintext `axum::serve`. `cert` + `key` serves HTTPS. `ca_cert` plus `require_client_cert: true` uses the shared `build_mtls_server_config` (`WebPkiClientVerifier`, fail-closed). Missing or unreadable `ca_cert` with `require_client_cert` aborts startup. Server fields match `raft.tls`: `cert`, `key`, `ca_cert`, `require_client_cert`.

`docker-compose.yml` and `docker/entrypoint.sh` take `PG_HA_RESTAPI_TLS_*`, default off (empty cert = plaintext). See the deployment guide, "REST API TLS / mTLS".

**Files:** `src/main.rs`, `crates/pg-ha-core/src/config.rs` (`RestApiTlsConfig`), `crates/pg-ha-api/src/lib.rs` (routing only).

**Proxy probes (SB-21, done):** `/primary` and `/replica` probes follow `restapi.tls`. Plaintext `http://` when unset; `https://` plus `ca_cert` when a certificate is configured; this node's REST server certificate when `require_client_cert` is set.

---

### 13. `/failover` had no TTL — fixed (SB-05)

A non-empty intent is written with `ttl: Some(lock_ttl())`, the same leader-lease TTL (default 30s, follows dynamic `ttl`). A dead or missing candidate drops out of the cluster view after about one TTL and election returns to "who is healthiest". Clearing the key still writes an empty string with no TTL.

**Where:** `RaftDcs::set_failover_value`, test `failover_intent_expires_and_clear_still_works`, `tests/e2e/test_failover_ttl.sh`, operations guide and invariants S3.

---

### 14. Raft snapshot was memory-only — done (SB-06)

The snapshot is stored in `snapshot.json` and loaded by `new_persistent` together with `hard_state.json`, `state_machine.json`, `raft_log.wal`, and `membership.json`. `get_current_snapshot` still returns it after restart.

**Where:** `crates/pg-ha-dcs/src/store.rs`.

---

### 15. Superuser password in member `conn_url` — done (SB-07 Option A)

`touch_member` writes only `host`, `port`, `dbname`, and `user`, each `libpq_quote`d. The password is not in the Raft log, `raft_log.wal`, `state_machine.json`, or snapshots. `pg_rewind`, `pg_basebackup`, and the pre-rewind `psql` probe take the peer password from this node's `postgresql.superuser.password` via the child `PGPASSWORD`. Every node's superuser password must match.

**Where:** `crates/pg-ha-core/src/ha/helpers.rs`, `crates/pg-ha-core/src/postgresql.rs`.

---

### 2. Synchronous-replication failover constraint — done (S5)

With `synchronous_mode` on, automatic election and a named Failover/Switchover candidate must be in DCS `/sync.sync_standby`. A missing, empty, or unsatisfiable strict placeholder promotes nobody. An ineligible candidate gets 409 `rejected`. `synchronous_mode_strict` only blocks commits; it does not vote.

**SB-01 (sync side):** `maximum_lag_on_syncnode` (bytes, `None` = off) is read from `pg_stat_replication`. Only a standby that is actually streaming and inside the lag is added to `/sync`. Two-phase writes keep `/sync` a subset of `synchronous_standby_names`. Strict mode uses an unsatisfiable placeholder, not `'*'`.

**Still open:**

- Quorum: `SyncMode::Quorum` can emit `ANY N (...)`, but `enforce_synchronous_replication` hardcodes `SyncMode::Priority`. There is no config switch. Only Priority / `FIRST N` runs.
- **SB-11 done:** `maximum_lag_on_failover` is read by `is_healthiest_node`. The reference is the largest `wal_position` among candidates that already passed eligibility, running, and sync gates (including self). A candidate farther behind than the threshold, including self, does not campaign. Unknown `wal_position` is excluded. `None` (default) turns the check off.
- **SB-10 done:** `timeline`, `wal_position`, and `replication_lag` on `/patroni`, `/primary`, `/replica`, and `/metrics` come from the cycle's `wal_status` via `ApiSnapshot` (no extra query). Lag is the leader's last published position minus this node's, in bytes, minimum 0. It can under-count by about one `loop_wait` of WAL under heavy write, and it measures received WAL, not applied WAL. The primary reports 0. Unknown is `null` and the metric line is omitted. `/replica?lag=` fails open when lag is unknown.

**Where:** `ha/election.rs` `sync_failover_allowed`, `ha/commands.rs`, `sync.rs` `SyncManager`, `ha/mod.rs` `enforce_synchronous_replication`, invariants S5.

---

## P1

### 3. Incremental Raft log persistence — done, including failure handling

When `data_dir` is set, the Raft log is an append-only `WalWriter` (`raft_log.wal`, bincode, compacted above 4 MiB). `hard_state.json`, `state_machine.json`, and `membership.json` stay atomic JSON. The whole log is no longer serialized to `log_entries.json`.

**Failure (invariant P4, holds):** a failed WAL or JSON write returns `StorageError`. openraft stops that node's Raft core. See invariants section 6.

**Optional later:** writes still update memory before disk because compaction needs the in-memory log. Strict write-ahead would not change P4.

**Where:** `crates/pg-ha-dcs/src/store.rs`, `crates/pg-ha-dcs/src/wal.rs` `COMPACTION_THRESHOLD`.

---

### 4. Failsafe — decided NO-GO, surface removed (SB-04)

Failsafe does not apply to embedded RaftDcs. Patroni's failsafe is for an external DCS: when etcd or Consul is down but PostgreSQL is healthy, the primary confirms topology over REST instead of demoting. pg-ha's Raft DCS is in-process, so:

- `RaftDcs::get_cluster` is a local read and never returns `Err`, so `handle_dcs_error` is unreachable.
- Loss of consensus is handled by lease-expiry fencing (invariants L5; SB-04 calls it R19). The primary fences itself if it cannot renew within `ttl - safety_margin`.

pg-ha does not support an external DCS. Keeping failsafe would mislead, so the surface is gone: `failsafe.rs`, the HA field, `Cluster.failsafe`, the `/failsafe` placeholder, and `pg_ha_failsafe_active`. `GlobalConfig.failsafe_mode` still parses and does nothing.

See invariants P3.

---

### 5. Proxy connection pool

After health checks (`HEAD /primary` and `HEAD /replica`, fall/rise, drop sessions on mark-down, default 300 connections per backend), the proxy is still accept, connect, and copy both ways. No transaction-level pooling. If a PgBouncer already sits in front, this can drop to P2.

**Where:** `crates/pg-ha-proxy/src/proxy.rs`.

---

## P2

### 6. Tests in CI

**Now:** `.github/workflows/ci.yml` runs `cargo fmt --check` and `cargo test --workspace` on every push and pull request. `.github/workflows/release.yml` still only builds on `v*` tags. `make e2e` runs `tests/e2e/` and is not in CI (slow and jittery; it stays a manual pre-release gate).

**Not done:**

- clippy with `-D warnings` in CI
- compose e2e in CI
- a Raft TLS integration test

---

### 7. Automatic TLS reload

Certificates are loaded once at startup. Either watch the files and reload, or document restart after an external rotator (cert-manager, Vault).

---

### 8. REST API extras

- Rate limiting
- An audit log of management calls
- CORS, if a dashboard is built
- `pg-ha-ctl` Basic Auth and the REST TLS client are done (SB-08): `--username` / `--password` (or `PG_HA_USERNAME` / `PG_HA_PASSWORD`) on management calls; `--cacert` / `--cert` / `--key` for HTTPS and mTLS. One side of a pair exits 2.

---

### 9. Backup and restore

No built-in backup. Integrate pgBackRest or WAL-G, or add a callback around switchover and failover.

---

## P3

### 10. Standby cluster / multiple regions

`StandbyClusterConfig` (`host`, `port`, `primary_slot_name`, `restore_command`, `create_replica_methods`) lives under dynamic `standby_cluster`. The lock holder follows the remote. `Ha::cascade_promote` can drop that config and promote. There is no REST or `pg-ha-ctl` command for it. A cross-region runbook for slots and archive is still missing.

### 11. Web dashboard

A UI for the cluster. Not started.

### 12. Benchmarks

Failover time, proxy throughput and latency, Raft write throughput on the WAL path.

---

## Code TODOs that were closed

| Place | What happened |
|------|----------------|
| `raft_dcs.rs` `get_cluster` | `/failsafe` read removed (SB-04) |
| `set_failover_value` | non-empty intent carries the cluster `ttl` (SB-05) |
| `store.rs` snapshot | persisted to `snapshot.json` and reloaded (SB-06) |
| `helpers.rs` `conn_url` | password removed (SB-07) |
| `election.rs` | `maximum_lag_on_failover` filters candidates (SB-11) |
| REST `replication_lag` | filled by the HA cycle (SB-10) |

---

## Where to start in the code

1. Name the TODO number.
2. Entry points: `src/main.rs`, `crates/pg-ha-core/src/ha/mod.rs`, `crates/pg-ha-core/src/config.rs`, `crates/pg-ha-dcs/src/raft_dcs.rs`, `crates/pg-ha-dcs/src/store.rs`, `crates/pg-ha-dcs/src/wal.rs`.
3. Build and test: `cargo build` / `make build` (Linux binaries for the container; on a Mac via `cargo-zigbuild`, `cross`, or `docker-compose.build.yml`). `make test` is `cargo test --workspace`. `make up` builds and starts the 3-node cluster (default PostgreSQL 16; `PG_VERSION` selects the official `postgres` image). `make e2e` runs the end-to-end scripts and is not in CI.
4. Toolchain: Rust **1.99.1** (`rust-toolchain.toml`, image `rust:1.99.1-bookworm`, CI `dtolnay/rust-toolchain@1.99.1`). `edition = "2024"`. Do not float on `stable`. openraft 0.9. Logs use `tracing`. Errors use `thiserror` / `anyhow`.
