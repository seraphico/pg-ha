# pg-ha Deployment Guide

[中文](../deployment-guide.md)

## System Requirements

| Component | Minimum version | Notes |
|-----------|-----------------|-------|
| PostgreSQL | 14+ | Requires `pg_ctl`, `pg_basebackup`, `pg_rewind`, `pg_controldata`, and `psql`. Docker uses official `postgres` image tags; `make up` defaults to 16 |
| Rust | 1.99.1 | Pinned by `rust-toolchain.toml`. `edition = "2024"`, and uses if-let chains that are already stable. Nightly is not required. The Docker build image and CI use the same version |
| Docker | 24.0+ | Containerized deployment |
| Docker Compose | v2.20+ | Multi-node orchestration |
| Operating system | Linux (amd64/arm64) / macOS | Linux is recommended for production |

**Hardware recommendations (per node):**

| Environment | CPU | Memory | Disk |
|-------------|-----|--------|------|
| Development/test | 2 cores | 2 GB | 10 GB SSD |
| Production | 4+ cores | 8+ GB | Sized to the data volume; SSD/NVMe |

**Network requirements:**

- Latency between cluster nodes < 5ms (Raft consensus performance depends on low latency)
- The following ports must be reachable between nodes: 2380 (Raft RPC; HTTPS with mTLS, only nodes holding a member certificate can connect), 8008 (REST API; HTTPS when `restapi.tls` is configured, otherwise plaintext HTTP), 5432 (PostgreSQL replication)

---

## Quick Start

Start a three-node development cluster with `make up`:

```bash
# Clone the project
git clone <repo-url> pg-ha && cd pg-ha

# Start the three-node cluster (compile Linux binaries + build the image + start; default PG 16)
make up

# Use another official postgres image tag
make up PG_VERSION=17
make up PG_VERSION=18

# Verify cluster topology (member names, roles, replication upstream)
curl -s http://localhost:8008/cluster | jq .

# View this node's details (role, state, timeline, lag)
curl -s http://localhost:8008/patroni | jq .

# REST_TLS=tls: use HTTPS and trust the development CA; REST_TLS=mtls also sends a client certificate
curl -s --cacert docker/tls/ca.crt https://localhost:8008/cluster | jq .
curl -s --cacert docker/tls/ca.crt --cert docker/tls/client.crt --key docker/tls/client.key https://localhost:8008/cluster | jq .

# Connect to PostgreSQL (through the Proxy RW port)
psql -h localhost -p 6432 -U postgres -d postgres
# Password: secret
# Basic Auth for the management API is also admin / secret (only /switchover, /config, and similar endpoints)

# View cluster logs
make logs

# Stop and wipe data
make down
```

**Port mappings after startup (docker-compose):**

| Node | PostgreSQL | REST API | Proxy RW | Proxy RO | Raft RPC |
|------|-----------|----------|----------|----------|----------|
| node1 | 5432 | 8008 | 6432 | 6433 | 2380 (internal) |
| node2 | 5433 | 8009 | 6434 | 6435 | 2380 (internal) |
| node3 | 5434 | 8010 | 6436 | 6437 | 2380 (internal) |

---

## Production Deployment (Bare Metal, 3 Nodes)

### Prerequisites

Install the following on each server:

```bash
# Install PostgreSQL 16
sudo apt-get install postgresql-16 postgresql-client-16

# Confirm the bin path
ls /usr/lib/postgresql/16/bin/pg_ctl

# Create the pg-ha user and directories
sudo useradd -r -m -s /bin/bash pgha
sudo mkdir -p /var/lib/pg-ha/raft /var/lib/postgresql/16/data /etc/pg-ha
sudo chown pgha:pgha /var/lib/pg-ha/raft /var/lib/postgresql/16/data
```

### Deploying the Binaries

Download the `pg-ha-*.tar.gz` for the platform from the GitHub Release (a `v*` tag triggers `.github/workflows/release.yml`), or compile locally.

On Linux you can build directly:

```bash
cargo build --release -p pg-ha -p pg-ha-ctl
sudo cp target/release/pg-ha target/release/pg-ha-ctl /usr/local/bin/
sudo chmod +x /usr/local/bin/pg-ha /usr/local/bin/pg-ha-ctl
```

To produce the Linux binaries that go into the Docker image, use `make build` from the repository. It selects `x86_64-unknown-linux-gnu` or `aarch64-unknown-linux-gnu` according to the host architecture: when the host already has `cargo-zigbuild` or `cross`, it uses those; on a Linux host it uses `cargo`; otherwise it uses the builder in `docker-compose.build.yml`. The artifacts are in `target/<target>/release/`.

```bash
make build
# Binaries are in target/$RUST_TARGET/release/; see RUST_TARGET with `make print-env`
sudo cp target/x86_64-unknown-linux-gnu/release/pg-ha /usr/local/bin/   # change the directory to match print-env
sudo cp target/x86_64-unknown-linux-gnu/release/pg-ha-ctl /usr/local/bin/
```

Validate the configuration or generate a sample (does not start the process):

```bash
pg-ha --validate-config /etc/pg-ha/pg-ha.yml
pg-ha --generate-sample-config
```

### Configuration File

Create `/etc/pg-ha/pg-ha.yml` on each node (change `name` and `raft.self_addr` for the node):

```yaml
name: node1                    # different on each node: node1, node2, node3
scope: prod-cluster
namespace: service
loop_wait: 10
ttl: 30
retry_timeout: 10

postgresql:
  data_dir: /var/lib/postgresql/16/data
  bin_dir: /usr/lib/postgresql/16/bin
  listen: 0.0.0.0
  port: 5432
  superuser:
    username: postgres
    password: "CHANGE_ME_superuser_password"
    dbname: postgres
  replication:
    username: replicator
    password: "CHANGE_ME_replication_password"
    dbname: postgres
  parameters:
    max_connections: "200"
    wal_level: replica
    max_wal_senders: "10"
    max_replication_slots: "10"
    hot_standby: "on"
    shared_buffers: "2GB"
    effective_cache_size: "6GB"
    wal_keep_size: "1GB"

restapi:
  listen: 0.0.0.0
  port: 8008
  username: admin
  password: "CHANGE_ME_api_password"
  tls:                              # optional. Omit it and the REST API is plaintext HTTP (same as before)
    cert: /etc/pg-ha/tls/server.crt # setting cert and key together enables HTTPS
    key: /etc/pg-ha/tls/server.key
    ca_cert: /etc/pg-ha/tls/ca.crt  # for mTLS: CA that verifies client certificates
    require_client_cert: true       # true forces mTLS (ca_cert must be set); default false is server-only TLS
  # Basic Auth credentials are sent in the clear on the network until TLS is enabled. When tls is not configured the port is still plaintext HTTP;
  # do not expose it to the public internet.

raft:
  self_addr: "10.0.1.1:2380"  # different on each node: .1, .2, .3
  partner_addrs:
    - "10.0.1.2:2380"         # the other two
    - "10.0.1.3:2380"
  data_dir: /var/lib/pg-ha/raft
  tls:                              # required. Raft RPC is always HTTPS + mTLS; without it validation fails and the node refuses to start
    cert: /etc/pg-ha/tls/node1.crt  # this node's member certificate (one per node, SAN only lists this node's host)
    key: /etc/pg-ha/tls/node1.key
    ca_cert: /etc/pg-ha/tls/ca.crt  # required: CA that verifies peer certificates
    # client_cert / client_key default to cert / key (the member certificate is both server and outbound client certificate)

log_format: json                    # text (default) or json

shutdown:
  drain_timeout_secs: 30            # seconds to wait after SIGTERM and before CHECKPOINT, renewing the lock every loop_wait

proxy:
  rw_listen: 0.0.0.0
  rw_port: 6432
  ro_listen: 0.0.0.0
  ro_port: 6433

watchdog:
  mode: "off"

bootstrap:
  initdb:
    - data-checksums
    - encoding: UTF8
  dcs:
    loop_wait: 10
    ttl: 30
    maximum_lag_on_failover: 1048576   # election rejects candidates that lag the cluster's maximum wal_position by more than N bytes; None/omitted = off
  post_bootstrap_sql:
    - "CREATE USER replicator WITH REPLICATION PASSWORD 'CHANGE_ME_replication_password'"
```

### systemd Service File

Create `/etc/systemd/system/pg-ha.service`:

```ini
[Unit]
Description=pg-ha PostgreSQL High Availability Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pgha
Group=pgha
ExecStart=/usr/local/bin/pg-ha /etc/pg-ha/pg-ha.yml
Restart=on-failure
RestartSec=5s
# Must be greater than drain_timeout_secs (default 30) + CHECKPOINT + pg_ctl stop (no more than ttl − safety_margin while holding the lock,
# at most about 2 × 60 seconds while not holding the lock) + retry_timeout; otherwise systemd SIGKILLs pg-ha and the shutdown safety rules cannot run
# (see the Operations Guide, "Graceful Shutdown")
TimeoutStopSec=180s
KillMode=mixed
KillSignal=SIGTERM

# Environment variables
Environment=RUST_LOG=pg_ha=info
Environment=PG_HA_LOG_FORMAT=json

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Security hardening
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable pg-ha
sudo systemctl start pg-ha

# Check status
sudo systemctl status pg-ha
journalctl -u pg-ha -f
```

---

## Configuration Reference

### Full YAML Schema

```yaml
# ─── Required fields ───
name: <string>                     # unique node name (required)
scope: <string>                    # cluster name/scope (required)

# ─── Optional top-level fields ───
namespace: <string>                # DCS key path prefix (default: "service")
loop_wait: <u64>                   # HA loop interval in seconds (default: 10)
ttl: <u64>                         # leader lock TTL in seconds (default: 30, must be > loop_wait, and see the constraint below)
retry_timeout: <u64>               # timeout in seconds for each DCS write; a timeout is treated as a write failure; also the grace period after reconfiguring the upstream (default: 10, at least 1)
                                   # constraint: ttl >= loop_wait + retry_timeout + watchdog.safety_margin; startup is refused when it is not met

# ─── PostgreSQL configuration (required) ───
postgresql:
  data_dir: <path>                 # PGDATA path (required)
  bin_dir: <path>                  # PG tools directory (required; must contain pg_ctl, pg_rewind, pg_basebackup, pg_controldata, and psql; psql is used before pg_rewind to confirm the source answers as a primary)
  listen: <string>                 # PG listen address (default: "0.0.0.0")
  port: <u16>                      # PG port (default: 5432)
  superuser:                       # superuser connection parameters (required)
    username: <string>             # username (required)
    password: <string>             # password (optional; see "Cross-node disaster-recovery credentials" below — must match on every node)
    dbname: <string>               # database name (default: "postgres")
  replication:                     # replication user connection parameters (required)
    username: <string>             # username (required)
    password: <string>             # password (optional)
    dbname: <string>               # database name (default: "postgres")
  remove_data_directory_on_rewind_failure: <bool>  # on pg_rewind failure, delete the data directory and rebuild with pg_basebackup (default: false; when false the node stops PostgreSQL and waits for an operator, see Operations Guide scenario 3)
  parameters:                      # PG parameters (optional, map<string, string>)
    <param_name>: <value>
```

> **Cross-node disaster-recovery credentials (SB-07, important)**: The superuser password is **not written to DCS/Raft**. A member `conn_url` contains only host/port/dbname/user. But the three disaster-recovery paths — `pg_rewind`, `pg_basebackup` (bootstrap clone and rejoin rebuild), and the `psql` liveness probe before rewind — must connect to the **peer** node as the superuser. They take the password from this node's **local** `postgresql.superuser.password` (passed in through the child process's `PGPASSWORD`; not written to disk, not placed in argv, and redacted in logs). Therefore **`postgresql.superuser.username` / `postgresql.superuser.password` must be identical on every node in the cluster** (one superuser for the cluster); otherwise cross-node rewind / clone / probe fails because authentication fails. If `superuser.password` is left empty, these commands do not set `PGPASSWORD` and rely on local PostgreSQL authentication such as trust/peer/ident or `.pgpass`.

```yaml

# ─── REST API configuration (required) ───
restapi:
  listen: <string>                 # API listen address (default: "0.0.0.0")
  port: <u16>                      # API port (default: 8008)
  username: <string>               # Basic Auth username (optional; once set, management endpoints require authentication)
  password: <string>               # Basic Auth password (optional)
  tls:                             # optional. Omit it and the REST API is plaintext HTTP (same as before)
    cert: <path>                   # server certificate PEM (HTTPS is enabled when set together with key)
    key: <path>                    # server private key PEM
    ca_cert: <path>                # optional. For mTLS: the CA that verifies client certificates
    require_client_cert: <bool>    # when true, force mTLS (ca_cert must be set) and reject connections with no certificate or an external CA; default false is server-only TLS
  # Basic Auth credentials are protected in transit only after TLS is enabled; over plaintext HTTP the credentials are transmitted in the clear.

# ─── Raft DCS configuration (required) ───
raft:
  self_addr: <string>              # this node's Raft RPC address, host:port (required)
  partner_addrs:                   # Raft addresses of the other nodes (required, at least 1)
    - <string>
  data_dir: <path>                 # persistence directory (optional). When set, writes raft_log.wal,
                                   # hard_state.json, state_machine.json, and membership.json
  node_id: <u64>                   # explicit node ID (optional; the default is the position after sorting the addresses, plus 1)
  allow_insecure_raft: <bool>      # default false. Takes effect only when there is no tls block: Raft RPC falls back to
                                   # unauthenticated plaintext HTTP and logs a WARN at startup. Development/testing only.
                                   # Ignored when a tls block exists (mTLS wins)
  tls:                             # required (unless allow_insecure_raft: true). Raft RPC is always HTTPS + mTLS
    cert: <path>                   # this node's member certificate PEM (required)
    key: <path>                    # matching private key PEM (required)
    ca_cert: <path>                # CA that verifies peer certificates (required; validation fails without it)
    client_cert: <path>            # outbound client certificate (optional, defaults to cert; set together with client_key or omit both)
    client_key: <path>             # outbound client private key (optional, defaults to key)
    # require_client_cert was removed: mTLS is always on. An old config with true has no effect; false fails validation

# ─── TCP Proxy configuration (required) ───
proxy:
  rw_listen: <string>              # RW Proxy listen address (default: "0.0.0.0")
  rw_port: <u16>                   # RW Proxy port (default: 6432)
  ro_listen: <string>              # RO Proxy listen address (default: "0.0.0.0")
  ro_port: <u16>                   # RO Proxy port (default: 6433)

# ─── Logging and shutdown (optional) ───
log_format: <string>               # "text" (default) or "json". Can also be overridden by PG_HA_LOG_FORMAT
shutdown:
  drain_timeout_secs: <u64>        # seconds to wait after SIGTERM, renewing the lock every loop_wait, and only then CHECKPOINT (default: 30)

# ─── Watchdog configuration (optional) ───
watchdog:
  mode: <string>                   # "off" | "automatic" | "required" (default: "off")
                                   # required: a node whose device is unavailable does not stand for election and does not initialize the cluster (publishes watchdog_failed)
                                   # Docker entrypoint: environment variable PG_HA_WATCHDOG_MODE (default off)
  device: <string>                 # watchdog device path (default: "/dev/watchdog")
  safety_margin: <u64>             # safety margin in seconds (default: 5). Also used for the leader lease: if the lock holder
                                   # fails to renew again within ttl - safety_margin seconds after the last renewal started, it stops PostgreSQL. Must be greater than the wall-clock skew between nodes

# ─── Node tags (optional) ───
tags:
  nofailover: <bool>               # forbid this node from becoming Primary (default: false)
  noloadbalance: <bool>            # exclude this node from the RO Proxy (default: false)
  noclone: <bool>                  # forbid use as a clone source (default: false)
  nosync: <bool>                   # do not take part in synchronous replication (default: false)
  nostream: <bool>                 # parsed only; default false. The HA runtime path does not read this field
  clonefrom: <bool>                # prefer this node as a clone source (default: false)
  replicatefrom: <string>          # replicate from the named node (cascading replication, optional)
  failover_priority: <u32>         # failover priority (default: 1, 0 = equivalent to nofailover)
  sync_priority: <u32>             # synchronous replication priority (default: 0)

# ─── Bootstrap configuration (optional, used only on first initialization) ───
bootstrap:
  initdb:                          # list of initdb options
    - <string>                     # flag option (for example "data-checksums")
    - <key>: <value>               # key-value option (for example encoding: UTF8)
  dcs:                             # initial dynamic configuration (written to DCS /config)
    loop_wait: <u64>
    ttl: <u64>
    maximum_lag_on_failover: <u64>   # election rejects candidates that lag the cluster's maximum wal_position by more than N bytes; None/omitted = off
    synchronous_mode: <async|sync|quorum> # replication mode (default: async): async / sync (priority-based) / quorum (quorum-based); only these three lowercase strings are accepted, any other value (including true/false) fails startup; enabling quorum requires every node on the same pg-ha version
    synchronous_mode_strict: <bool># whether to block writes when there is no synchronous standby (default: false)
    synchronous_node_count: <u32>  # number of synchronous standbys (default: 1)
  post_init: [<string>]            # paths of SQL scripts to run after initialization
  custom_command: <string>         # custom bootstrap command (replaces initdb)
  post_bootstrap_sql: [<string>]   # SQL statements to run after bootstrap
```

Tags related to synchronous replication: `tags.nosync` (exclude the node from synchronous standbys), `tags.sync_priority` (synchronous priority). Every node publishes its own `tags` in its DCS member record on each heartbeat, and `GET /patroni` reports them; peers use the published values for election, candidate validation, synchronous standby selection and the cascade topology. Synchronous replication can also be switched on or off at runtime with `PATCH /config`. See the [Operations Guide](operations-guide.md#synchronous-replication). For `synchronous_mode: "quorum"` (requirements, enabling, known limitation) see [Quorum synchronous replication](operations-guide.md#quorum-synchronous-replication).

### Environment Variables

The binary overrides only the fields below (`Config::apply_env_overrides`, plus the log format read separately in `main`). `PG_HA_*` variables that are not listed do **not** change the YAML. Nested fields use a double underscore `__`.

| Environment variable | Field overridden |
|---------|---------|
| `PG_HA_NAME` | `name` |
| `PG_HA_SCOPE` | `scope` |
| `PG_HA_LOOP_WAIT` | `loop_wait` |
| `PG_HA_TTL` | `ttl` |
| `PG_HA_RETRY_TIMEOUT` | `retry_timeout` |
| `PG_HA_POSTGRESQL__PORT` | `postgresql.port` |
| `PG_HA_POSTGRESQL__LISTEN` | `postgresql.listen` |
| `PG_HA_RESTAPI__PORT` | `restapi.port` |
| `PG_HA_RAFT__SELF_ADDR` | `raft.self_addr` |
| `PG_HA_LOG_FORMAT` | log format; takes precedence over YAML `log_format` (`json` is JSON; any other value is text) |
| `RUST_LOG` | `tracing` filter directive. When unset, `pg_ha=info` is appended by default and `openraft::replication` is turned off |

The Docker entrypoint `docker/entrypoint.sh` is a separate layer: it **generates** `/etc/pg-ha.yml` from the variables below, and does not call the override logic in the table above. `PG_HA_RAFT_SELF` and `PG_HA_RAFT__SELF_ADDR` are not the same variable.

| Entrypoint variable | Effect |
|---------|------|
| `PG_HA_NAME` / `PG_HA_SCOPE` | node name and cluster name (default hostname / `pg-ha-cluster`) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | superuser and replication user (the replication user is the same as the superuser) |
| `PGDATA` / `PG_PORT` / `PG_HA_BIN_DIR` | data directory, port, and `bin_dir` (by default, probed from `PG_MAJOR` in the image) |
| `PG_HA_API_PORT` / `PG_HA_PROXY_RW_PORT` / `PG_HA_PROXY_RO_PORT` | API and proxy ports |
| `PG_HA_RAFT_SELF` / `PG_HA_RAFT_PARTNERS` | this node's address, and a comma-separated list of peers |
| `PG_HA_RAFT_TLS_CERT` and `_KEY` / `_CA_CERT` / `_CLIENT_CERT` / `_CLIENT_KEY` | when `PG_HA_RAFT_TLS_CERT` is set, write `raft.tls` (Raft is always mTLS). `_CLIENT_CERT` / `_CLIENT_KEY` default to `_CERT` / `_KEY` |
| `PG_HA_RAFT_ALLOW_INSECURE` | when `true` and `PG_HA_RAFT_TLS_CERT` is unset, write `raft.allow_insecure_raft: true` (plaintext Raft, development only). When neither is set, the entrypoint prints an error and exits |
| `PG_HA_RESTAPI_TLS_CERT` and `_KEY` / `_CA_CERT` / `_REQUIRE_CLIENT_CERT` | when `PG_HA_RESTAPI_TLS_CERT` is set, write `restapi.tls` (REST API HTTPS/mTLS); leaving it unset, the default, means plaintext HTTP |
| `PG_HA_TAG_NOFAILOVER` / `_NOLOADBALANCE` / `_NOCLONE` / `_NOSYNC` / `_NOSTREAM` / `_CLONEFROM` (`true` or `false`), `PG_HA_TAG_FAILOVER_PRIORITY` / `_SYNC_PRIORITY` (integer), `PG_HA_TAG_REPLICATEFROM` (member name) | each variable that is set is written as the matching key of the `tags:` section; when none is set no `tags:` section is written (the default). An invalid value stops the container at start |

The development cluster `docker-compose.yml` enables Raft mTLS by default (the member certificate is `docker/tls/server.crt`, whose SAN covers node1 to node5 and the container names, and which serves as both server and outbound client certificate; it is a shared development certificate, production needs one per node) and mounts `./docker/tls` at `/etc/pg-ha/tls`. REST API TLS is off by default (an empty `PG_HA_RESTAPI_TLS_CERT` means plaintext HTTP). The recommended way to switch is the Makefile toggle: `make up REST_TLS=off|tls|mtls` (default `off`; `make up-tls` / `make up-mtls` also work). It exports the `PG_HA_RESTAPI_TLS_*` variables below. `make status` and `tests/e2e/*.sh` read the same `REST_TLS` (an environment variable, default `off`; for `tls`/`mtls` they reach HTTPS with the CA and client certificate under `docker/tls`). A given port is either plaintext or TLS and cannot listen as both. To switch modes, run `make up REST_TLS=<new mode>` directly; `make down` is not required first. When the environment variables change, `docker compose up -d` recreates the three containers (the Raft/PGDATA volumes are kept), the entrypoint rewrites `/etc/pg-ha.yml` when the container starts, and the new mode takes effect (verified in Docker for off→tls and tls→mtls). Note that `make restart REST_TLS=...` does not switch the mode (it only restarts the existing containers and keeps the environment variables from when they were created). Use `make down` only when the data should be wiped as well. To enable it manually, set `PG_HA_RESTAPI_TLS_CERT=/etc/pg-ha/tls/server.crt`, and for mTLS also set `PG_HA_RESTAPI_TLS_REQUIRE_CLIENT_CERT=true` (`ca_cert` defaults to `/etc/pg-ha/tls/ca.crt`). The HA parameters generated by the entrypoint are `loop_wait: 5`, `ttl: 15`, and `retry_timeout: 5` (`watchdog.safety_margin` uses the default 5, which exactly satisfies `ttl >= loop_wait + retry_timeout + safety_margin`). REST authentication is `admin` / `secret`.

---

## TLS Configuration

### Certificate Generation (Self-Signed CA Example)

```bash
# 1. Generate the CA private key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=pg-ha-ca/O=pg-ha"

# 2. Generate a member certificate for each node (one per node, SAN lists only that node's host)
#    Shown for node1 / 10.0.1.1; repeat for node2 and node3 with their own host names and IPs
openssl genrsa -out node1.key 2048
openssl req -new -key node1.key -out node1.csr \
  -subj "/CN=node1/O=pg-ha"

cat > node1.ext << EOF
subjectAltName = DNS:node1, IP:10.0.1.1
extendedKeyUsage = serverAuth, clientAuth
EOF

openssl x509 -req -days 365 -in node1.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out node1.crt -extfile node1.ext

# 3. Generate a client certificate for REST / pg-ha-ctl / Prometheus (no member-host SAN; used for restapi mTLS)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/CN=pg-ha-client/O=pg-ha"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt
```

### Certificate File Distribution

```
/etc/pg-ha/tls/
├── ca.crt         # CA certificate (identical on every node)
├── node1.crt      # this node's member certificate (unique per node, SAN lists only this node's host)
└── node1.key      # matching private key (mode 600, never copied to other nodes)
```

Set permissions:

```bash
sudo chown pgha:pgha /etc/pg-ha/tls/*
sudo chmod 600 /etc/pg-ha/tls/*.key
sudo chmod 644 /etc/pg-ha/tls/*.crt
```

Write the paths into `raft.tls` on each node (see the production YAML above). Raft RPC (2380) is always HTTPS + mTLS, with no switch: the server requires the peer to present a client certificate, verifies the chain, validity and `clientAuth` usage against `ca_cert`, and then applies the member identity check below. A missing `raft.tls`, a missing `ca_cert`, or an old config that sets `require_client_cert: false` makes both `--validate-config` and startup fail. `client_cert` / `client_key` default to `cert` / `key`, so one member certificate serves as both the server certificate and the outbound client certificate.

**Member identity rule.** After the chain verifies, the leaf certificate's DNS or IP SAN must match at least one "member host" (SAN only, never CN), or the handshake is rejected. Member hosts are the union of the following, recomputed on every handshake:

- the host part of `raft.self_addr` and `raft.partner_addrs` in the config;
- the host part of every node address in the live Raft membership.

Identity therefore follows membership: a new node's certificate is accepted as soon as the node is in the membership, and a removed node's certificate stops being accepted once it is neither in the config list nor in the membership. The config half keeps bootstrap working, when a fresh node does not know the membership yet. Startup also self-checks: the outbound certificate (`client_cert`, default `cert`) must carry a DNS/IP SAN equal to the host of this node's `self_addr`, otherwise peers would reject it and the process exits with an error.

**Certificate requirements and cautions:**

- One certificate per node, SAN listing only that node's host, EKU containing both `serverAuth` and `clientAuth`. A leaked node key then impersonates one node only.
- A certificate that lists several node names (for example the development `docker/tls/server.crt`) is accepted if any SAN matches any member host, and cannot be tied to one node. That is acceptable for development (a private compose network, one trust domain) and not for production.
- Do not use wildcard SANs (such as `*.db.example.com`): they match every member host under that domain.
- The SAN must have the same form as the host written in `self_addr` / `partner_addrs` (an IP needs an IP SAN, a host name needs a DNS SAN).
- Use a separate CA for Raft. REST, pg-ha-ctl and Prometheus certificates from the same CA are rejected by Raft as long as they carry no member-host SAN, but a REST server certificate that has a member SAN and the `clientAuth` usage would be accepted. If you share a CA, issue REST/ctl/Prometheus certificates without member SANs.
- Adding a node: issue its certificate, add it to `partner_addrs` on the nodes, and join it to the membership as described in the [operations guide](operations-guide.md); its certificate is accepted once it is a member. Rotating certificates: they are loaded once at startup with no hot reload, so replace the files node by node and restart the process.

**Development mode (development/testing only).** With no `raft.tls` and `raft.allow_insecure_raft: true` (`PG_HA_RAFT_ALLOW_INSECURE=true` in the container), Raft RPC is unauthenticated plaintext HTTP and a WARN is logged at startup. Do not use it in production.

### REST API TLS / mTLS (8008)

The REST management API also supports built-in HTTPS. Its configuration shape matches the server side of `raft.tls` (the same field names), and the same CA can be reused, though issuing separate certificates is better (see the member identity rule above):

- Setting `cert` + `key` under `restapi.tls` switches 8008 from plaintext HTTP to HTTPS (server authentication only).
- Also setting `ca_cert` and `require_client_cert` to `true` enables mTLS: the server requires the client to present a certificate issued by `ca_cert`, and rejects connections with no certificate (TLS alert 116 `certificate_required`) or a certificate from an external CA (TLS alert 48 `unknown_ca`). When `require_client_cert=true`, if `ca_cert` is missing, unreadable, or empty, the process reports an error and exits at startup (fail-fast).
- Omitting the whole `restapi.tls` block behaves exactly as before: plaintext HTTP, and existing deployments are unaffected.

**How Basic Auth relates to TLS**: Basic Auth itself is unchanged and still applies after TLS is enabled. Basic Auth credentials are protected in transit only after TLS is enabled on the REST API. Over plaintext HTTP, the credentials and every management command (switchover/failover/restart/reinitialize/config, and so on) are transmitted in the clear. In production, enable TLS for the REST API.

Proxy probes of `/primary` and `/replica` use the same `restapi.tls`. When it is unset, probes use plaintext `http://`. After `cert` + `key` are set, probes switch to `https://` and verify the server certificate with `ca_cert`. When `require_client_cert: true`, probes present this node's own REST server certificate.

**Every node's REST TLS mode must be the same**: all plaintext, all HTTPS (server-auth), or all mTLS. Mixed clusters are not supported. Each node's proxy probes every member with its own `restapi.tls`, and rewrites the scheme of the `api_url` published by the member to this node's scheme. If a member publishes a scheme that differs from this node, the log warns once for that member (`All nodes must use the same restapi.tls mode`), but behavior continues using this node's scheme. That does not make a mixed cluster actually usable.

**Quorum votes use the same material.** With `synchronous_mode: "quorum"`, a failover candidate calls `GET /wal` on every voter over the same `restapi.tls` (HTTPS verified with `ca_cert`, and its own server certificate as the client certificate under mTLS). `GET /wal` needs no Basic Auth. Every node must therefore reach every other node's port 8008, and every node's certificate must be accepted by every other node (with mTLS, also usable for `clientAuth`). See the [Operations Guide](operations-guide.md#quorum-synchronous-replication).
**Certificate requirements for proxy probes**:
- `ca_cert` is also the CA that proxy probes use to trust server certificates. With only `cert` + `key` (server-auth TLS) and no `ca_cert`, probes trust only the system root certificates. When the certificate is issued by a private CA, every backend is marked DOWN, the proxy refuses all connections on 6432/6433, and leader election is unaffected. At startup there is one `restapi.tls is set without ca_cert` warning. The reason a probe failed (the URL and the reqwest error chain) is also logged as `Health probe failed` (at most one line every 60 seconds while the same backend keeps failing). Certificates issued by a public CA are not affected.
- The server certificate's SAN must cover the hostname used in `raft.self_addr` (probes connect using the hostname published by the member; verification fails when that hostname is not in the SAN).
- An mTLS probe presents this node's REST server certificate as the client certificate, so that certificate's Extended Key Usage must also allow `clientAuth` (a CA that issues only `serverAuth` makes the probe handshake fail).

`raft.tls` does not affect the REST API, and `restapi.tls` does not affect Raft RPC (2380). The two are configured independently. REST `require_client_cert` is still optional; only Raft lost it.

---

## Docker Deployment

### Choosing the PostgreSQL Version

The image is the official `postgres:${PG_VERSION}`. `make up` passes `PG_VERSION` to compose; the default is **16**. When `docker/Dockerfile` does not receive that argument, its own `ARG` defaults to 19. A normal `make up` does not use that default.

```bash
# Default PostgreSQL 16
make up

# Use another official image tag
make up PG_VERSION=18

PG_VERSION=17 docker compose build
docker compose up -d
```

The entrypoint fills `bin_dir` and `data_dir` from `PG_MAJOR` inside the container (or from `PG_HA_BIN_DIR` / `PGDATA`).

The compose file in the repository is `docker-compose.yml` (development cluster, Raft TLS, scope `test-cluster`). The production example below is not a file in the repository. Save it separately before use, and include the same TLS variables as the development cluster, or change it to mount your own certificates.

### Production docker-compose Example

```yaml
# docker-compose.prod.yml (example; this file is not in the repository)
services:
  node1:
    image: pg-ha:latest
    container_name: pg-ha-node1
    hostname: node1
    restart: unless-stopped
    environment:
      PG_HA_NAME: node1
      PG_HA_SCOPE: prod-cluster
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
      PGDATA: /var/lib/postgresql/data
      PG_HA_RAFT_SELF: "node1:2380"
      PG_HA_RAFT_PARTNERS: "node2:2380,node3:2380"
      PG_HA_RAFT_TLS_CERT: /etc/pg-ha/tls/server.crt   # member certificate; the client certificate defaults to it
      PG_HA_RAFT_TLS_KEY: /etc/pg-ha/tls/server.key
      PG_HA_RAFT_TLS_CA_CERT: /etc/pg-ha/tls/ca.crt
      PG_HA_LOG_FORMAT: json
      RUST_LOG: "pg_ha=info"
    ports:
      - "5432:5432"
      - "8008:8008"
      - "6432:6432"
      - "6433:6433"
    volumes:
      - node1-raft:/var/lib/pg-ha/raft
      - node1-pgdata:/var/lib/postgresql/data
      - ./docker/tls:/etc/pg-ha/tls:ro
    networks:
      - pg-ha-net
    healthcheck:
      # Plaintext REST (this example does not set PG_HA_RESTAPI_TLS_CERT). After restapi.tls is enabled this must be changed to HTTPS,
      # or the health check fails forever:
      #   server-auth TLS: ["CMD", "curl", "-sf", "--cacert", "/etc/pg-ha/tls/ca.crt", "https://localhost:8008/liveness"]
      #   mTLS also add:  "--cert", "/etc/pg-ha/tls/client.crt", "--key", "/etc/pg-ha/tls/client.key"
      test: ["CMD", "curl", "-sf", "http://localhost:8008/liveness"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 30s

  node2:
    image: pg-ha:latest
    container_name: pg-ha-node2
    hostname: node2
    restart: unless-stopped
    environment:
      PG_HA_NAME: node2
      PG_HA_SCOPE: prod-cluster
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
      PGDATA: /var/lib/postgresql/data
      PG_HA_RAFT_SELF: "node2:2380"
      PG_HA_RAFT_PARTNERS: "node1:2380,node3:2380"
      PG_HA_RAFT_TLS_CERT: /etc/pg-ha/tls/server.crt   # member certificate; the client certificate defaults to it
      PG_HA_RAFT_TLS_KEY: /etc/pg-ha/tls/server.key
      PG_HA_RAFT_TLS_CA_CERT: /etc/pg-ha/tls/ca.crt
      PG_HA_LOG_FORMAT: json
      RUST_LOG: "pg_ha=info"
    ports:
      - "5433:5432"
      - "8009:8008"
      - "6434:6432"
      - "6435:6433"
    volumes:
      - node2-raft:/var/lib/pg-ha/raft
      - node2-pgdata:/var/lib/postgresql/data
      - ./docker/tls:/etc/pg-ha/tls:ro
    networks:
      - pg-ha-net
    healthcheck:
      # Plaintext REST (this example does not set PG_HA_RESTAPI_TLS_CERT). After restapi.tls is enabled this must be changed to HTTPS,
      # or the health check fails forever:
      #   server-auth TLS: ["CMD", "curl", "-sf", "--cacert", "/etc/pg-ha/tls/ca.crt", "https://localhost:8008/liveness"]
      #   mTLS also add:  "--cert", "/etc/pg-ha/tls/client.crt", "--key", "/etc/pg-ha/tls/client.key"
      test: ["CMD", "curl", "-sf", "http://localhost:8008/liveness"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 30s

  node3:
    image: pg-ha:latest
    container_name: pg-ha-node3
    hostname: node3
    restart: unless-stopped
    environment:
      PG_HA_NAME: node3
      PG_HA_SCOPE: prod-cluster
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
      PGDATA: /var/lib/postgresql/data
      PG_HA_RAFT_SELF: "node3:2380"
      PG_HA_RAFT_PARTNERS: "node1:2380,node2:2380"
      PG_HA_RAFT_TLS_CERT: /etc/pg-ha/tls/server.crt   # member certificate; the client certificate defaults to it
      PG_HA_RAFT_TLS_KEY: /etc/pg-ha/tls/server.key
      PG_HA_RAFT_TLS_CA_CERT: /etc/pg-ha/tls/ca.crt
      PG_HA_LOG_FORMAT: json
      RUST_LOG: "pg_ha=info"
    ports:
      - "5434:5432"
      - "8010:8008"
      - "6436:6432"
      - "6437:6433"
    volumes:
      - node3-raft:/var/lib/pg-ha/raft
      - node3-pgdata:/var/lib/postgresql/data
      - ./docker/tls:/etc/pg-ha/tls:ro
    networks:
      - pg-ha-net
    healthcheck:
      # Plaintext REST (this example does not set PG_HA_RESTAPI_TLS_CERT). After restapi.tls is enabled this must be changed to HTTPS,
      # or the health check fails forever:
      #   server-auth TLS: ["CMD", "curl", "-sf", "--cacert", "/etc/pg-ha/tls/ca.crt", "https://localhost:8008/liveness"]
      #   mTLS also add:  "--cert", "/etc/pg-ha/tls/client.crt", "--key", "/etc/pg-ha/tls/client.key"
      test: ["CMD", "curl", "-sf", "http://localhost:8008/liveness"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 30s

networks:
  pg-ha-net:
    driver: bridge

volumes:
  node1-raft:
  node2-raft:
  node3-raft:
  node1-pgdata:
  node2-pgdata:
  node3-pgdata:
```

Run `make build` before building the image (`docker/Dockerfile` copies Linux binaries that are already built, and does not compile Rust during the image build):

```bash
export PG_PASSWORD="your-secure-password"
make build
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d
curl -s http://localhost:8008/cluster | jq .
# If restapi.tls is enabled, use this instead:
# curl -s --cacert docker/tls/ca.crt https://localhost:8008/cluster | jq .
# (for mTLS also add --cert docker/tls/client.crt --key docker/tls/client.key)
```

For the development cluster, run `make up` directly and do not use this example. `make down` deletes the Raft and PGDATA volumes.

---

## Ports

| Port | Protocol | Direction | Purpose |
|------|----------|-----------|---------|
| 5432 | TCP | Inbound | PostgreSQL client connections (direct) |
| 8008 | HTTP or HTTPS | Inbound | REST API (health checks and `GET /wal`, which quorum votes read, require no authentication; management endpoints require Basic Auth when `restapi.username` is set). `restapi.tls` can be set to enable HTTPS / mTLS; when unset, the API is plaintext HTTP |
| 2380 | HTTPS (mTLS) | Between nodes | Raft RPC. Always mTLS; only certificates whose SAN matches a cluster member host are accepted. Falls back to plaintext HTTP only with `allow_insecure_raft: true` (development only) |
| 6432 | TCP | Inbound | Proxy RW (routes to the Primary) |
| 6433 | TCP | Inbound | Proxy RO (load-balances to Replicas) |

**Suggested firewall rules:**

```bash
# Client access (from the application tier to pg-ha)
ufw allow 6432/tcp   # Proxy RW
ufw allow 6433/tcp   # Proxy RO
ufw allow 8008/tcp   # API (management network only)

# Intra-cluster communication (between nodes)
ufw allow from 10.0.1.0/24 to any port 2380  # Raft RPC (mTLS; never open to the internet)
ufw allow from 10.0.1.0/24 to any port 5432  # PG replication
ufw allow from 10.0.1.0/24 to any port 8008  # Proxy health checks
```

---

## Security Notes

**Never expose Raft port 2380 without mTLS.** A peer that can reach 2380 without authentication can write any DCS key (including the leader lock), inject entries into the Raft log, replace the whole state with a snapshot, and disrupt elections. That is full control of the cluster, including who is primary. Raft RPC therefore now enforces mTLS and checks member identity (see "TLS Configuration"); there is no optional one-way TLS and no plaintext default any more.

- Use a firewall to allow 2380 only between cluster nodes, even with mTLS.
- Use one certificate per node, a separate CA for Raft, and mode 600 on node private keys.
- `allow_insecure_raft: true` is for development and testing only; seeing it in a production config is a defect.
- Upgrade note: this is a breaking change. Old configs need `raft.tls` (with `ca_cert`) and member certificates, and must drop `require_client_cert`. Upgrade all nodes together (see the release note).
