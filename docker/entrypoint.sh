#!/bin/bash
set -e

# pg-ha entrypoint for Docker
# Generates config from environment variables and starts pg-ha

NODE_NAME=${PG_HA_NAME:-$(hostname)}
SCOPE=${PG_HA_SCOPE:-pg-ha-cluster}
PG_DATA=${PGDATA:-/var/lib/postgresql/data}
PG_BIN=/usr/lib/postgresql/16/bin
PG_PORT=${PG_PORT:-5432}
PG_USER=${POSTGRES_USER:-postgres}
PG_PASSWORD=${POSTGRES_PASSWORD:-secret}
API_PORT=${PG_HA_API_PORT:-8008}
RAFT_PORT=${PG_HA_RAFT_PORT:-2380}
PROXY_RW_PORT=${PG_HA_PROXY_RW_PORT:-6432}
PROXY_RO_PORT=${PG_HA_PROXY_RO_PORT:-6433}
RAFT_SELF=${PG_HA_RAFT_SELF:-$(hostname):${RAFT_PORT}}
RAFT_PARTNERS=${PG_HA_RAFT_PARTNERS:-}

# Build partner_addrs YAML list
PARTNER_YAML=""
IFS=',' read -ra PARTNERS <<< "$RAFT_PARTNERS"
for p in "${PARTNERS[@]}"; do
    if [ -n "$p" ]; then
        PARTNER_YAML="${PARTNER_YAML}    - \"${p}\"\n"
    fi
done

# If no partners specified, use a dummy (single-node mode)
if [ -z "$PARTNER_YAML" ]; then
    PARTNER_YAML="    - \"127.0.0.1:9999\"\n"
fi

# Ensure data directory exists with correct permissions
mkdir -p "$PG_DATA"
chown postgres:postgres "$PG_DATA"
chmod 0700 "$PG_DATA"

# Ensure Raft data directory exists with correct permissions
RAFT_DATA_DIR="/var/lib/pg-ha/raft"
mkdir -p "$RAFT_DATA_DIR"
chown postgres:postgres "$RAFT_DATA_DIR"

# Generate pg-ha config
cat > /etc/pg-ha.yml << EOF
name: ${NODE_NAME}
scope: ${SCOPE}
namespace: service
loop_wait: 5
ttl: 15
retry_timeout: 5

postgresql:
  data_dir: ${PG_DATA}
  bin_dir: ${PG_BIN}
  listen: 0.0.0.0
  port: ${PG_PORT}
  superuser:
    username: ${PG_USER}
    password: ${PG_PASSWORD}
    dbname: postgres
  replication:
    username: ${PG_USER}
    password: ${PG_PASSWORD}
    dbname: postgres
  parameters:
    wal_level: replica
    max_wal_senders: "10"
    max_replication_slots: "10"
    hot_standby: "on"
    listen_addresses: "'*'"

restapi:
  listen: 0.0.0.0
  port: ${API_PORT}
  username: admin
  password: secret

raft:
  self_addr: "${RAFT_SELF}"
  data_dir: "${RAFT_DATA_DIR}"
  partner_addrs:
$(echo -e "$PARTNER_YAML")
$(if [ -n "$PG_HA_RAFT_TLS_CERT" ]; then
cat << TLSEOF
  tls:
    cert: ${PG_HA_RAFT_TLS_CERT}
    key: ${PG_HA_RAFT_TLS_KEY}
    ca_cert: ${PG_HA_RAFT_TLS_CA_CERT}
    client_cert: ${PG_HA_RAFT_TLS_CLIENT_CERT}
    client_key: ${PG_HA_RAFT_TLS_CLIENT_KEY}
    require_client_cert: false
TLSEOF
fi)

proxy:
  rw_listen: 0.0.0.0
  rw_port: ${PROXY_RW_PORT}
  ro_listen: 0.0.0.0
  ro_port: ${PROXY_RO_PORT}

watchdog:
  mode: "off"

bootstrap:
  initdb:
    - data-checksums
    - encoding: UTF8
  dcs:
    loop_wait: 5
    ttl: 15
    maximum_lag_on_failover: 1048576
EOF

echo "=== pg-ha config ==="
cat /etc/pg-ha.yml
echo "===================="

# Run pg-ha as postgres user (needs access to PG data dir)
exec gosu postgres /usr/local/bin/pg-ha /etc/pg-ha.yml
