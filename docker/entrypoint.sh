#!/bin/bash
set -e

# pg-ha entrypoint for Docker
# Generates config from environment variables and starts pg-ha

NODE_NAME=${PG_HA_NAME:-$(hostname)}
SCOPE=${PG_HA_SCOPE:-pg-ha-cluster}

# Auto-detect PostgreSQL version and paths from the official postgres Docker image
# The image sets PG_MAJOR and places binaries under /usr/lib/postgresql/<ver>/bin
PG_MAJOR=${PG_MAJOR:-$(ls /usr/lib/postgresql/ 2>/dev/null | sort -V | tail -1)}
PG_BIN=${PG_HA_BIN_DIR:-/usr/lib/postgresql/${PG_MAJOR}/bin}
PG_DATA=${PGDATA:-/var/lib/postgresql/${PG_MAJOR}/data}
PG_PORT=${PG_PORT:-5432}
PG_USER=${POSTGRES_USER:-postgres}
PG_PASSWORD=${POSTGRES_PASSWORD:-secret}
API_PORT=${PG_HA_API_PORT:-8008}
RAFT_PORT=${PG_HA_RAFT_PORT:-2380}
PROXY_RW_PORT=${PG_HA_PROXY_RW_PORT:-6432}
PROXY_RO_PORT=${PG_HA_PROXY_RO_PORT:-6433}
RAFT_SELF=${PG_HA_RAFT_SELF:-$(hostname):${RAFT_PORT}}
RAFT_PARTNERS=${PG_HA_RAFT_PARTNERS:-}

# Lease timing. Defaults keep the historical dev values; overridable via env so
# tests can widen the window. Constraint: ttl >= loop_wait + retry_timeout +
# watchdog.safety_margin (see config.rs).
LOOP_WAIT=${PG_HA_LOOP_WAIT:-5}
TTL=${PG_HA_TTL:-15}
RETRY_TIMEOUT=${PG_HA_RETRY_TIMEOUT:-5}

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

# Watchdog device (when passed in with --device): pg-ha runs as postgres and
# opens it only when it becomes primary.
WATCHDOG_DEVICE=${PG_HA_WATCHDOG_DEVICE:-/dev/watchdog}
if [ "${PG_HA_WATCHDOG_MODE:-off}" != "off" ] && [ -c "$WATCHDOG_DEVICE" ]; then
    chown postgres:postgres "$WATCHDOG_DEVICE"
fi

# Ensure Raft data directory exists with correct permissions
RAFT_DATA_DIR="/var/lib/pg-ha/raft"
mkdir -p "$RAFT_DATA_DIR"
chown postgres:postgres "$RAFT_DATA_DIR"

# Node tags (optional). Nothing is rendered unless a PG_HA_TAG_* variable is
# set, so the generated config is unchanged by default. Keys match the `tags:`
# section of the config (see config.rs `Tags`):
#   booleans (true|false): PG_HA_TAG_NOFAILOVER, PG_HA_TAG_NOLOADBALANCE,
#     PG_HA_TAG_NOCLONE, PG_HA_TAG_NOSYNC, PG_HA_TAG_NOSTREAM, PG_HA_TAG_CLONEFROM
#   integers: PG_HA_TAG_FAILOVER_PRIORITY (0 = never promote), PG_HA_TAG_SYNC_PRIORITY
#   member name: PG_HA_TAG_REPLICATEFROM
TAGS_YAML=""
tag_bool() { # <env value> <yaml key>
    [ -z "$1" ] && return 0
    case "$1" in
        true|false) TAGS_YAML="${TAGS_YAML}  $2: $1\n" ;;
        *) echo "pg-ha entrypoint: tag '$2' must be true or false, got '$1'" >&2; exit 1 ;;
    esac
}
tag_int() { # <env value> <yaml key>
    [ -z "$1" ] && return 0
    case "$1" in
        *[!0-9]*) echo "pg-ha entrypoint: tag '$2' must be a non-negative integer, got '$1'" >&2; exit 1 ;;
        *) TAGS_YAML="${TAGS_YAML}  $2: $1\n" ;;
    esac
}
tag_bool "$PG_HA_TAG_NOFAILOVER" nofailover
tag_bool "$PG_HA_TAG_NOLOADBALANCE" noloadbalance
tag_bool "$PG_HA_TAG_NOCLONE" noclone
tag_bool "$PG_HA_TAG_NOSYNC" nosync
tag_bool "$PG_HA_TAG_NOSTREAM" nostream
tag_bool "$PG_HA_TAG_CLONEFROM" clonefrom
tag_int "$PG_HA_TAG_FAILOVER_PRIORITY" failover_priority
tag_int "$PG_HA_TAG_SYNC_PRIORITY" sync_priority
if [ -n "$PG_HA_TAG_REPLICATEFROM" ]; then
    case "$PG_HA_TAG_REPLICATEFROM" in
        *[!A-Za-z0-9._-]*) echo "pg-ha entrypoint: tag 'replicatefrom' must be a member name, got '$PG_HA_TAG_REPLICATEFROM'" >&2; exit 1 ;;
        *) TAGS_YAML="${TAGS_YAML}  replicatefrom: \"${PG_HA_TAG_REPLICATEFROM}\"\n" ;;
    esac
fi

# Generate pg-ha config
cat > /etc/pg-ha.yml << EOF
name: ${NODE_NAME}
scope: ${SCOPE}
namespace: service
loop_wait: ${LOOP_WAIT}
ttl: ${TTL}
retry_timeout: ${RETRY_TIMEOUT}

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
    wal_keep_size: "512MB"  

restapi:
  listen: 0.0.0.0
  port: ${API_PORT}
  username: admin
  password: secret
$(if [ -n "$PG_HA_RESTAPI_TLS_CERT" ]; then
cat << TLSEOF
  tls:
    cert: ${PG_HA_RESTAPI_TLS_CERT}
    key: ${PG_HA_RESTAPI_TLS_KEY}
    ca_cert: ${PG_HA_RESTAPI_TLS_CA_CERT}
    require_client_cert: ${PG_HA_RESTAPI_TLS_REQUIRE_CLIENT_CERT:-false}
TLSEOF
fi)

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
    require_client_cert: ${PG_HA_RAFT_TLS_REQUIRE_CLIENT_CERT:-false}
TLSEOF
fi)

proxy:
  rw_listen: 0.0.0.0
  rw_port: ${PROXY_RW_PORT}
  ro_listen: 0.0.0.0
  ro_port: ${PROXY_RO_PORT}

watchdog:
  mode: "${PG_HA_WATCHDOG_MODE:-off}"

bootstrap:
  initdb:
    - data-checksums
    - encoding: UTF8
  dcs:
    loop_wait: ${LOOP_WAIT}
    ttl: ${TTL}
    maximum_lag_on_failover: 1048576
EOF

# Append the tags block only when a tag was requested.
if [ -n "$TAGS_YAML" ]; then
    printf '\ntags:\n%b' "$TAGS_YAML" >> /etc/pg-ha.yml
fi

echo "=== pg-ha config ==="
sed 's/password: .*/password: ***/' /etc/pg-ha.yml
echo "===================="

# Run pg-ha as postgres user (needs access to PG data dir)
exec gosu postgres /usr/local/bin/pg-ha /etc/pg-ha.yml
