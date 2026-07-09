#!/bin/bash
# tests/e2e/lib.sh — Shared helpers for pg-ha end-to-end tests

set -euo pipefail

# --- Color output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Node configuration ---
NODE1_API="http://localhost:8008"
NODE2_API="http://localhost:8009"
NODE3_API="http://localhost:8010"
ALL_NODES=("$NODE1_API" "$NODE2_API" "$NODE3_API")

NODE1_PG_PORT=5432
NODE2_PG_PORT=5433
NODE3_PG_PORT=5434

NODE1_PROXY_RW=6432
NODE1_PROXY_RO=6433
NODE2_PROXY_RW=6434
NODE2_PROXY_RO=6435
NODE3_PROXY_RW=6436
NODE3_PROXY_RO=6437

CONTAINERS=("pg-ha-node1" "pg-ha-node2" "pg-ha-node3")

PG_PASSWORD="secret"
PG_USER="postgres"

# --- Test tracking ---
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
CURRENT_TEST=""

# --- Logging ---
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $*"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $*${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# --- Assertions ---
assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="${3:-assertion failed}"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$expected" = "$actual" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "$msg (expected='$expected')"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "$msg (expected='$expected', got='$actual')"
        return 1
    fi
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="${3:-assertion failed}"
    TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$haystack" | grep -q "$needle"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "$msg (contains '$needle')"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "$msg (does not contain '$needle')"
        return 1
    fi
}

assert_not_empty() {
    local value="$1"
    local msg="${2:-value should not be empty}"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$value" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "$msg"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "$msg (value is empty)"
        return 1
    fi
}

assert_http_status() {
    local url="$1"
    local expected_code="$2"
    local msg="${3:-HTTP status check}"
    local actual_code
    actual_code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    assert_eq "$expected_code" "$actual_code" "$msg"
}

# --- HTTP helpers ---
http_get() {
    local url="$1"
    curl -s --max-time 5 "$url" 2>/dev/null || echo ""
}

http_get_status() {
    local url="$1"
    curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null || echo "000"
}

http_post() {
    local url="$1"
    local data="${2:-}"
    if [ -n "$data" ]; then
        curl -s --max-time 10 -X POST -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
    else
        curl -s --max-time 10 -X POST "$url" 2>/dev/null || echo ""
    fi
}

http_patch() {
    local url="$1"
    local data="$2"
    curl -s --max-time 10 -X PATCH -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
}

http_put() {
    local url="$1"
    local data="$2"
    curl -s --max-time 10 -X PUT -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
}

# --- Cluster inspection ---
get_primary() {
    # Returns the API URL of the current primary node
    for node in "${ALL_NODES[@]}"; do
        local status
        status=$(http_get_status "$node/primary")
        if [ "$status" = "200" ]; then
            echo "$node"
            return 0
        fi
    done
    echo ""
    return 1
}

get_primary_name() {
    # Returns the name of the current primary node
    local primary_url
    primary_url=$(get_primary)
    if [ -n "$primary_url" ]; then
        local resp
        resp=$(http_get "$primary_url/patroni")
        echo "$resp" | jq -r '.patroni.name // .name // empty' 2>/dev/null || echo ""
    fi
}

get_replicas() {
    # Returns API URLs of replica nodes (one per line)
    for node in "${ALL_NODES[@]}"; do
        local status
        status=$(http_get_status "$node/replica")
        if [ "$status" = "200" ]; then
            echo "$node"
        fi
    done
}

get_node_role() {
    local node_url="$1"
    local resp
    resp=$(http_get "$node_url/patroni")
    echo "$resp" | jq -r '.role // empty' 2>/dev/null || echo ""
}

get_node_state() {
    local node_url="$1"
    local resp
    resp=$(http_get "$node_url/patroni")
    echo "$resp" | jq -r '.state // empty' 2>/dev/null || echo ""
}

get_node_name() {
    local node_url="$1"
    local resp
    resp=$(http_get "$node_url/patroni")
    echo "$resp" | jq -r '.patroni.name // .name // empty' 2>/dev/null || echo ""
}

node_url_to_container() {
    local url="$1"
    case "$url" in
        *8008*) echo "pg-ha-node1" ;;
        *8009*) echo "pg-ha-node2" ;;
        *8010*) echo "pg-ha-node3" ;;
        *) echo "" ;;
    esac
}

node_url_to_pg_port() {
    local url="$1"
    case "$url" in
        *8008*) echo "$NODE1_PG_PORT" ;;
        *8009*) echo "$NODE2_PG_PORT" ;;
        *8010*) echo "$NODE3_PG_PORT" ;;
        *) echo "" ;;
    esac
}

# --- Wait helpers ---
wait_for_cluster_ready() {
    local timeout="${1:-120}"
    local start=$SECONDS
    log_info "Waiting for cluster to be ready (timeout: ${timeout}s)..."

    while true; do
        local ready=0
        for node in "${ALL_NODES[@]}"; do
            local status
            status=$(http_get_status "$node/patroni")
            if [ "$status" = "200" ]; then
                ready=$((ready + 1))
            fi
        done

        if [ "$ready" -eq 3 ]; then
            # Also check that we have exactly one primary
            local primary
            primary=$(get_primary)
            if [ -n "$primary" ]; then
                local elapsed=$((SECONDS - start))
                log_info "Cluster ready in ${elapsed}s (primary: $primary)"
                return 0
            fi
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            log_fail "Cluster not ready after ${timeout}s (${ready}/3 nodes responding)"
            return 1
        fi

        sleep 2
    done
}

wait_for_role() {
    local node_url="$1"
    local expected_role="$2"
    local timeout="${3:-60}"
    local start=$SECONDS

    log_info "Waiting for $(node_url_to_container "$node_url") to become '$expected_role' (timeout: ${timeout}s)..."

    while true; do
        local role
        role=$(get_node_role "$node_url")
        if [ "$role" = "$expected_role" ]; then
            local elapsed=$((SECONDS - start))
            log_info "Node became '$expected_role' in ${elapsed}s"
            return 0
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            log_fail "Node did not become '$expected_role' after ${timeout}s (current: '$role')"
            return 1
        fi

        sleep 1
    done
}

wait_for_primary() {
    # Wait for any node to become primary
    local timeout="${1:-60}"
    local start=$SECONDS

    log_info "Waiting for a primary to appear (timeout: ${timeout}s)..."

    while true; do
        local primary
        primary=$(get_primary)
        if [ -n "$primary" ]; then
            local elapsed=$((SECONDS - start))
            log_info "Primary found at $primary in ${elapsed}s"
            echo "$primary"
            return 0
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            log_fail "No primary found after ${timeout}s"
            return 1
        fi

        sleep 1
    done
}

wait_for_new_primary() {
    # Wait for a primary different from the given one
    local old_primary="$1"
    local timeout="${2:-60}"
    local start=$SECONDS

    log_info "Waiting for a new primary (not $old_primary, timeout: ${timeout}s)..."

    while true; do
        local primary
        primary=$(get_primary)
        if [ -n "$primary" ] && [ "$primary" != "$old_primary" ]; then
            local elapsed=$((SECONDS - start))
            log_info "New primary at $primary in ${elapsed}s"
            echo "$primary"
            return 0
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            log_fail "No new primary after ${timeout}s"
            return 1
        fi

        sleep 1
    done
}

# --- Docker helpers ---
docker_pause() {
    local container="$1"
    log_info "Pausing container: $container"
    docker pause "$container"
}

docker_unpause() {
    local container="$1"
    log_info "Unpausing container: $container"
    docker unpause "$container"
}

docker_stop() {
    local container="$1"
    log_info "Stopping container: $container"
    docker stop "$container"
}

docker_start() {
    local container="$1"
    log_info "Starting container: $container"
    docker start "$container"
}

docker_kill() {
    local container="$1"
    log_info "Killing container: $container"
    docker kill "$container"
}

docker_restart() {
    local container="$1"
    log_info "Restarting container: $container"
    docker restart "$container"
}

# --- Network partition simulation ---
# Uses docker network disconnect/connect for true network isolation
docker_disconnect() {
    local container="$1"
    local network="${2:-pg-ha_pg-ha-net}"
    log_info "Disconnecting $container from network $network"
    docker network disconnect "$network" "$container"
}

docker_reconnect() {
    local container="$1"
    local network="${2:-pg-ha_pg-ha-net}"
    log_info "Reconnecting $container to network $network"
    docker network connect "$network" "$container"
}

# --- PostgreSQL helpers ---
pg_query() {
    local port="$1"
    local query="$2"
    PGPASSWORD="$PG_PASSWORD" psql -h localhost -p "$port" -U "$PG_USER" -d postgres -t -A -c "$query" 2>/dev/null || echo ""
}

pg_is_accepting_connections() {
    local port="$1"
    local result
    result=$(pg_query "$port" "SELECT 1" 2>/dev/null)
    [ "$result" = "1" ]
}

pg_is_in_recovery() {
    local port="$1"
    local result
    result=$(pg_query "$port" "SELECT pg_is_in_recovery()")
    echo "$result"
}

# --- Timing ---
measure_failover_time() {
    # Returns the time in seconds between primary kill and new primary election
    local primary_url
    primary_url=$(get_primary)
    if [ -z "$primary_url" ]; then
        echo "-1"
        return 1
    fi

    local container
    container=$(node_url_to_container "$primary_url")

    local start=$SECONDS

    # Kill the primary
    docker kill "$container" > /dev/null 2>&1

    # Wait for new primary
    local new_primary
    new_primary=$(wait_for_new_primary "$primary_url" 60)
    if [ -z "$new_primary" ]; then
        echo "-1"
        return 1
    fi

    local elapsed=$((SECONDS - start))
    echo "$elapsed"
}

# --- Test result summary ---
print_results() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  TEST RESULTS${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total:  ${TESTS_RUN}"
    echo -e "  ${GREEN}Passed: ${TESTS_PASSED}${NC}"
    echo -e "  ${RED}Failed: ${TESTS_FAILED}${NC}"
    echo ""

    if [ "$TESTS_FAILED" -eq 0 ]; then
        echo -e "  ${GREEN}✓ All tests passed!${NC}"
    else
        echo -e "  ${RED}✗ Some tests failed.${NC}"
    fi
    echo ""

    return "$TESTS_FAILED"
}
