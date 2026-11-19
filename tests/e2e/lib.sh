#!/bin/bash
# tests/e2e/lib.sh — Shared helpers for pg-ha end-to-end tests

# Keep undefined-variable detection, but turn OFF errexit and pipefail. The
# test scripts rely on assertions ACCUMULATING into TESTS_FAILED and the script
# running all the way to print_results (which exits with the failure count). A
# bare failing assertion or a SIGPIPE from `producer | head` must NOT abort the
# script before the summary — that was the old bug (scripts died at the first
# failure with exit 1 or 141 instead of reporting the real failure count).
set +e +o pipefail
set -u

# --- Color output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- REST scheme / curl options (REST_TLS=off|tls|mtls, default off) ---
source "$(dirname "${BASH_SOURCE[0]}")/rest_env.sh"
# --- Node configuration ---
NODE1_API="$SCHEME://localhost:8008"
NODE2_API="$SCHEME://localhost:8009"
NODE3_API="$SCHEME://localhost:8010"
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
    actual_code=$(curl "${CURL_OPTS[@]}" -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    assert_eq "$expected_code" "$actual_code" "$msg"
}

# --- HTTP helpers ---
# GETs carry REST_AUTH too: health endpoints are open, but GET /config requires
# Basic Auth (returns 401 without it). Sending credentials on every GET is
# harmless for the open endpoints and fixes the /config reads.
http_get() {
    local url="$1"
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 5 "$url" 2>/dev/null || echo ""
}

http_get_status() {
    local url="$1"
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null || echo "000"
}

http_post() {
    local url="$1"
    local data="${2:-}"
    if [ -n "$data" ]; then
        curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -X POST -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
    else
        curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -X POST "$url" 2>/dev/null || echo ""
    fi
}

http_patch() {
    local url="$1"
    local data="$2"
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -X PATCH -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
}

http_put() {
    local url="$1"
    local data="$2"
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -X PUT -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
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
    # /patroni returns the role in Debug form (uppercase "Primary"/"Replica").
    # Lowercase it so callers can compare against "primary"/"replica".
    local node_url="$1"
    local resp
    resp=$(http_get "$node_url/patroni")
    echo "$resp" | jq -r '.role // empty' 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo ""
}

get_node_state() {
    # /patroni returns the state in Debug form (uppercase "Running"); lowercase
    # it to match the expectations in the test scripts.
    local node_url="$1"
    local resp
    resp=$(http_get "$node_url/patroni")
    echo "$resp" | jq -r '.state // empty' 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo ""
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
            # A primary is not enough: right after bootstrap/failover the two
            # replicas can still be Uninitialized/catching up while a primary
            # already answers. Require a settled cluster — exactly one primary
            # AND two replicas answering /replica=200 — so suites that run after
            # this barrier (quick_test's "2 replicas", failover_ttl's
            # get_replicas, ha_full's replicated reads) don't assert too early.
            local primary replica_count=0
            primary=$(get_primary) || primary=""  # no primary yet: keep polling (set -e)
            local rnode
            for rnode in "${ALL_NODES[@]}"; do
                if [ "$(http_get_status "$rnode/replica")" = "200" ]; then
                    replica_count=$((replica_count + 1))
                fi
            done
            if [ -n "$primary" ] && [ "$replica_count" -eq 2 ]; then
                local elapsed=$((SECONDS - start))
                log_info "Cluster ready in ${elapsed}s (primary: $primary, replicas: $replica_count)"
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
    # Judge role by the case-free health endpoints (/primary → 200 when primary,
    # /replica → 200 when replica) rather than the role string, which the
    # product returns capitalised. expected_role is "primary" or "replica".
    local node_url="$1"
    local expected_role="$2"
    local timeout="${3:-60}"
    local start=$SECONDS

    local probe
    case "$expected_role" in
        primary|master) probe="$node_url/primary" ;;
        replica)        probe="$node_url/replica" ;;
        *)              probe="$node_url/$expected_role" ;;
    esac

    log_info "Waiting for $(node_url_to_container "$node_url") to become '$expected_role' (timeout: ${timeout}s)..."

    while true; do
        local status
        status=$(http_get_status "$probe")
        if [ "$status" = "200" ]; then
            local elapsed=$((SECONDS - start))
            log_info "Node became '$expected_role' in ${elapsed}s"
            return 0
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            local role
            role=$(get_node_role "$node_url")
            log_fail "Node did not become '$expected_role' after ${timeout}s (current role: '$role')"
            return 1
        fi

        sleep 1
    done
}

# wait_for_primary / wait_for_new_primary are used as `x=$(wait_for_...)` and
# then tested with `[ -n "$x" ]`, so stdout carries ONLY the primary URL, and
# only on success. Every log line goes to stderr. (Before, the log lines were
# on stdout: a timeout left "[FAIL] No new primary..." in the variable, the
# `-n` check passed, and two false passes were recorded.)
wait_for_primary() {
    # Wait for any node to become primary
    local timeout="${1:-60}"
    local start=$SECONDS

    log_info "Waiting for a primary to appear (timeout: ${timeout}s)..." >&2

    while true; do
        local primary
        primary=$(get_primary) || primary=""  # no primary yet: keep polling (set -e)
        if [ -n "$primary" ]; then
            local elapsed=$((SECONDS - start))
            log_info "Primary found at $primary in ${elapsed}s" >&2
            echo "$primary"
            return 0
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            log_fail "No primary found after ${timeout}s" >&2
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

    log_info "Waiting for a new primary (not $old_primary, timeout: ${timeout}s)..." >&2

    while true; do
        local primary
        primary=$(get_primary) || primary=""  # no primary yet: keep polling (set -e)
        if [ -n "$primary" ] && [ "$primary" != "$old_primary" ]; then
            local elapsed=$((SECONDS - start))
            log_info "New primary at $primary in ${elapsed}s" >&2
            echo "$primary"
            return 0
        fi

        local elapsed=$((SECONDS - start))
        if [ "$elapsed" -ge "$timeout" ]; then
            log_fail "No new primary after ${timeout}s" >&2
            return 1
        fi

        sleep 1
    done
}

# --- Synchronous mode / quorum helpers ---
# All of these log to stderr (they may run inside $(...) captures).

get_sync_mode() {
    # async | sync | quorum, as stored in the dynamic config; empty when no
    # primary answers. An unset key is async (the default).
    local primary
    primary=$(get_primary) || true
    [ -n "$primary" ] || { echo ""; return 1; }
    http_get "$primary/config" | jq -r '.synchronous_mode // "async"' 2>/dev/null
}

wait_quorum_settled() {
    # Wait until q is relaxed to |V| - n (SB-13). After a voter is added
    # (quorum just enabled, or a node rejoined) /sync holds q = |V| for about
    # 20 s, nobody can win a vote then, and a kill or switchover inside that
    # window leaves the cluster without a primary. Signal: the leader's
    # GET /cluster, sync.settled == true (and kind quorum). Because "settled"
    # is also true briefly before a rejoining node is added to V, also wait
    # until V has at least `expected_voters` names (default: nodes answering
    # /patroni, minus the primary). Usage: wait_quorum_settled [timeout] [expected_voters]
    local timeout="${1:-120}"
    local expected="${2:-}"
    local start=$SECONDS
    log_info "Waiting for quorum to settle (leader /cluster sync.settled, timeout: ${timeout}s)..." >&2
    while true; do
        local primary
        primary=$(get_primary) || true
        if [ -n "$primary" ]; then
            local want="$expected"
            if [ -z "$want" ]; then
                local up=0 node
                for node in "${ALL_NODES[@]}"; do
                    [ "$(http_get_status "$node/patroni")" = "200" ] && up=$((up + 1))
                done
                want=$((up - 1))
            fi
            local state
            state=$(http_get "$primary/cluster" | jq -r --argjson want "$want" \
                'if (.sync.kind == "quorum" and .sync.settled == true and ((.sync.voters | length) >= $want)) then "settled" else "waiting" end' 2>/dev/null || true)
            if [ "$state" = "settled" ]; then
                log_info "Quorum settled in $((SECONDS - start))s" >&2
                return 0
            fi
        fi
        if [ "$((SECONDS - start))" -ge "$timeout" ]; then
            log_fail "Quorum not settled after ${timeout}s (sync: $(http_get "${primary:-$NODE1_API}/cluster" | jq -c .sync 2>/dev/null))" >&2
            return 1
        fi
        sleep 1
    done
}

wait_quorum_settled_if_quorum() {
    # No-op unless synchronous_mode is quorum. Never fails the caller: a
    # timeout is logged, and the test that follows shows the real symptom.
    [ "$(get_sync_mode)" = "quorum" ] || return 0
    wait_quorum_settled "${1:-120}" || log_warn "Quorum did not settle; continuing" >&2
    return 0
}

set_sync_mode() {
    # PATCH synchronous_mode (async|sync|quorum). For quorum, wait until q is
    # relaxed before returning. Usage: set_sync_mode <mode>
    local mode="$1"
    local primary code
    primary=$(wait_for_primary 60) || return 1
    code=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -o /dev/null -w '%{http_code}' \
        -X PATCH -H "Content-Type: application/json" \
        -d "{\"synchronous_mode\":\"$mode\"}" "$primary/config" 2>/dev/null || echo 000)
    if [ "$code" != "200" ]; then
        log_fail "PATCH /config synchronous_mode=$mode returned HTTP $code" >&2
        return 1
    fi
    log_info "synchronous_mode set to $mode" >&2
    if [ "$mode" = "quorum" ]; then
        wait_quorum_settled 120
    fi
}

wait_for_switchover_candidate() {
    # Wait until a node can take a switchover. async: a healthy replica
    # (/replica 200). sync and quorum: the node must also be in /sync, and
    # GET /sync answers 200 only for a healthy replica that is on that list
    # (/replica 200 is true earlier, from a stale published state, and the
    # switchover is then refused with 409 'not a synchronous standby').
    # Usage: wait_for_switchover_candidate <node_url> [timeout]
    local node_url="$1"
    local timeout="${2:-90}"
    local mode
    mode=$(get_sync_mode)
    if [ "$mode" != "sync" ] && [ "$mode" != "quorum" ]; then
        wait_for_role "$node_url" "replica" "$timeout"
        return
    fi
    local start=$SECONDS
    log_info "Waiting for $(node_url_to_container "$node_url") to answer /sync 200 (mode: $mode, timeout: ${timeout}s)..." >&2
    while [ "$((SECONDS - start))" -lt "$timeout" ]; do
        if [ "$(http_get_status "$node_url/sync")" = "200" ]; then
            log_info "Node answers /sync 200 after $((SECONDS - start))s" >&2
            return 0
        fi
        sleep 1
    done
    log_fail "Node did not answer /sync 200 after ${timeout}s" >&2
    return 1
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

    # Under quorum a kill inside the q = |V| window leaves no primary.
    wait_quorum_settled_if_quorum
    primary_url=$(get_primary)
    if [ -z "$primary_url" ]; then
        echo "-1"
        return 1
    fi
    container=$(node_url_to_container "$primary_url")

    local start=$SECONDS

    # Kill the primary
    docker kill "$container" > /dev/null 2>&1

    # Wait for new primary
    local new_primary
    new_primary=$(wait_for_new_primary "$primary_url" 60)

    # Always restart the node we killed so the cluster is left whole. Otherwise
    # this measurement permanently removes a node, and every suite that runs
    # after it in run_tests.sh fails on "2/3 nodes responding".
    docker start "$container" > /dev/null 2>&1 || true

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
