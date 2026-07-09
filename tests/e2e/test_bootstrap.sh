#!/bin/bash
# tests/e2e/test_bootstrap.sh — Verify cluster bootstraps correctly
#
# Tests:
#   - Cluster starts with exactly one primary and two replicas
#   - All nodes report Running state
#   - PostgreSQL is accepting connections on all nodes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

log_section "TEST: Bootstrap"

# --- Test: Exactly one primary ---
test_one_primary() {
    log_info "Checking for exactly one primary..."
    local primary_count=0
    for node in "${ALL_NODES[@]}"; do
        local role
        role=$(get_node_role "$node")
        if [ "$role" = "primary" ] || [ "$role" = "master" ]; then
            primary_count=$((primary_count + 1))
        fi
    done
    assert_eq "1" "$primary_count" "Cluster has exactly one primary"
}

# --- Test: Exactly two replicas ---
test_two_replicas() {
    log_info "Checking for exactly two replicas..."
    local replica_count=0
    for node in "${ALL_NODES[@]}"; do
        local role
        role=$(get_node_role "$node")
        if [ "$role" = "replica" ]; then
            replica_count=$((replica_count + 1))
        fi
    done
    assert_eq "2" "$replica_count" "Cluster has exactly two replicas"
}

# --- Test: All nodes Running ---
test_all_running() {
    log_info "Checking all nodes are in Running state..."
    for node in "${ALL_NODES[@]}"; do
        local state
        state=$(get_node_state "$node")
        local container
        container=$(node_url_to_container "$node")
        assert_eq "running" "$state" "$container is in Running state"
    done
}

# --- Test: PG accepting connections ---
test_pg_connections() {
    log_info "Checking PostgreSQL is accepting connections..."
    local ports=("$NODE1_PG_PORT" "$NODE2_PG_PORT" "$NODE3_PG_PORT")
    local names=("node1" "node2" "node3")
    for i in 0 1 2; do
        if pg_is_accepting_connections "${ports[$i]}"; then
            TESTS_RUN=$((TESTS_RUN + 1))
            TESTS_PASSED=$((TESTS_PASSED + 1))
            log_pass "PostgreSQL on ${names[$i]} (port ${ports[$i]}) accepts connections"
        else
            TESTS_RUN=$((TESTS_RUN + 1))
            TESTS_FAILED=$((TESTS_FAILED + 1))
            log_fail "PostgreSQL on ${names[$i]} (port ${ports[$i]}) not accepting connections"
        fi
    done
}

# --- Test: Primary is writable ---
test_primary_writable() {
    log_info "Checking primary is writable..."
    local primary_url
    primary_url=$(get_primary)
    local pg_port
    pg_port=$(node_url_to_pg_port "$primary_url")
    local in_recovery
    in_recovery=$(pg_is_in_recovery "$pg_port")
    assert_eq "f" "$in_recovery" "Primary is not in recovery (writable)"
}

# --- Test: Replicas are in recovery ---
test_replicas_readonly() {
    log_info "Checking replicas are in recovery..."
    local replicas
    replicas=$(get_replicas)
    while IFS= read -r node; do
        [ -z "$node" ] && continue
        local pg_port
        pg_port=$(node_url_to_pg_port "$node")
        local container
        container=$(node_url_to_container "$node")
        local in_recovery
        in_recovery=$(pg_is_in_recovery "$pg_port")
        assert_eq "t" "$in_recovery" "$container is in recovery (read-only)"
    done <<< "$replicas"
}

# --- Test: /cluster endpoint ---
test_cluster_endpoint() {
    log_info "Checking /cluster endpoint..."
    local primary_url
    primary_url=$(get_primary)
    local cluster_json
    cluster_json=$(http_get "$primary_url/cluster")
    assert_not_empty "$cluster_json" "/cluster endpoint returns data"

    local member_count
    member_count=$(echo "$cluster_json" | jq '.members | length' 2>/dev/null || echo "0")
    assert_eq "3" "$member_count" "/cluster shows 3 members"
}

# --- Run all tests ---
test_one_primary
test_two_replicas
test_all_running
test_pg_connections
test_primary_writable
test_replicas_readonly
test_cluster_endpoint

print_results
