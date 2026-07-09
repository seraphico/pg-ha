#!/bin/bash
# tests/e2e/test_switchover.sh — Verify manual switchover via REST API
#
# Tests:
#   - POST /switchover → leader changes to specified candidate
#   - Old leader becomes replica
#   - Cluster remains healthy after switchover

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

log_section "TEST: Manual Switchover"

# --- Test: Switchover to specific candidate ---
test_switchover_to_candidate() {
    log_info "Identifying current primary and a replica candidate..."

    local primary_url
    primary_url=$(get_primary)
    assert_not_empty "$primary_url" "Primary exists before switchover"

    local primary_name
    primary_name=$(get_node_name "$primary_url")
    log_info "Current primary: $primary_name ($primary_url)"

    # Pick the first replica as candidate
    local candidate_url
    candidate_url=$(get_replicas | head -n 1)
    assert_not_empty "$candidate_url" "Replica candidate exists"

    local candidate_name
    candidate_name=$(get_node_name "$candidate_url")
    log_info "Switchover candidate: $candidate_name ($candidate_url)"

    # Perform switchover
    log_info "Posting switchover request..."
    local response
    response=$(http_post "$primary_url/switchover" "{\"candidate\": \"$candidate_name\"}")
    log_info "Switchover response: $response"

    # Wait for the candidate to become primary
    if wait_for_role "$candidate_url" "primary" 60; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Candidate ($candidate_name) became primary after switchover"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Candidate ($candidate_name) did not become primary"
    fi
}

# --- Test: Old leader becomes replica ---
test_old_leader_demotes() {
    log_info "Checking old leader demoted to replica..."

    # Find nodes that are NOT the current primary
    local new_primary
    new_primary=$(get_primary)

    local non_primary_count=0
    for node in "${ALL_NODES[@]}"; do
        if [ "$node" = "$new_primary" ]; then
            continue
        fi
        local role
        role=$(get_node_role "$node")
        if [ "$role" = "replica" ]; then
            non_primary_count=$((non_primary_count + 1))
        fi
    done

    assert_eq "2" "$non_primary_count" "Two nodes are replicas after switchover"
}

# --- Test: Cluster health after switchover ---
test_cluster_healthy_after_switchover() {
    log_info "Checking cluster health after switchover..."

    # All nodes should be running
    for node in "${ALL_NODES[@]}"; do
        local state
        state=$(get_node_state "$node")
        local container
        container=$(node_url_to_container "$node")
        assert_eq "running" "$state" "$container is running after switchover"
    done

    # Primary should be writable
    local primary_url
    primary_url=$(get_primary)
    local pg_port
    pg_port=$(node_url_to_pg_port "$primary_url")
    local in_recovery
    in_recovery=$(pg_is_in_recovery "$pg_port")
    assert_eq "f" "$in_recovery" "New primary is writable after switchover"
}

# --- Test: Switchover without candidate (auto-select) ---
test_switchover_auto_candidate() {
    log_info "Testing switchover without specifying candidate..."

    local primary_url
    primary_url=$(get_primary)
    local primary_name
    primary_name=$(get_node_name "$primary_url")
    log_info "Current primary: $primary_name"

    # Switchover with leader specified but no candidate
    local response
    response=$(http_post "$primary_url/switchover" "{\"leader\": \"$primary_name\"}")
    log_info "Switchover response: $response"

    # Wait for a different primary
    sleep 5
    local new_primary
    new_primary=$(wait_for_new_primary "$primary_url" 60) || true

    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$new_primary" ] && [ "$new_primary" != "$primary_url" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Auto-candidate switchover succeeded"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Auto-candidate switchover did not change primary"
    fi
}

# --- Run tests ---
test_switchover_to_candidate
sleep 5
test_old_leader_demotes
test_cluster_healthy_after_switchover

# Wait before next test
sleep 10
wait_for_cluster_ready 60 || log_warn "Cluster not ready for auto-candidate test"
test_switchover_auto_candidate

print_results
