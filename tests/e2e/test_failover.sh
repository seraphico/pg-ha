#!/bin/bash
# tests/e2e/test_failover.sh — Verify automatic failover on primary failure
#
# Tests:
#   - Kill primary container → new primary elected within TTL
#   - New primary holds leader lock
#   - Old primary (if restarted) becomes replica

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

log_section "TEST: Auto Failover"

# --- Test: Primary failure triggers failover ---
test_failover_on_kill() {
    log_info "Identifying current primary..."
    local old_primary
    old_primary=$(get_primary)
    assert_not_empty "$old_primary" "Primary exists before test"

    local old_container
    old_container=$(node_url_to_container "$old_primary")
    log_info "Current primary: $old_container ($old_primary)"

    # Kill the primary
    log_info "Killing primary container: $old_container"
    docker kill "$old_container" > /dev/null 2>&1

    # Wait for new primary to be elected
    sleep 2
    local new_primary
    new_primary=$(wait_for_new_primary "$old_primary" 30) || true

    assert_not_empty "$new_primary" "New primary elected after killing old primary"

    if [ -n "$new_primary" ]; then
        local new_container
        new_container=$(node_url_to_container "$new_primary")
        log_info "New primary: $new_container ($new_primary)"

        # Verify it's truly a different node
        TESTS_RUN=$((TESTS_RUN + 1))
        if [ "$new_primary" != "$old_primary" ]; then
            TESTS_PASSED=$((TESTS_PASSED + 1))
            log_pass "New primary is different from old primary"
        else
            TESTS_FAILED=$((TESTS_FAILED + 1))
            log_fail "New primary is the same as old primary"
        fi
    fi
}

# --- Test: New primary holds leader lock ---
test_new_primary_holds_lock() {
    log_info "Checking new primary holds leader lock..."
    local primary
    primary=$(get_primary)
    if [ -z "$primary" ]; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "No primary available to check leader lock"
        return
    fi

    # The primary should respond 200 on /primary
    assert_http_status "$primary/primary" "200" "New primary responds 200 on /primary"

    # The primary should be writable
    local pg_port
    pg_port=$(node_url_to_pg_port "$primary")
    local in_recovery
    in_recovery=$(pg_is_in_recovery "$pg_port")
    assert_eq "f" "$in_recovery" "New primary is writable"
}

# --- Test: Old primary rejoins as replica ---
test_old_primary_rejoins() {
    log_info "Restarting old primary container..."

    # Find which container is stopped
    local stopped_container=""
    for container in "${CONTAINERS[@]}"; do
        local state
        state=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || echo "false")
        if [ "$state" = "false" ]; then
            stopped_container="$container"
            break
        fi
    done

    if [ -z "$stopped_container" ]; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "No stopped container found to restart"
        return
    fi

    log_info "Restarting $stopped_container..."
    docker start "$stopped_container" > /dev/null 2>&1

    # Determine the API URL for this container
    local node_url=""
    case "$stopped_container" in
        pg-ha-node1) node_url="$NODE1_API" ;;
        pg-ha-node2) node_url="$NODE2_API" ;;
        pg-ha-node3) node_url="$NODE3_API" ;;
    esac

    # Wait for it to become a replica
    sleep 5
    if wait_for_role "$node_url" "replica" 60; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Restarted node ($stopped_container) rejoined as replica"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Restarted node ($stopped_container) did not become replica"
    fi
}

# --- Test: Failover time measurement ---
test_failover_time() {
    log_info "Measuring failover time..."

    # Ensure cluster is healthy first
    wait_for_cluster_ready 60 || {
        log_warn "Cluster not fully ready, skipping failover time measurement"
        return
    }

    local failover_seconds
    failover_seconds=$(measure_failover_time)

    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$failover_seconds" -gt 0 ] && [ "$failover_seconds" -le 30 ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Failover completed in ${failover_seconds}s (within 30s budget)"
    elif [ "$failover_seconds" -gt 30 ]; then
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Failover took ${failover_seconds}s (exceeds 30s budget)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Failover measurement failed"
    fi
}

# --- Run tests ---
test_failover_on_kill
test_new_primary_holds_lock
test_old_primary_rejoins

# Restore cluster before measuring failover time again
log_info "Waiting for cluster to stabilize before failover time test..."
sleep 10
wait_for_cluster_ready 60 || log_warn "Cluster not fully ready for timing test"
test_failover_time

print_results
