#!/bin/bash
# tests/e2e/test_pause_mode.sh — Verify pause mode prevents automatic failover
#
# Tests:
#   - PATCH /config with pause=true → no automatic failover
#   - Kill primary while paused → no new election
#   - PATCH /config with pause=false → failover resumes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

log_section "TEST: Pause Mode"

# --- Test: Enable pause mode ---
test_enable_pause() {
    log_info "Enabling pause mode via PATCH /config..."

    local primary_url
    primary_url=$(get_primary)
    assert_not_empty "$primary_url" "Primary exists before enabling pause"

    local response
    response=$(http_patch "$primary_url/config" '{"pause": true}')
    log_info "Pause response: $response"

    # Verify pause is reflected in config
    sleep 3
    local config
    config=$(http_get "$primary_url/config")
    local pause_value
    pause_value=$(echo "$config" | jq -r '.pause // false' 2>/dev/null)

    assert_eq "true" "$pause_value" "Pause mode enabled in config"
}

# --- Test: No failover while paused ---
test_no_failover_while_paused() {
    log_info "Testing that failover does not occur while paused..."

    local primary_url
    primary_url=$(get_primary)
    local primary_container
    primary_container=$(node_url_to_container "$primary_url")
    log_info "Killing primary $primary_container while paused..."

    # Kill the primary
    docker kill "$primary_container" > /dev/null 2>&1

    # Wait and check that no new primary is elected
    log_info "Waiting 20s to confirm no failover occurs..."
    sleep 20

    local new_primary=""
    for node in "${ALL_NODES[@]}"; do
        # Skip the killed node's URL
        if [ "$node" = "$primary_url" ]; then
            continue
        fi
        local status
        status=$(http_get_status "$node/primary")
        if [ "$status" = "200" ]; then
            new_primary="$node"
            break
        fi
    done

    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -z "$new_primary" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "No failover occurred while paused (as expected)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Failover occurred while paused — new primary at $new_primary"
    fi

    # Store the killed container for later
    export KILLED_CONTAINER="$primary_container"
    export KILLED_NODE_URL="$primary_url"
}

# --- Test: Disable pause → failover resumes ---
test_disable_pause_triggers_failover() {
    log_info "Disabling pause mode..."

    # Find a running node to issue the config change
    local running_node=""
    for node in "${ALL_NODES[@]}"; do
        local status
        status=$(http_get_status "$node/patroni")
        if [ "$status" = "200" ]; then
            running_node="$node"
            break
        fi
    done

    if [ -z "$running_node" ]; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "No running node available to disable pause"
        return
    fi

    local response
    response=$(http_patch "$running_node/config" '{"pause": false}')
    log_info "Unpause response: $response"

    # Now failover should proceed
    log_info "Waiting for failover after unpausing..."
    local new_primary
    new_primary=$(wait_for_primary 45) || true

    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$new_primary" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        local container
        container=$(node_url_to_container "$new_primary")
        log_pass "Failover occurred after disabling pause (primary: $container)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "No failover after disabling pause"
    fi
}

# --- Test: Restart killed node and verify cluster recovers ---
test_cluster_recovery_after_pause() {
    local container="${KILLED_CONTAINER:-}"
    if [ -z "$container" ]; then
        log_warn "No killed container to restart, skipping"
        return
    fi

    log_info "Restarting $container..."
    docker start "$container" > /dev/null 2>&1

    sleep 10
    wait_for_cluster_ready 60 || log_warn "Cluster slow to recover"

    # Verify pause is still off
    local primary_url
    primary_url=$(get_primary)
    local config
    config=$(http_get "$primary_url/config")
    local pause_value
    pause_value=$(echo "$config" | jq -r '.pause // false' 2>/dev/null)

    assert_eq "false" "$pause_value" "Pause mode remains disabled after recovery"
}

# --- Run tests ---
test_enable_pause
test_no_failover_while_paused
test_disable_pause_triggers_failover
test_cluster_recovery_after_pause

print_results
