#!/bin/bash
# tests/e2e/test_switchover.sh — Verify manual switchover via REST API
#
# Tests:
#   - POST /switchover → leader changes to specified candidate
#   - Old leader becomes replica
#   - Cluster remains healthy after switchover
#   - The first switchover is posted right after the cluster is ready, with no
#     sleep and no retry (SB-29: a stale cluster view rejected it)
#   - Switchover posted to the new primary right after a failover is accepted
#     and does not fence that primary (SB-29)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

log_section "TEST: Manual Switchover"
# POST and print "<http_code> <body>" (http_post drops the status code).
post_with_status() {
    local url="$1"
    local data="$2"
    local out
    out=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 15 -w $'\n%{http_code}' \
        -X POST -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null) || true
    local code="${out##*$'\n'}"
    local body="${out%$'\n'*}"
    [ "$code" = "$out" ] && body=""
    echo "${code:-000} $body"
}
# wait_for_* print their log lines on stdout too; keep only the URL line.
url_only() {
    grep -E '^https?://[^ ]+$' | tail -n 1
}

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
    # No sleep and no retry: the cluster was just reported ready (SB-29).
    log_info "Posting switchover request..."
    local response
    response=$(post_with_status "$primary_url/switchover" "{\"candidate\": \"$candidate_name\"}")
    log_info "Switchover response: $response"
    assert_eq "200" "${response%% *}" "Switchover right after ready returns HTTP 200"

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

    # A switchover demotes the old leader and the other nodes re-point their
    # replication, so a node can briefly report a non-Running state right after.
    # Poll each node until it settles on Running (bounded) before asserting,
    # instead of reading the state once during the transition.
    for node in "${ALL_NODES[@]}"; do
        local container
        container=$(node_url_to_container "$node")
        local state=""
        local start=$SECONDS
        while [ "$((SECONDS - start))" -lt 60 ]; do
            state=$(get_node_state "$node")
            [ "$state" = "running" ] && break
            sleep 2
        done
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

# --- Test: Switchover right after a failover (SB-29) ---
test_switchover_after_failover() {
    log_info "Testing switchover right after a failover..."
    local old_primary
    old_primary=$(wait_for_primary 60 | url_only)
    assert_not_empty "$old_primary" "Primary exists before the failover"
    [ -z "$old_primary" ] && return
    local old_container
    old_container=$(node_url_to_container "$old_primary")
    local since
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    docker_kill "$old_container" > /dev/null 2>&1
    local new_primary
    new_primary=$(wait_for_new_primary "$old_primary" 90 | url_only)
    assert_not_empty "$new_primary" "New primary elected after killing $old_container"
    if [ -z "$new_primary" ]; then
        docker_start "$old_container" > /dev/null 2>&1
        return
    fi
    local new_container
    new_container=$(node_url_to_container "$new_primary")
    local candidate_url="" node
    for node in "${ALL_NODES[@]}"; do
        if [ "$node" != "$old_primary" ] && [ "$node" != "$new_primary" ]; then
            candidate_url="$node"
        fi
    done
    local candidate_name
    candidate_name=$(get_node_name "$candidate_url")
    # Post at once, no sleep and no retry.
    local response
    response=$(post_with_status "$new_primary/switchover" "{\"candidate\": \"$candidate_name\"}")
    # A fence marks the node before the reply; the clean demotion of an
    # accepted switchover marks it later, on restart as a replica. So only
    # markers logged up to the reply (plus 0.5 s) count as a fence.
    local cutoff
    cutoff=$(perl -MTime::HiRes=time -MPOSIX=strftime -e \
        'my $t = time + 0.5; printf "%s.%06dZ", strftime("%Y-%m-%dT%H:%M:%S", gmtime($t)), ($t - int($t)) * 1e6')
    log_info "Switchover to $candidate_name via $new_container: $response"
    assert_eq "200" "${response%% *}" "Switchover right after the failover returns HTTP 200"
    local after
    after=$(wait_for_new_primary "$new_primary" 60 | url_only)
    assert_not_empty "$after" "A primary is available within 60s after the switchover"
    local markers
    markers=$(docker logs --since "$since" "$new_container" 2>&1 \
        | grep "Created rewind-check marker" \
        | awk -v cut="$cutoff" '{ if ($1 <= cut) n++ } END { print n + 0 }')
    assert_eq "0" "$markers" "$new_container was not fenced (no rewind-check marker from the kill to the reply)"
    # Bring the killed node back so the cluster is whole again.
    docker_start "$old_container" > /dev/null 2>&1
    wait_for_cluster_ready 180 || log_warn "Cluster not ready after restarting $old_container"
}
# --- Run tests ---
wait_for_cluster_ready 240 || log_warn "Cluster not ready before the first switchover"
test_switchover_to_candidate
sleep 5
test_old_leader_demotes
test_cluster_healthy_after_switchover

# Wait before next test
sleep 10
wait_for_cluster_ready 60 || log_warn "Cluster not ready for auto-candidate test"
test_switchover_auto_candidate

wait_for_cluster_ready 120 || log_warn "Cluster not ready for the failover test"
test_switchover_after_failover
print_results
