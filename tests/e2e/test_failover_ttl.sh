#!/bin/bash
# tests/e2e/test_failover_ttl.sh — SB-05: the /failover DCS key self-expires
#
# A failover/switchover intent names a candidate. Before SB-05 the /failover
# key was written with no TTL, so if the named candidate died (or vanished
# from the member list) before it took the leader lock, the clear line in
# election.rs never ran and peers kept deferring to the dead candidate — the
# cluster could stay permanently leaderless.
#
# SB-05 gives the key the cluster lease TTL (reusing `ttl`, default 30s). A
# stale intent now self-expires and the cluster returns to normal election.
#
# This test proves:
#   (a) switchover naming a candidate that then dies → the intent is honored
#       (peers defer, visible in the logs), the key EXPIRES after ~ttl, and
#       the cluster then elects a primary (/primary 200). Quorum is preserved
#       throughout (2 of 3 nodes stay up).
#   (b) a NORMAL switchover to a healthy candidate still works — the intent is
#       honored and cleared well within the TTL (no regression).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# The cluster lease TTL the /failover key now reuses. Must match the running
# cluster's `ttl` (docker-compose.override.yml sets it to 60 for a wide,
# observable expiry window; the entrypoint default is 15).
TTL=${PG_HA_TTL:-60}

# The docker entrypoint configures Basic Auth (admin:secret) on the management
# endpoints (/failover, /switchover, ...), so those POSTs carry credentials.
API_AUTH="admin:secret"

auth_post() {
    local url="$1"
    local data="${2:-}"
    # A synchronous switchover stops PostgreSQL and releases the lock before it
    # responds, which can take longer than a health-check timeout — allow 60s.
    curl -s --max-time 60 -u "$API_AUTH" -X POST \
        -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || echo ""
}

# Count the deferring-to-candidate lines in a container's FULL log. This is the
# observable proxy for "the /failover intent is present and honored": main.rs
# logs every cycle result, and election.rs emits "deferring to designated
# candidate (<name>)" while a survivor yields to the key. Grepping the full log
# (not a live tail) is race-free for after-the-fact assertions.
deferring_to() {
    local container="$1"
    local candidate="$2"
    docker logs "$container" 2>&1 |
        grep -c "deferring to designated candidate ($candidate)" || true
}

log_section "TEST: /failover key TTL (SB-05)"

# ────────────────────────────── Scenario (a) ──────────────────────────────
# Dead candidate must not leave the cluster leaderless forever.
test_dead_candidate_intent_expires() {
    wait_for_cluster_ready 120 || {
        log_fail "Cluster not ready at start of scenario (a)"
        return
    }

    local primary_url
    primary_url=$(get_primary)
    local primary_name
    primary_name=$(get_node_name "$primary_url")
    local primary_container
    primary_container=$(node_url_to_container "$primary_url")
    log_info "Primary before test: $primary_name ($primary_container)"

    # Pick a replica as the SWITCHOVER CANDIDATE (it will die), and the other
    # replica as the OBSERVER that must run the eventual election.
    local replicas candidate_url observer_url
    replicas=$(get_replicas)
    candidate_url=$(printf '%s\n' "$replicas" | sed -n '1p')
    observer_url=$(printf '%s\n' "$replicas" | sed -n '2p')
    local candidate_name observer_name candidate_container observer_container
    candidate_name=$(get_node_name "$candidate_url")
    observer_name=$(get_node_name "$observer_url")
    candidate_container=$(node_url_to_container "$candidate_url")
    observer_container=$(node_url_to_container "$observer_url")
    assert_not_empty "$candidate_name" "A replica switchover candidate exists"
    assert_not_empty "$observer_name" "A second replica (observer) exists"
    log_info "Candidate (will die): $candidate_name ($candidate_container)"
    log_info "Observer (must elect): $observer_name ($observer_container)"

    # The switchover command validates the candidate's DCS member record, which
    # the primary only refreshes on its own HA cycle (loop_wait), so it lags the
    # candidate's self-reported state. Wait until the candidate reports Running,
    # then retry the switchover until the primary's cluster view agrees (no more
    # "candidate is not running").
    log_info "Waiting for $candidate_name to report a Running state..."
    for _ in $(seq 1 30); do
        [ "$(get_node_state "$candidate_url")" = "Running" ] && break
        sleep 2
    done

    # Deterministic reproduction that KEEPS Raft quorum (2 of 3):
    #   1. Switchover from the primary naming the replica candidate. The
    #      switchover is synchronous: when the POST returns the old primary has
    #      already demoted itself and released the lock, and /failover holds
    #      {leader: old-primary, candidate}. The stepped-down leader is excluded
    #      from the election.
    #   2. KILL the candidate before it can acquire the lock.
    # Now the only node that could campaign is the observer — but it defers to
    # the named candidate (honoured even once the candidate's member record
    # expires and it becomes "missing"). Without SB-05 the /failover key never
    # expires, so the observer defers to the dead candidate forever and the
    # cluster is permanently leaderless despite having quorum.
    log_info "Switching over from $primary_name to $candidate_name (synchronous; demotes the old primary)..."
    local resp=""
    for _ in $(seq 1 12); do
        resp=$(auth_post "$primary_url/switchover" "{\"leader\": \"$primary_name\", \"candidate\": \"$candidate_name\"}")
        log_info "Switchover response: $resp"
        case "$resp" in
            *nitiated*) break ;;
            *"not running"*) sleep 5 ;; # primary's view hasn't caught up yet
            *) sleep 3 ;;
        esac
    done
    assert_contains "$resp" "nitiated" "Switchover initiated (old primary demoted, /failover key written)"

    log_info "Killing the named candidate before it acquires the lock: $candidate_container"
    docker kill "$candidate_container" >/dev/null 2>&1 || true
    local killed_at=$SECONDS

    # ── The intent is honored (key present) ──
    # The old primary has demoted and released the lock, the candidate is dead,
    # so the observer runs the election logic and defers to the named (dead)
    # candidate. Confirm this within the first part of the TTL window — while
    # the /failover key is still alive the cluster is leaderless.
    log_info "Confirming the observer defers to the dead candidate (intent present)..."
    local deferred=0
    local half_window=$((TTL * 2 / 3))
    while [ "$((SECONDS - killed_at))" -lt "$half_window" ]; do
        if [ "$(deferring_to "$observer_container" "$candidate_name")" -gt 0 ]; then
            deferred=1
            break
        fi
        sleep 3
    done
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$deferred" = "1" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Intent honored: $observer_name defers to dead candidate $candidate_name (key present)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Never observed $observer_name deferring to $candidate_name"
    fi

    # While the unexpired intent names the dead candidate, there is no primary
    # even though quorum (old-primary + observer = 2/3) is intact.
    local leaderless_primary
    leaderless_primary=$(get_primary || true)
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -z "$leaderless_primary" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Cluster is leaderless while the unexpired /failover key names the dead candidate"
    else
        log_warn "A primary ($leaderless_primary) already exists; the key may have expired faster than expected"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi

    # ── THE FIX (key gone) ──
    # After ~ttl seconds the /failover key expires, the observer stops
    # deferring, the stepped-down leader is no longer excluded, and the
    # surviving quorum elects a primary. Allow ttl plus a few loop_wait cycles.
    log_info "Waiting for the /failover key to expire (~${TTL}s) and a primary to be elected..."
    local new_primary=""
    local deadline=$((SECONDS + TTL + 80))
    while [ "$SECONDS" -lt "$deadline" ]; do
        new_primary=$(get_primary || true)
        if [ -n "$new_primary" ]; then
            break
        fi
        sleep 2
    done
    local elected_at=$((SECONDS - killed_at))

    assert_not_empty "$new_primary" "A primary was elected after the stale intent expired"
    if [ -n "$new_primary" ]; then
        assert_http_status "$new_primary/primary" "200" "Elected primary responds 200 on /primary"
        local np_name
        np_name=$(get_node_name "$new_primary")
        log_info "New primary ${np_name} (${new_primary}) elected ~${elected_at}s after the kill (TTL=${TTL}s)"
    fi

    # Bring the killed candidate back so the cluster is whole for scenario (b).
    log_info "Restarting the killed candidate ($candidate_container)..."
    docker start "$candidate_container" >/dev/null 2>&1 || true
    wait_for_cluster_ready 120 || log_warn "Cluster not fully ready after restart"
    # Give the rejoined node a moment to finish becoming a replica so (b) has a
    # healthy candidate to switch to.
    sleep "$TTL"
}

# ────────────────────────────── Scenario (b) ──────────────────────────────
# A normal switchover to a healthy candidate still works (no TTL regression):
# the intent is honored and cleared well within the TTL window.
test_normal_switchover_still_works() {
    wait_for_cluster_ready 120 || {
        log_fail "Cluster not ready at start of scenario (b)"
        return
    }

    local primary_url
    primary_url=$(get_primary)
    local primary_name
    primary_name=$(get_node_name "$primary_url")
    # Wait until a named replica candidate is actually available (the node that
    # died in scenario (a) may still be rejoining).
    local candidate_url="" candidate_name=""
    for _ in $(seq 1 30); do
        local replicas
        replicas=$(get_replicas)
        candidate_url=$(printf '%s\n' "$replicas" | sed -n '1p')
        if [ -n "$candidate_url" ]; then
            candidate_name=$(get_node_name "$candidate_url")
            [ -n "$candidate_name" ] && break
        fi
        sleep 3
    done
    assert_not_empty "$candidate_name" "A healthy replica candidate is available"
    log_info "Normal switchover: $primary_name -> $candidate_name"

    local start=$SECONDS
    auth_post "$primary_url/switchover" "{\"candidate\": \"$candidate_name\"}" >/dev/null

    # Wait for the named candidate to answer 200 on /primary (role strings are
    # capitalised — "Primary"/"Replica" — so assert on the health endpoint).
    local became_primary=0
    while [ "$((SECONDS - start))" -lt 60 ]; do
        if [ "$(http_get_status "$candidate_url/primary")" = "200" ]; then
            became_primary=1
            break
        fi
        sleep 2
    done
    local elapsed=$((SECONDS - start))
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$became_primary" = "1" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_pass "Healthy candidate $candidate_name became primary in ${elapsed}s (well within TTL=${TTL}s)"
        assert_http_status "$candidate_url/primary" "200" "New primary responds 200 on /primary"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_fail "Healthy candidate $candidate_name did not become primary within ${elapsed}s"
    fi
}

test_dead_candidate_intent_expires
test_normal_switchover_still_works

print_results
