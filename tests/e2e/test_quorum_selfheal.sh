#!/bin/bash
# tests/e2e/test_quorum_selfheal.sh — SB-37: a quorum-mode cluster without a
# leader heals itself when it can, and says so (503 on /replica) when it cannot.
#
#   (a) an ordinary failover never shows the "election is stuck" reason on the
#       standbys' /replica;
#   (b) scenario 8: the primary is killed, the new primary is killed right
#       after it appears, then both are started again. A primary appears with
#       no command, no acknowledged row is lost, the standbys are `Replica`,
#       and no node logs "no timeline evidence published";
#   (c) the lost-voter case: the old primary and a voter stay down. With
#       5 nodes the survivors keep the Raft majority but cannot count q votes:
#       /replica is 503 with the reason after the lock ttl and /cluster says
#       election.stalled. With 3 nodes the Raft majority is lost too, which
#       the DCS-unreadable rule reports the same way. After the nodes return,
#       /replica is 200 again.
#
# Real PostgreSQL only: the writer's acknowledged ids are compared with the
# rows of the final primary.
#
# Usage (3 nodes, the default compose file):
#   make PG_VERSION=18 up
#   SYNC_MODE=quorum ./tests/e2e/run_tests.sh --no-build quorum_selfheal
# 5 nodes (docker-compose.5node.yml, started by hand, REST ports 8008..8012):
#   QSH_NODES=5 ./tests/e2e/run_tests.sh --no-build --no-setup quorum_selfheal
# The suite sets synchronous_mode=quorum itself and waits for it to settle.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

QSH_NODES="${QSH_NODES:-3}"
case "$QSH_NODES" in
    3) CPFX=pg-ha-node ;;
    5) CPFX=pg-ha5-node ;;
    *) echo "QSH_NODES must be 3 or 5"; exit 2 ;;
esac
STUCK_REASON="cluster has no leader and the election is stuck"
NODES=()
for i in $(seq 1 "$QSH_NODES"); do NODES+=("node$i"); done
SUITE_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TMPD=$(mktemp -d)
trap 'rm -rf "$TMPD"; qsh_start_all' EXIT

qsh_c() { echo "$CPFX${1#node}"; }
qsh_url() { echo "$SCHEME://localhost:$((8007 + ${1#node}))"; }
qsh_psql() { docker exec "$(qsh_c "$1")" psql -U "$PG_USER" -d postgres -Atc "$2" 2>/dev/null; }
qsh_code() { curl "${CURL_OPTS[@]}" -m 3 -o /dev/null -w '%{http_code}' "$(qsh_url "$1")/$2" 2>/dev/null || echo 000; }
qsh_get() { curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 5 "$(qsh_url "$1")/$2" 2>/dev/null; }
qsh_running() { [ "$(docker inspect -f '{{.State.Running}}' "$(qsh_c "$1")" 2>/dev/null)" = true ]; }
qsh_primary() {
    local n
    for n in "${NODES[@]}"; do
        qsh_running "$n" || continue
        [ "$(qsh_code "$n" primary)" = 200 ] && { echo "$n"; return 0; }
    done
    echo ""
}
qsh_start_all() {
    local n
    for n in "${NODES[@]}"; do qsh_running "$n" || docker start "$(qsh_c "$n")" > /dev/null 2>&1; done
}
# One primary, every other node a 200 on /replica, quorum settled over all of them.
qsh_ready() { # [timeout]
    local timeout="${1:-240}" start=$SECONDS p n ok sync
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        p=$(qsh_primary)
        if [ -n "$p" ]; then
            ok=1
            for n in "${NODES[@]}"; do
                [ "$n" = "$p" ] && continue
                [ "$(qsh_code "$n" replica)" = 200 ] || { ok=0; break; }
            done
            if [ "$ok" = 1 ]; then
                sync=$(qsh_get "$p" cluster | jq -r --argjson want $((QSH_NODES - 1)) \
                    'if (.sync.kind == "quorum" and .sync.settled == true and ((.sync.voters | length) >= $want)) then "settled" else "waiting" end' 2>/dev/null)
                [ "$sync" = settled ] && { echo "$p"; return 0; }
            fi
        fi
        sleep 2
    done
    return 1
}
# qsh_check <true|false> <message>: an assertion on a condition already evaluated
qsh_check() {
    if [ "$1" = true ]; then assert_eq "ok" "ok" "$2"; else assert_eq "ok" "failed" "$2"; fi
}
qsh_set_quorum() {
    local p code
    p=$(qsh_primary)
    [ -n "$p" ] || return 1
    code=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 15 -o /dev/null -w '%{http_code}' -X PATCH \
        -H 'Content-Type: application/json' -d '{"synchronous_mode":"quorum"}' "$(qsh_url "$p")/config")
    [ "$code" = 200 ]
}
qsh_prepare() { # leaves the cluster ready in quorum mode; echoes the primary
    local n p
    qsh_start_all
    for n in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
        p=$(qsh_primary); [ -n "$p" ] && break; sleep 2
    done
    qsh_set_quorum || log_warn "could not PATCH synchronous_mode=quorum" >&2
    qsh_ready 300 && return 0
    # Not ready: show what each node says (stderr, so the caller's value stays empty).
    for n in "${NODES[@]}"; do
        {
            echo "[DIAG] $n /patroni $(qsh_get "$n" patroni | jq -c '{role,state,timeline,pg_in_recovery}' 2>/dev/null) /replica $(qsh_code "$n" replica) /primary $(qsh_code "$n" primary)"
            docker logs --tail 400 "$(qsh_c "$n")" 2>&1 | grep pg_ha | grep -v "no action\|openraft\|Health probe" | tail -n 12 | cut -c1-260 | sed "s/^/[DIAG] $n: /"
        } >&2
    done
    echo "[DIAG] sync $(qsh_get "${NODES[0]}" cluster | jq -c .sync 2>/dev/null)" >&2
    return 1
}
# The writer runs inside the primary's container; every id psql acknowledged is
# appended to /tmp/acked (the file survives `docker kill`).
qsh_start_writer() { # <node>
    docker exec "$(qsh_c "$1")" sh -c 'rm -f /tmp/acked /tmp/acked.rejected; : > /tmp/acked'
    qsh_psql "$1" 'create table if not exists ql_ack(id serial primary key, t timestamptz default now())' > /dev/null
    # An id counts as acknowledged only when psql exited 0 with nothing on
    # stderr: a commit whose synchronous wait was cancelled still returns its
    # id, with a WARNING, and is NOT acknowledged. Anything else goes to
    # /tmp/acked.rejected.
    docker exec -d "$(qsh_c "$1")" sh -c 'while true; do id=$(psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -qAtc "insert into ql_ack default values returning id" 2>/tmp/acked.err); rc=$?; if [ "$rc" = 0 ] && [ ! -s /tmp/acked.err ] && [ -n "$id" ]; then echo "$id" >> /tmp/acked; else echo "id=${id:-none} rc=$rc $(head -c 200 /tmp/acked.err | tr "\n" " ")" >> /tmp/acked.rejected; fi; sleep 0.05; done'
}
qsh_wait_primary() { # <timeout> [exclude...]: first node answering /primary 200
    local timeout="$1" start=$SECONDS n skip x
    shift
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        for n in "${NODES[@]}"; do
            skip=0
            for x in "$@"; do [ "$x" = "$n" ] && skip=1; done
            [ "$skip" = 1 ] && continue
            qsh_running "$n" || continue
            [ "$(qsh_code "$n" primary)" = 200 ] && { echo "$n"; return 0; }
        done
        sleep 0.4
    done
    echo ""
    return 1
}

# ── (a) ordinary failover never shows the stuck reason ──
test_ordinary_failover_never_reports_stuck() {
    log_section "(a) ordinary failover: /replica never carries the stuck reason"
    local p newp n log="$TMPD/poll-a.log" stop="$TMPD/stop-a"
    p=$(qsh_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    rm -f "$stop"; : > "$log"
    (
        while [ ! -f "$stop" ]; do
            for n in "${NODES[@]}"; do
                [ "$n" = "$p" ] && continue
                echo "$n $(curl "${CURL_OPTS[@]}" -m 2 -w ' HTTP %{http_code}' "$(qsh_url "$n")/replica" 2>/dev/null | tr -d '\n')" >> "$log"
            done
            sleep 0.5
        done
    ) &
    local poller=$!
    sleep 2
    docker kill "$(qsh_c "$p")" > /dev/null
    newp=$(qsh_wait_primary 90 "$p")
    sleep 3
    touch "$stop"; wait "$poller" 2>/dev/null
    assert_not_empty "$newp" "a new primary appeared after the kill"
    local samples stuck non200
    samples=$(wc -l < "$log" | tr -d ' ')
    stuck=$(grep -c "$STUCK_REASON" "$log")
    non200=$(grep -vc "HTTP 200" "$log")
    log_info "polled /replica $samples times; $non200 were not 200 (a standby that is the candidate or is being promoted)"
    assert_eq "0" "$stuck" "no /replica answer carried the stuck reason during an ordinary failover"
    qsh_check "$([ "$samples" -ge 10 ] && echo true || echo false)" "polled /replica $samples times (>= 10) from the kill"
    docker start "$(qsh_c "$p")" > /dev/null
}

# ── (b) scenario 8: double kill in the post-failover window ──
test_scenario_8_heals_by_itself() {
    log_section "(b) scenario 8 ($QSH_NODES nodes): kill the primary, kill the new primary, start both"
    local p newp n t0 prim acked missing
    p=$(qsh_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    local since
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    qsh_start_writer "$p"
    sleep 4
    docker kill "$(qsh_c "$p")" > /dev/null
    newp=$(qsh_wait_primary 90 "$p")
    assert_not_empty "$newp" "kill 1: a new primary appeared"
    [ -n "$newp" ] || return
    docker kill "$(qsh_c "$newp")" > /dev/null
    log_info "killed $p, then the new primary $newp right after it appeared"
    sleep 25
    prim=$(qsh_wait_primary 3 "$p" "$newp")
    log_info "primary among the survivors while both are down: ${prim:-none}"
    # Precondition of scenario 8: the survivors are stuck while both are
    # down. If they elected by themselves, the run does not test self-healing.
    if [ -n "$prim" ]; then
        log_fail "INVALID: $prim became primary while $p and $newp were down; scenario 8 was not reached"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
        return
    fi
    local k
    for k in "$p" "$newp"; do
        qsh_check "$(docker inspect -f '{{.State.Running}}' "$(qsh_c "$k")" 2>/dev/null | grep -qx false && echo true || echo false)" "precondition: $k is really stopped"
    done
    docker cp "$(qsh_c "$p"):/tmp/acked" "$TMPD/acked" > /dev/null 2>&1
    grep '^[0-9]' "$TMPD/acked" | sort > "$TMPD/acked.ids"
    acked=$(wc -l < "$TMPD/acked.ids" | tr -d ' ')
    qsh_check "$([ "${acked:-0}" -gt 0 ] && echo true || echo false)" "the writer acknowledged $acked rows before the kill"
    local restarted
    restarted=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    docker start "$(qsh_c "$p")" "$(qsh_c "$newp")" > /dev/null
    t0=$SECONDS
    prim=$(qsh_wait_primary 240)
    local heal=$((SECONDS - t0)) ttl
    assert_not_empty "$prim" "a primary appeared with no command ($heal s after the start)"
    [ -n "$prim" ] || return
    # Within the order of one lock ttl: the two nodes need a start and a few
    # HA cycles each, so the bound is 4 x ttl (60 s with the dev ttl of 15 s).
    ttl=$(qsh_get "$prim" config | jq -r '.ttl // 30' 2>/dev/null)
    case "$ttl" in ''|*[!0-9]*) log_warn "ttl read from /config is '$ttl', using the default 30"; ttl=30 ;; esac
    qsh_check "$([ "$heal" -le $((4 * ttl)) ] && echo true || echo false)" "healed in $heal s (<= 4 x ttl = $((4 * ttl)) s)"
    local final
    final=$(qsh_ready 300)
    assert_not_empty "$final" "the cluster is ready again (one primary, every standby 200 on /replica)"
    prim=${final:-$prim}
    qsh_psql "$prim" "select id from ql_ack" | sort > "$TMPD/rows.ids"
    missing=$(comm -23 "$TMPD/acked.ids" "$TMPD/rows.ids" | wc -l | tr -d ' ')
    assert_eq "0" "$missing" "zero of $acked acknowledged rows are missing on the primary $prim"
    local role bad=0
    for n in "${NODES[@]}"; do
        [ "$n" = "$prim" ] && continue
        role=$(qsh_get "$n" patroni | jq -r '.role' 2>/dev/null)
        [ "$role" = "Replica" ] || { bad=$((bad + 1)); log_info "$n reports role '$role'"; }
    done
    assert_eq "0" "$bad" "every standby reports role Replica"
    # The G3 symptom is a RETURNED node that publishes no timeline evidence.
    # While two of three nodes are down the survivor cannot write its member
    # record (no Raft majority) and may log the line about its stale record;
    # those lines are printed, the assertion counts the ones after the start.
    local evidence=0 before=0 lines
    for n in "${NODES[@]}"; do
        lines=$(docker logs --since "$since" "$(qsh_c "$n")" 2>&1 | grep "no timeline evidence published")
        [ -n "$lines" ] && echo "$lines" | cut -c1-200 | sed "s/^/[INFO] $n: /"
        before=$((before + $(echo "$lines" | awk -v t="$restarted" 'NF && $1 < t' | wc -l)))
        evidence=$((evidence + $(echo "$lines" | awk -v t="$restarted" 'NF && $1 >= t' | wc -l)))
    done
    log_info "'no timeline evidence published' lines while the two nodes were down: $before"
    assert_eq "0" "$evidence" "no node logged 'no timeline evidence published' after the restart"
}

# ── (c) the lost-voter case: stuck, 503 with the reason, then healed ──
test_lost_voter_is_reported_and_heals_on_return() {
    log_section "(c) lost voter ($QSH_NODES nodes): 503 with the reason after the ttl, 200 after the return"
    local p v n survivors=() ttl=30 t0 got
    p=$(qsh_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    # The voter that stays down with the old primary: any other node.
    for n in "${NODES[@]}"; do [ "$n" != "$p" ] && { v=$n; break; }; done
    for n in "${NODES[@]}"; do [ "$n" != "$p" ] && [ "$n" != "$v" ] && survivors+=("$n"); done
    ttl=$(qsh_get "$p" config | jq -r '.ttl // 30' 2>/dev/null)
    docker kill "$(qsh_c "$p")" "$(qsh_c "$v")" > /dev/null
    log_info "killed the primary $p and the voter $v; survivors: ${survivors[*]}; ttl ${ttl}s"
    # Before the ttl a survivor is not reported stuck (an ordinary failover window).
    sleep 3
    # It must answer 200 without the reason: no answer is not "not stuck".
    got=$(curl "${CURL_OPTS[@]}" -m 3 -w ' HTTP %{http_code}' "$(qsh_url "${survivors[0]}")/replica" 2>/dev/null)
    qsh_check "$(echo "$got" | grep -q "$STUCK_REASON" || ! echo "$got" | grep -q ' HTTP 200$' || echo true)" \
        "a standby answers 200 without the stuck reason 3 s after the kill (${got##* HTTP })"

    # After the ttl (plus loop_wait slack) the survivors answer 503 with the reason.
    t0=$SECONDS
    local stuck_nodes=0
    while [ $((SECONDS - t0)) -lt $((ttl + 60)) ]; do
        stuck_nodes=0
        for n in "${survivors[@]}"; do
            curl "${CURL_OPTS[@]}" -m 3 -w ' HTTP %{http_code}' "$(qsh_url "$n")/replica" 2>/dev/null | grep -q "$STUCK_REASON.* HTTP 503" && stuck_nodes=$((stuck_nodes + 1))
        done
        [ "$stuck_nodes" -eq "${#survivors[@]}" ] && break
        sleep 2
    done
    log_info "survivors reporting the stuck reason: $stuck_nodes of ${#survivors[@]} after $((SECONDS - t0)) s"
    assert_eq "${#survivors[@]}" "$stuck_nodes" "every survivor answers 503 with the stuck reason after the ttl"
    if [ "$QSH_NODES" = 5 ]; then
        local stalled=0
        for n in "${survivors[@]}"; do
            [ "$(qsh_get "$n" cluster | jq -r '.election.stalled' 2>/dev/null)" = true ] && stalled=$((stalled + 1))
        done
        qsh_check "$([ "$stalled" -ge 1 ] && echo true || echo false)" "/cluster election.stalled is true on a survivor ($stalled)"
    fi
    local elec
    elec=$(qsh_get "${survivors[0]}" cluster | jq -r '.election.leaderless_stuck' 2>/dev/null)
    assert_eq "true" "$elec" "/cluster election.leaderless_stuck is true"
    # The two nodes return: 200 again, one primary.
    docker start "$(qsh_c "$p")" "$(qsh_c "$v")" > /dev/null
    t0=$SECONDS
    local final
    final=$(qsh_ready 300)
    assert_not_empty "$final" "after the return the cluster is ready again ($((SECONDS - t0)) s; /replica is 200 on every standby)"
    local gone=0
    for n in "${NODES[@]}"; do
        [ "$n" = "$final" ] && continue
        qsh_get "$n" cluster | jq -r '.election.leaderless_stuck' 2>/dev/null | grep -q true && gone=$((gone + 1))
    done
    assert_eq "0" "$gone" "no node still reports leaderless_stuck"
}

log_section "TEST: SB-37 quorum self-heal ($QSH_NODES nodes)"
test_ordinary_failover_never_reports_stuck
test_scenario_8_heals_by_itself
test_lost_voter_is_reported_and_heals_on_return
print_results
