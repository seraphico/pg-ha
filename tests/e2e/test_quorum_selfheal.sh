#!/bin/bash
# tests/e2e/test_quorum_selfheal.sh — SB-37: a quorum-mode cluster without a
# leader heals itself when it can, and says so (503 on /replica) when it cannot.
#
#   (a) an ordinary failover never shows the "election is stuck" reason on the
#       standbys' /replica;
#   (b) scenario 8: the primary is killed, the new primary is killed right
#       after it appears, then both are started again. A primary appears with
#       no command, no acknowledged row is lost, the standbys are `replica`,
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
#
# INVALID is not PASS and not a product FAIL (CONVENTIONS section 12). The
# post-promote window (q = |V|, docs "Known limitation") means a self-heal
# election cannot finish if the NEW leader is fenced or lost within about
# 10-20 s of its election (a host stall longer than ttl - safety_margin). When
# that happens (a primary was seen after the restart and later lost on every
# node, or a node logs "Leader lease expired before the lock renewal
# completed" / "DCS write timed out" after that first primary was seen) the run did not test
# self-healing: it is reported as INVALID with a one-line reason and the part
# is retried once. Two INVALID attempts are a counted failure, never a pass.
# A real heal bug (no primary ever seen, or a primary that stays and never
# gets 200 standbys) is still a FAIL, with per-node evidence printed.
#
# Opt-in negative control for the INVALID path (a host stall after the first
# primary shows up after the restart; parts (b) and (c)):
#   QSH_FREEZE_AFTER=8    seconds after that first /primary 200 (unset = off)
#   QSH_FREEZE_FOR=8      seconds the containers stay paused
#   QSH_FREEZE_TARGET=all all | leader | others
# QSH_PARTS="a b c" selects the parts (default all).
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

QSH_NODES="${QSH_NODES:-3}"
QSH_PARTS="${QSH_PARTS:-a b c}"
QSH_FREEZE_FOR="${QSH_FREEZE_FOR:-8}"
QSH_FREEZE_TARGET="${QSH_FREEZE_TARGET:-all}"
case "$QSH_FREEZE_TARGET" in all|leader|others) ;; *) echo "QSH_FREEZE_TARGET must be all, leader or others"; exit 2 ;; esac
case "${QSH_FREEZE_AFTER:-0}" in ''|*[!0-9]*) echo "QSH_FREEZE_AFTER must be a number of seconds"; exit 2 ;; esac
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
trap 'rm -rf "$TMPD"; qsh_unpause_all; qsh_start_all' EXIT
QSH_INVALID=""   # one-line reason when the current attempt did not test self-healing
QSH_FINAL=""     # the primary of a ready cluster, set by qsh_wait_healed
QSH_SEEN=0       # a primary answered /primary 200 after the restart
QSH_SEEN_AT=""   # UTC time of that first answer
QSH_FROZEN=0     # the opt-in freeze ran in this attempt
QSH_MARKERS='Leader lease expired before the lock renewal completed|DCS write timed out'

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
qsh_unpause_all() {
    local n
    for n in "${NODES[@]}"; do
        [ "$(docker inspect -f '{{.State.Paused}}' "$(qsh_c "$n")" 2>/dev/null)" = true ] && docker unpause "$(qsh_c "$n")" > /dev/null 2>&1
    done
}
qsh_start_all() {
    local n
    for n in "${NODES[@]}"; do qsh_running "$n" || docker start "$(qsh_c "$n")" > /dev/null 2>&1; done
}
# One primary, every other node a 200 on /replica, quorum settled over all of them.
qsh_ready_once() { # <primary>: one look; prints the primary when the cluster is ready
    local p="$1" n sync
    for n in "${NODES[@]}"; do
        [ "$n" = "$p" ] && continue
        [ "$(qsh_code "$n" replica)" = 200 ] || return 1
    done
    sync=$(qsh_get "$p" cluster | jq -r --argjson want $((QSH_NODES - 1)) \
        'if (.sync.kind == "quorum" and .sync.settled == true and ((.sync.voters | length) >= $want)) then "settled" else "waiting" end' 2>/dev/null)
    [ "$sync" = settled ] && { echo "$p"; return 0; }
    return 1
}
qsh_ready() { # [timeout]
    local timeout="${1:-240}" start=$SECONDS p
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        p=$(qsh_primary)
        [ -n "$p" ] && qsh_ready_once "$p" && return 0
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

# Per-node state of a failed wait: /cluster, /patroni, pg_control_recovery(),
# the replay LSN and the last 40 pg-ha lines. Stdout, so it is in the run output.
qsh_evidence() { # <label>
    local n
    echo "[EVIDENCE] $1 ($(date -u +%H:%M:%S))"
    for n in "${NODES[@]}"; do
        echo "[EVIDENCE] $n running=$(qsh_running "$n" && echo yes || echo no) /primary $(qsh_code "$n" primary) /replica $(qsh_code "$n" replica)"
        echo "[EVIDENCE] $n /cluster $(qsh_get "$n" cluster | jq -c . 2>/dev/null)"
        echo "[EVIDENCE] $n /patroni $(qsh_get "$n" patroni | jq -c . 2>/dev/null)"
        echo "[EVIDENCE] $n pg_control_recovery() $(qsh_psql "$n" 'select row_to_json(r) from pg_control_recovery() r')"
        echo "[EVIDENCE] $n replay_lsn $(qsh_psql "$n" 'select pg_last_wal_replay_lsn()')"
        docker logs --tail 1500 "$(qsh_c "$n")" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep pg_ha | tail -n 40 | cut -c1-300 | sed "s/^/[EVIDENCE] $n log: /"
    done
}
# First line of any node's logs since the first primary was seen with a fence
# or DCS-timeout marker. Lines before that are no evidence about the new
# leader: a DCS write can time out while the restarted nodes form a Raft majority.
qsh_markers_since() { # <time>
    local n l
    for n in "${NODES[@]}"; do
        l=$(docker logs --since "$1" "$(qsh_c "$n")" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "$QSH_MARKERS" | head -n 1)
        [ -n "$l" ] && { echo "$n logged: $(echo "$l" | cut -c1-200)"; return 0; }
    done
    return 1
}
# Every running node shows sync.quorum >= len(sync.voters) (>= 1 voter): the
# vote can never pass and only a leader could relax q.
qsh_q_covers_voters() {
    local n v seen=0
    for n in "${NODES[@]}"; do
        qsh_running "$n" || continue
        v=$(qsh_get "$n" cluster | jq -r 'if (.sync.quorum != null and (.sync.voters | length) > 0 and .sync.quorum >= (.sync.voters | length)) then "y" else "n" end' 2>/dev/null)
        [ "$v" = y ] || return 1
        seen=1
    done
    [ "$seen" = 1 ]
}
# Opt-in negative control: a host stall (docker pause) QSH_FREEZE_AFTER s after
# the first primary of the attempt, for QSH_FREEZE_FOR s. Once per attempt.
qsh_freeze_once() { # <leader>
    [ -n "${QSH_FREEZE_AFTER:-}" ] && [ "$QSH_FROZEN" = 0 ] || return 0
    QSH_FROZEN=1
    local n fz=()
    log_info "FREEZE: primary $1 is up; pausing ${QSH_FREEZE_TARGET} in ${QSH_FREEZE_AFTER} s for ${QSH_FREEZE_FOR} s"
    sleep "$QSH_FREEZE_AFTER"
    for n in "${NODES[@]}"; do
        case "$QSH_FREEZE_TARGET" in
            all) fz+=("$n") ;;
            leader) [ "$n" = "$1" ] && fz+=("$n") ;;
            others) [ "$n" != "$1" ] && fz+=("$n") ;;
        esac
    done
    for n in "${fz[@]}"; do docker pause "$(qsh_c "$n")" > /dev/null; done
    log_info "FREEZE: paused ${fz[*]} at $(date -u +%H:%M:%S)"
    sleep "$QSH_FREEZE_FOR"
    for n in "${fz[@]}"; do docker unpause "$(qsh_c "$n")" > /dev/null; done
    log_info "FREEZE: unpaused at $(date -u +%H:%M:%S)"
}
# Wait for a ready cluster after the restart. On return QSH_FINAL
# holds the primary of a ready cluster. QSH_INVALID holds the reason when the
# run did not test self-healing (the new leader was fenced or lost, see the
# header); both empty after the timeout is a real heal failure. A primary seen
# and then absent on every node for two polls counts as lost.
qsh_wait_healed() { # <timeout>
    local timeout="$1" start=$SECONDS p absent=0 lost=0 mk="" tick=0
    QSH_FINAL=""; QSH_INVALID=""
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        tick=$((tick + 1))
        p=$(qsh_primary)
        if [ -n "$p" ]; then
            absent=0
            if [ "$QSH_SEEN" = 0 ]; then
                QSH_SEEN=1; QSH_SEEN_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
                qsh_freeze_once "$p"
                continue
            fi
            if qsh_ready_once "$p" > "$TMPD/ready.out"; then QSH_FINAL=$(cat "$TMPD/ready.out"); return 0; fi
        elif [ "$QSH_SEEN" = 1 ]; then
            absent=$((absent + 1))
            [ "$absent" -ge 2 ] && lost=1
            if [ "$lost" = 1 ] && qsh_q_covers_voters; then
                mk=$(qsh_markers_since "$QSH_SEEN_AT")
                QSH_INVALID="a primary was seen after the restart and lost on every node, and q >= |V| on every node (post-promote window)${mk:+; $mk}"
                return 1
            fi
        fi
        sleep 2
    done
    # Timed out. A primary that was seen and is gone, or a fence / DCS timeout
    # marker since the restart, means the run did not test self-healing.
    [ "$QSH_SEEN" = 1 ] && mk=$(qsh_markers_since "$QSH_SEEN_AT")
    if [ "$QSH_SEEN" = 1 ] && [ -z "$(qsh_primary)" ]; then
        QSH_INVALID="a primary was seen after the restart and later lost on every node${mk:+; $mk}"
    elif [ -n "$mk" ]; then
        QSH_INVALID="fence or DCS timeout after the first primary: $mk"
    fi
    return 1
}
# Before the retry of an INVALID attempt the cluster may be leaderless (that is
# what made the attempt invalid). Start and unpause every node, wait for a
# primary, and if there is none apply the operator's way out, `pg-ha-ctl
# recover`, with the command its explain prints.
qsh_unstick() {
    local n out cmd
    qsh_unpause_all; qsh_start_all
    [ -n "$(qsh_wait_primary 45)" ] && return 0
    for n in "${NODES[@]}"; do
        qsh_running "$n" || continue
        out=$(docker exec "$(qsh_c "$n")" pg-ha-ctl -e "$SCHEME://127.0.0.1:8008" --username admin --password secret recover 2>&1)
        cmd=$(echo "$out" | sed -n 's/^  pg-ha-ctl --endpoint [^ ]* \(recover --lost .*\)$/\1/p' | head -n 1)
        [ -n "$cmd" ] || continue
        log_info "no primary after the INVALID attempt: applying 'pg-ha-ctl $cmd' on $n"
        # shellcheck disable=SC2086
        docker exec "$(qsh_c "$n")" pg-ha-ctl --endpoint "$SCHEME://127.0.0.1:8008" --username admin --password secret $cmd 2>&1 | tail -n 3 | sed 's/^/[INFO] recover: /'
        return 0
    done
    log_warn "no primary after the INVALID attempt and no recover command to apply"
}
# Run a part; an INVALID attempt is retried once and its counters are dropped.
# A second INVALID is one counted failure, never a pass.
qsh_run_part() { # <function> <name>
    local fn="$1" name="$2" run=$TESTS_RUN passed=$TESTS_PASSED failed=$TESTS_FAILED reason
    QSH_INVALID=""; QSH_SEEN=0; QSH_SEEN_AT=""; QSH_FROZEN=0
    "$fn"
    [ -n "$QSH_INVALID" ] || return 0
    reason="$QSH_INVALID"
    TESTS_RUN=$run; TESTS_PASSED=$passed; TESTS_FAILED=$failed
    log_warn "INVALID ($name, attempt 1): $reason. Not a heal result; retrying once"
    qsh_unstick
    QSH_INVALID=""; QSH_SEEN=0; QSH_SEEN_AT=""; QSH_FROZEN=0
    "$fn"
    [ -n "$QSH_INVALID" ] || return 0
    reason="$QSH_INVALID"
    TESTS_RUN=$run; TESTS_PASSED=$passed; TESTS_FAILED=$failed
    log_fail "INVALID ($name, attempt 2): $reason. Self-healing was not tested; not a pass"
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    qsh_unstick   # leave a usable cluster for the next part
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
        QSH_INVALID="$prim became primary while $p and $newp were down; scenario 8 was not reached"
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
    if [ -z "$prim" ]; then qsh_evidence "(b) no primary appeared within 240 s"; return; fi
    # Within the order of one lock ttl: the two nodes need a start and a few
    # HA cycles each, so the bound is 4 x ttl (60 s with the dev ttl of 15 s).
    ttl=$(qsh_get "$prim" config | jq -r '.ttl // 30' 2>/dev/null)
    case "$ttl" in ''|*[!0-9]*) log_warn "ttl read from /config is '$ttl', using the default 30"; ttl=30 ;; esac
    qsh_check "$([ "$heal" -le $((4 * ttl)) ] && echo true || echo false)" "healed in $heal s (<= 4 x ttl = $((4 * ttl)) s)"
    QSH_SEEN=1; QSH_SEEN_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    qsh_freeze_once "$prim"
    local final
    qsh_wait_healed 300
    final="$QSH_FINAL"
    if [ -n "$QSH_INVALID" ]; then qsh_evidence "(b) INVALID: $QSH_INVALID"; return; fi
    assert_not_empty "$final" "the cluster is ready again (one primary, every standby 200 on /replica)"
    [ -n "$final" ] || qsh_evidence "(b) the cluster was not ready within 300 s"
    prim=${final:-$prim}
    qsh_psql "$prim" "select id from ql_ack" | sort > "$TMPD/rows.ids"
    missing=$(comm -23 "$TMPD/acked.ids" "$TMPD/rows.ids" | wc -l | tr -d ' ')
    assert_eq "0" "$missing" "zero of $acked acknowledged rows are missing on the primary $prim"
    local role bad=0 checked=0
    for n in "${NODES[@]}"; do
        [ "$n" = "$prim" ] && continue
        checked=$((checked + 1))
        role=$(qsh_get "$n" patroni | jq -r '.role' 2>/dev/null)
        [ "$role" = "replica" ] || { bad=$((bad + 1)); log_info "$n reports role '$role'"; }
    done
    assert_eq "0" "$bad" "every standby reports role replica"
    assert_not_empty "$([ "$checked" -gt 0 ] && echo "$checked")" "precondition: at least one standby was checked"
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
    qsh_wait_healed 300
    final="$QSH_FINAL"
    if [ -n "$QSH_INVALID" ]; then qsh_evidence "(c) INVALID: $QSH_INVALID"; return; fi
    assert_not_empty "$final" "after the return the cluster is ready again ($((SECONDS - t0)) s; /replica is 200 on every standby)"
    # The same cause already failed the ready assertion: do not count it twice.
    if [ -z "$final" ]; then qsh_evidence "(c) the cluster was not ready within 300 s"; return; fi
    local gone=0
    for n in "${NODES[@]}"; do
        [ "$n" = "$final" ] && continue
        qsh_get "$n" cluster | jq -r '.election.leaderless_stuck' 2>/dev/null | grep -q true && gone=$((gone + 1))
    done
    assert_eq "0" "$gone" "no node still reports leaderless_stuck"
}
log_section "TEST: SB-37 quorum self-heal ($QSH_NODES nodes)"
for part in $QSH_PARTS; do
    case "$part" in
        a) test_ordinary_failover_never_reports_stuck ;;
        b) qsh_run_part test_scenario_8_heals_by_itself "(b)" ;;
        c) qsh_run_part test_lost_voter_is_reported_and_heals_on_return "(c)" ;;
        *) echo "QSH_PARTS: unknown part '$part'"; exit 2 ;;
    esac
done
print_results
