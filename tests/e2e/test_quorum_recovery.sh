#!/bin/bash
# tests/e2e/test_quorum_recovery.sh — SB-37 PR B: `pg-ha-ctl recover` against real
# PostgreSQL containers.
#
#   T1 refusals on a healthy cluster (nothing to do, usage errors, wrong digest,
#      no grant written) and REST authentication on GET/POST/DELETE /recovery
#      (and, with REST_TLS=mtls, no client certificate);
#   T2 pause mode refuses; a leaderless cluster that heals by itself is told so
#      (exit 3) and the apply is refused;
#   T3 (5 nodes) the primary and one voter are gone for good: explain, every
#      refusal, an existing grant (409, cancel), the real apply, time to the first
#      acknowledged write, no two nodes answer 200 on /primary, the writer's
#      acknowledged ids against the rows of the new primary, the returned lost
#      node is rewound;
#   T4a (5 nodes) race: the lost voter returns between explain and apply;
#   T4b (5 nodes) race: the candidate is killed right after the apply; the lost
#      voter's container and volumes are destroyed and it joins again as a new node.
#
# Why 5 nodes for T3/T4: the DCS is the built-in Raft over the pg-ha nodes. With
# 3 nodes, losing the primary and one voter loses the Raft majority, so no
# survivor can read or write the DCS and no recovery can be authorized. On
# 3 nodes the suite runs T1 and T2 only (a primary loss alone always heals by
# itself there). With 5 nodes the primary plus one voter may be lost.
#
# Real PostgreSQL only: the writer's acknowledged ids are compared with the rows
# of the new primary; which ids only the lost voter held is read from that voter's
# own rows before it is killed.
#
# Usage:
#   make PG_VERSION=18 up
#   SYNC_MODE=quorum ./tests/e2e/run_tests.sh --no-build quorum_recovery
# 5 nodes (docker-compose.5node.yml started by hand, REST ports 8008..8012):
#   QRC_NODES=5 ./tests/e2e/run_tests.sh --no-build --no-setup quorum_recovery
# mTLS check: start the cluster with `make up REST_TLS=mtls` and run with REST_TLS=mtls.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

QRC_NODES="${QRC_NODES:-3}"
case "$QRC_NODES" in
    3) CPFX=pg-ha-node; COMPOSE_FILE=docker-compose.yml ;;
    5) CPFX=pg-ha5-node; COMPOSE_FILE=docker-compose.5node.yml ;;
    *) echo "QRC_NODES must be 3 or 5"; exit 2 ;;
esac
ZERO_DIGEST=0000000000000000000000000000000000000000000000000000000000000000
NODES=()
for i in $(seq 1 "$QRC_NODES"); do NODES+=("node$i"); done
TMPD=$(mktemp -d)
SAMPLER_PID=""
trap 'qrc_sampler_kill; rm -rf "$TMPD"; qrc_resume > /dev/null 2>&1; qrc_start_all' EXIT

qrc_c() { echo "$CPFX${1#node}"; }
qrc_url() { echo "$SCHEME://localhost:$((8007 + ${1#node}))"; }
qrc_psql() { docker exec "$(qrc_c "$1")" psql -U "$PG_USER" -d postgres -Atc "$2" 2>/dev/null; }
qrc_code() { curl "${CURL_OPTS[@]}" -m 3 -o /dev/null -w '%{http_code}' "$(qrc_url "$1")/$2" 2>/dev/null || echo 000; }
qrc_get() { curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 5 "$(qrc_url "$1")/$2" 2>/dev/null; }
qrc_running() { [ "$(docker inspect -f '{{.State.Running}}' "$(qrc_c "$1")" 2>/dev/null)" = true ]; }
qrc_now() { perl -MTime::HiRes=time -e 'printf "%.2f\n", time'; }
qrc_check() { # <true|false> <message>
    if [ "$1" = true ]; then assert_eq "ok" "ok" "$2"; else assert_eq "ok" "failed" "$2"; fi
}
qrc_primary() {
    local n
    for n in "${NODES[@]}"; do
        qrc_running "$n" || continue
        [ "$(qrc_code "$n" primary)" = 200 ] && { echo "$n"; return 0; }
    done
    echo ""
}
qrc_running_nodes() {
    local n
    for n in "${NODES[@]}"; do qrc_running "$n" && echo "$n"; done
}
qrc_start_all() {
    local n
    for n in "${NODES[@]}"; do
        qrc_running "$n" && continue
        docker start "$(qrc_c "$n")" > /dev/null 2>&1 \
            || (cd "$E2E_REPO_ROOT" && docker compose -f "$COMPOSE_FILE" up -d --no-build "$n" > /dev/null 2>&1)
    done
}
qrc_patch_config() { # <node> <json>: HTTP code
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 15 -o /dev/null -w '%{http_code}' -X PATCH \
        -H 'Content-Type: application/json' -d "$2" "$(qrc_url "$1")/config"
}
qrc_resume() { # pause=false through any running node
    local n
    for n in "${NODES[@]}"; do
        qrc_running "$n" || continue
        [ "$(qrc_patch_config "$n" '{"pause":false}')" = 200 ] && return 0
    done
    return 1
}
# One primary, every other node 200 on /replica, quorum settled over all of them.
qrc_ready() { # [timeout]
    local timeout="${1:-300}" start=$SECONDS p n ok sync
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        p=$(qrc_primary)
        if [ -n "$p" ]; then
            ok=1
            for n in "${NODES[@]}"; do
                [ "$n" = "$p" ] && continue
                [ "$(qrc_code "$n" replica)" = 200 ] || { ok=0; break; }
            done
            if [ "$ok" = 1 ]; then
                sync=$(qrc_get "$p" cluster | jq -r --argjson want $((QRC_NODES - 1)) \
                    'if (.sync.kind == "quorum" and .sync.settled == true and ((.sync.voters | length) >= $want)) then "settled" else "waiting" end' 2>/dev/null)
                [ "$sync" = settled ] && { echo "$p"; return 0; }
            fi
        fi
        sleep 2
    done
    return 1
}
qrc_diag() { # what each node says (stderr)
    local n
    for n in $(qrc_running_nodes); do
        {
            echo "[DIAG] $n /patroni $(qrc_get "$n" patroni | jq -c '{role,state,timeline,pg_in_recovery}' 2>/dev/null) /replica $(qrc_code "$n" replica) /primary $(qrc_code "$n" primary)"
            docker logs --tail 600 "$(qrc_c "$n")" 2>&1 | grep pg_ha | grep -v "no action\|openraft\|Health probe" | tail -n 8 | cut -c1-260 | sed "s/^/[DIAG] $n: /"
        } >&2
    done
    echo "[DIAG] sync $(qrc_get "$(qrc_running_nodes | head -n1)" cluster | jq -c .sync 2>/dev/null)" >&2
}
qrc_prepare() { # a ready cluster in quorum mode, no pause, no grant; echoes the primary
    local p i
    qrc_start_all
    for i in $(seq 1 40); do p=$(qrc_primary); [ -n "$p" ] && break; sleep 2; done
    [ -n "$p" ] || return 1
    qrc_resume > /dev/null
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 15 -o /dev/null -X DELETE "$(qrc_url "$p")/recovery" 2>/dev/null
    [ "$(qrc_patch_config "$p" '{"synchronous_mode":"quorum"}')" = 200 ] || log_warn "could not PATCH synchronous_mode=quorum" >&2
    qrc_ready 360
}
# ctl runs inside a node's container (the Linux pg-ha-ctl of the image); the
# result is in QRC_OUT and QRC_RC.
QRC_OUT=""; QRC_RC=0
qrc_ctl() { # <node> <args...>
    local n="$1"; shift
    local tls=()
    case "$REST_TLS" in
        tls) tls=(--cacert /etc/pg-ha/tls/ca.crt) ;;
        mtls) tls=(--cacert /etc/pg-ha/tls/ca.crt --cert /etc/pg-ha/tls/client.crt --key /etc/pg-ha/tls/client.key) ;;
    esac
    QRC_OUT=$(docker exec "$(qrc_c "$n")" pg-ha-ctl --endpoint "$SCHEME://$n:8008" \
        --username admin --password secret ${tls[@]+"${tls[@]}"} "$@" 2>&1)
    QRC_RC=$?
}
qrc_show() { echo "$QRC_OUT" | sed 's/^/    | /'; echo "    | [exit $QRC_RC]"; }
qrc_digest_of() { echo "$QRC_OUT" | sed -n 's/.*--digest \([0-9a-f]\{64\}\).*/\1/p' | head -n1; }
qrc_candidate_of() { echo "$QRC_OUT" | sed -n 's/^  candidate: *//p' | head -n1; }
# Poll the explain on <node> until it exits with <want> (QRC_OUT keeps that answer).
qrc_wait_explain() { # <node> <want_rc> <timeout> [interval] [pattern the output must contain]
    local n="$1" want="$2" timeout="$3" step="${4:-2}" pat="${5:-}" start=$SECONDS
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        qrc_ctl "$n" recover
        [ "$QRC_RC" = "$want" ] && { [ -z "$pat" ] || echo "$QRC_OUT" | grep -q "$pat"; } && return 0
        sleep "$step"
    done
    return 1
}
qrc_recovery_key() { # <node>: the /cluster recovery object as one JSON line
    qrc_get "$1" cluster | jq -c '.recovery' 2>/dev/null
}
qrc_no_grant_anywhere() {
    local n bad=0
    for n in $(qrc_running_nodes); do
        [ "$(qrc_recovery_key "$n")" = null ] || bad=$((bad + 1))
    done
    echo "$bad"
}
# The writer runs inside the primary's container; every id psql acknowledged is
# appended to /tmp/acked (the file survives `docker kill`).
qrc_start_writer() { # <node>
    docker exec "$(qrc_c "$1")" sh -c 'rm -f /tmp/acked; : > /tmp/acked'
    qrc_psql "$1" 'drop table if exists ql_ack; create table ql_ack(id serial primary key, t timestamptz default now())' > /dev/null
    docker exec -d "$(qrc_c "$1")" sh -c 'while true; do id=$(psql -U postgres -d postgres -qAtc "insert into ql_ack default values returning id" 2>/dev/null | head -n1); [ -n "$id" ] && echo "$id" >> /tmp/acked; sleep 0.05; done'
}
qrc_ids() { # <node> <outfile>: the ids a node holds, sorted
    qrc_psql "$1" "select id from ql_ack" 2> /dev/null | sort > "$2"
}
# Sampler: every 0.2 s, how many running nodes answer 200 on /primary.
qrc_sampler_start() { # <name>
    local log="$TMPD/sample-$1.log" stop="$TMPD/stop-$1"
    rm -f "$stop"; : > "$log"
    (
        while [ ! -f "$stop" ]; do
            cnt=0; who=""
            for n in "${NODES[@]}"; do
                c=$(curl "${CURL_OPTS[@]}" -m 1 -o /dev/null -w '%{http_code}' "$(qrc_url "$n")/primary" 2>/dev/null)
                [ "$c" = 200 ] && { cnt=$((cnt + 1)); who="$who $n"; }
            done
            echo "$(qrc_now) $cnt$who" >> "$log"
            sleep 0.2
        done
    ) &
    SAMPLER_PID=$!
}
qrc_sampler_kill() { [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null; SAMPLER_PID=""; }
qrc_sampler_stop() { # <name>: asserts at most one 200 at every sample
    local log="$TMPD/sample-$1.log"
    touch "$TMPD/stop-$1"
    [ -n "$SAMPLER_PID" ] && wait "$SAMPLER_PID" 2>/dev/null
    SAMPLER_PID=""
    local samples two with1 with0
    samples=$(wc -l < "$log" | tr -d ' ')
    two=$(awk '$2 > 1' "$log" | wc -l | tr -d ' ')
    with1=$(awk '$2 == 1' "$log" | wc -l | tr -d ' ')
    with0=$(awk '$2 == 0' "$log" | wc -l | tr -d ' ')
    log_info "$1: $samples samples of /primary every 0.2 s on all nodes: $with1 with one 200, $with0 with none, $two with more than one"
    awk '$2 > 1' "$log" | head -n 3 | sed 's/^/[INFO] two 200s: /'
    assert_eq "0" "$two" "$1: never two nodes answering 200 on /primary"
    qrc_check "$([ "$samples" -ge 25 ] && echo true || echo false)" "$1: sampled /primary $samples times (>= 25)"
}
qrc_wait_primary() { # <timeout> [exclude...]
    local timeout="$1" start=$SECONDS n skip x
    shift
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        for n in "${NODES[@]}"; do
            skip=0
            for x in "$@"; do [ "$x" = "$n" ] && skip=1; done
            [ "$skip" = 1 ] && continue
            qrc_running "$n" || continue
            [ "$(qrc_code "$n" primary)" = 200 ] && { echo "$n"; return 0; }
        done
        sleep 0.4
    done
    echo ""
    return 1
}
qrc_last_voter() { # <exclude>: the highest-numbered node that is not <exclude>
    local n r=""
    for n in "${NODES[@]}"; do [ "$n" != "$1" ] && r=$n; done
    echo "$r"
}
# Compare the writer's acked ids with the rows of <primary> given what the
# lost voter alone held. Sets nothing, asserts.
#   $1 label  $2 acked.ids  $3 rows.ids  $4 only-lost.ids  $5 survivors-union.ids
qrc_assert_acked() {
    local label="$1" acked="$2" rows="$3" onlylost="$4" surv="$5"
    local nacked missing unexplained undeclared
    nacked=$(wc -l < "$acked" | tr -d ' ')
    comm -23 "$acked" "$rows" > "$TMPD/missing.ids"
    missing=$(wc -l < "$TMPD/missing.ids" | tr -d ' ')
    unexplained=$(comm -23 "$TMPD/missing.ids" "$onlylost" | wc -l | tr -d ' ')
    undeclared=$(comm -12 "$acked" "$surv" | comm -23 - "$rows" | wc -l | tr -d ' ')
    log_info "$label: $nacked acked, $missing missing on the new primary, $(wc -l < "$onlylost" | tr -d ' ') ids were held only by the lost voter"
    qrc_check "$([ "$nacked" -gt 0 ] && echo true || echo false)" "$label: the writer acknowledged $nacked rows before the kill"
    assert_eq "0" "$unexplained" "$label: every acked id is present except those only the lost voter held"
    assert_eq "0" "$undeclared" "$label: every acked id that any surviving node held is present"
}

# ── T1: refusals on a healthy cluster, authentication ──
test_refusals_and_auth() {
    log_section "T1 ($QRC_NODES nodes): healthy cluster refusals and authentication"
    local p s code
    p=$(qrc_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    for s in "${NODES[@]}"; do [ "$s" != "$p" ] && break; done
    qrc_ctl "$s" recover; qrc_show
    assert_eq "0" "$QRC_RC" "healthy cluster: recover exits 0"
    assert_contains "$QRC_OUT" "nothing to do" "healthy cluster: the explain says nothing to do"
    qrc_ctl "$s" recover --lost "$p"; qrc_show
    assert_eq "2" "$QRC_RC" "recover --lost without --digest is a usage error (exit 2)"
    qrc_ctl "$s" recover --cancel --lost "$p" --digest "$ZERO_DIGEST"; qrc_show
    assert_eq "2" "$QRC_RC" "recover --cancel with --lost is a usage error (exit 2)"
    qrc_ctl "$s" recover --lost "$p" --digest "$ZERO_DIGEST" --no-wait; qrc_show
    assert_eq "4" "$QRC_RC" "healthy cluster: applying a made-up digest is refused (exit 4)"
    assert_contains "$QRC_OUT" "leader lock is held" "healthy cluster: the refusal names the held lock"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "healthy cluster: no grant was written"
    qrc_ctl "$s" recover --cancel; qrc_show
    assert_contains "$QRC_OUT" "no quorum recovery grant" "cancel without a grant says so"
    assert_eq "1" "$QRC_RC" "cancel without a grant is not a success (exit 1)"
    # REST authentication on the three methods.
    local body='{"lost":["'"$p"'"],"digest":"'"$ZERO_DIGEST"'"}' m
    for m in GET POST DELETE; do
        code=$(curl "${CURL_OPTS[@]}" -m 8 -o /dev/null -w '%{http_code}' -X "$m" -H 'Content-Type: application/json' \
            -d "$body" "$(qrc_url "$s")/recovery")
        assert_eq "401" "$code" "$m /recovery without credentials is rejected (401)"
        code=$(curl "${CURL_OPTS[@]}" -u admin:wrong -m 8 -o /dev/null -w '%{http_code}' -X "$m" -H 'Content-Type: application/json' \
            -d "$body" "$(qrc_url "$s")/recovery")
        assert_eq "401" "$code" "$m /recovery with a wrong password is rejected (401)"
    done
    code=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 8 -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        -d '{"lost":["x"],"digest":"abc","extra":1}' "$(qrc_url "$s")/recovery")
    assert_eq "400" "$code" "POST /recovery with an unknown field and a short digest is a 400"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "no grant after the rejected requests"
    if [ "$REST_TLS" = mtls ]; then
        for m in GET POST DELETE; do
            code=$(curl -s --cacert "$E2E_REPO_ROOT/docker/tls/ca.crt" "${REST_AUTH[@]}" -m 8 -o /dev/null -w '%{http_code}' -X "$m" \
                -H 'Content-Type: application/json' -d "$body" "$(qrc_url "$s")/recovery")
            assert_eq "000" "$code" "mTLS: $m /recovery without a client certificate is refused at the handshake"
        done
    else
        log_info "REST_TLS=$REST_TLS: the client-certificate check runs with REST_TLS=mtls"
    fi
}

# ── T2: pause refuses; a cluster that heals by itself says so ──
test_pause_and_will_heal() {
    log_section "T2 ($QRC_NODES nodes): pause refuses, a self-healing cluster is told to wait"
    local p s code t0 seen3=0 seen_apply3="" seen_explain="" newp
    p=$(qrc_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    for s in "${NODES[@]}"; do [ "$s" != "$p" ] && break; done
    code=$(qrc_patch_config "$s" '{"pause":true}')
    assert_eq "200" "$code" "pause mode on"
    docker kill "$(qrc_c "$p")" > /dev/null
    log_info "paused, then killed the primary $p"
    if qrc_wait_explain "$s" 4 90 2; then
        qrc_show
        assert_contains "$QRC_OUT" "pause mode is on" "paused leaderless cluster: the explain refuses with the pause reason"
        assert_eq "4" "$QRC_RC" "paused leaderless cluster: exit 4"
    else
        qrc_show
        assert_eq "4" "$QRC_RC" "paused leaderless cluster: the explain exits 4 once the lock expired"
    fi
    qrc_ctl "$s" recover --lost "$(qrc_last_voter "$p")" --digest "$ZERO_DIGEST" --no-wait
    assert_eq "4" "$QRC_RC" "paused: applying is refused (exit 4)"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "paused: no grant was written"
    assert_eq "" "$(qrc_wait_primary 3 "$p")" "paused: no node became primary"
    # Resume: the survivors elect by themselves.
    code=$(qrc_patch_config "$s" '{"pause":false}')
    assert_eq "200" "$code" "pause mode off"
    newp=$(qrc_wait_primary 90 "$p")
    assert_not_empty "$newp" "after the resume a primary appeared with no command"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "no grant was written"
    docker start "$(qrc_c "$p")" > /dev/null
    p=$(qrc_ready 300)
    assert_not_empty "$p" "the old primary returned and the cluster is ready (primary $p)"
    [ -n "$p" ] || return
    # Without pause: the window between the lock expiring and the election is
    # short, so the explain is polled right from the kill.
    for s in "${NODES[@]}"; do [ "$s" != "$p" ] && break; done
    docker kill "$(qrc_c "$p")" > /dev/null
    log_info "killed the primary $p; polling the explain on $s every 0.1 s"
    t0=$SECONDS
    while [ $((SECONDS - t0)) -lt 90 ]; do
        qrc_ctl "$s" recover
        if [ "$QRC_RC" = 3 ]; then
            seen3=1
            seen_explain="$QRC_OUT"
            qrc_show
            qrc_ctl "$s" recover --lost "$(qrc_last_voter "$p")" --digest "$ZERO_DIGEST" --no-wait
            seen_apply3="$QRC_RC $QRC_OUT"
            break
        fi
        [ "$QRC_RC" != 0 ] && { qrc_show; break; }
        sleep 0.1
    done
    qrc_check "$([ "$seen3" = 1 ] && echo true || echo false)" "leaderless but electable: the explain exited 3 (will heal by itself)"
    [ "$seen3" = 1 ] && {
        assert_contains "$seen_explain" "will heal by itself" "the exit-3 explain says it heals by itself"
        assert_eq "4" "${seen_apply3%% *}" "the apply is refused while the cluster heals by itself (exit 4)"
        # Either the vote can still pass ("heals by itself") or the new leader already holds the lock.
        assert_contains "$seen_apply3" "nothing to accept" "the refusal says there is nothing to accept"
        log_info "apply during the window: ${seen_apply3#* }"
    }
    newp=$(qrc_wait_primary 90 "$p")
    assert_not_empty "$newp" "a primary appeared with no command"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "no grant was written"
    docker start "$(qrc_c "$p")" > /dev/null
    qrc_ready 300 > /dev/null
}

# ── T3: the primary and one voter are gone for good ──
test_destroyed_voter_recovery() {
    log_section "T3 ($QRC_NODES nodes): the primary and one voter are lost, recover with the named loss"
    local p L n cand digest D2 t0 t_apply first_ack newp
    p=$(qrc_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    L=$(qrc_last_voter "$p")
    local survs=()
    for n in "${NODES[@]}"; do [ "$n" != "$p" ] && [ "$n" != "$L" ] && survs+=("$n"); done
    qrc_start_writer "$p"
    sleep 6
    qrc_sampler_start t3
    docker kill "$(qrc_c "$p")" > /dev/null
    log_info "killed the primary $p; the lost voter will be $L; survivors ${survs[*]}"
    sleep 4
    # Static WAL now (the primary is dead and the lock has not expired): read what each voter holds.
    for n in "${survs[@]}" "$L"; do qrc_ids "$n" "$TMPD/hold-$n.ids"; done
    : > "$TMPD/surv.all"
    for n in "${survs[@]}"; do cat "$TMPD/hold-$n.ids" >> "$TMPD/surv.all"; done
    sort -u "$TMPD/surv.all" > "$TMPD/surv.ids"
    comm -23 "$TMPD/hold-$L.ids" "$TMPD/surv.ids" > "$TMPD/onlylost.ids"
    for n in "${NODES[@]}"; do [ -f "$TMPD/hold-$n.ids" ] && log_info "$n holds $(wc -l < "$TMPD/hold-$n.ids" | tr -d ' ') ids"; done
    docker kill "$(qrc_c "$L")" > /dev/null
    docker cp "$(qrc_c "$p"):/tmp/acked" "$TMPD/acked" > /dev/null 2>&1
    grep '^[0-9]' "$TMPD/acked" | sort -u > "$TMPD/acked.ids"
    # The explain, once the old lock has expired.
    local s=${survs[0]}
    if qrc_wait_explain "$s" 4 120 2; then
        qrc_show
        assert_eq "4" "$QRC_RC" "the explain needs acceptance (exit 4)"
    else
        qrc_show
        assert_eq "4" "$QRC_RC" "the explain needs acceptance (exit 4) within 120 s"
        return
    fi
    assert_contains "$QRC_OUT" "needs acceptance" "the explain says it needs acceptance"
    assert_contains "$QRC_OUT" "lost: $L" "the explain names $L as the voter to declare lost"
    digest=$(qrc_digest_of)
    cand=$(qrc_candidate_of)
    assert_not_empty "$digest" "the explain prints a 64-hex digest ($digest)"
    assert_not_empty "$cand" "the explain names the candidate ($cand)"
    assert_contains "$QRC_OUT" "recover --lost $L --digest $digest" "the explain prints the exact apply command"
    # Refusals; none writes a grant.
    local flipped
    flipped="${digest:0:63}$([ "${digest:63:1}" = 0 ] && echo 1 || echo 0)"
    qrc_ctl "$s" recover --lost "$L" --digest "$flipped" --no-wait
    assert_eq "4" "$QRC_RC" "a digest that differs by one character is refused (exit 4)"
    assert_contains "$QRC_OUT" "digest differs" "the refusal says the digest differs"
    local other
    for n in "${survs[@]}"; do [ "$n" != "$cand" ] && other=$n && break; done
    qrc_ctl "$s" recover --lost "$L,$other" --digest "$digest" --no-wait
    assert_eq "4" "$QRC_RC" "naming a voter that is up and answering as lost is refused (exit 4)"
    assert_contains "$QRC_OUT" "not what was named" "the refusal says the named set is not the lost set"
    qrc_ctl "$s" recover --lost "$other" --digest "$digest" --no-wait
    assert_eq "4" "$QRC_RC" "naming only a live voter, not the lost one, is refused (exit 4)"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "no grant after the refusals"
    # The grant is written through a node that is not the candidate: the
    # candidate's own HA loop would otherwise use it in the same cycle.
    for n in "${survs[@]}"; do [ "$n" != "$cand" ] && { s=$n; break; }; done
    log_info "grant and pause go through $s (candidate $cand)"
    # An existing grant. Pause mode stops the candidate from using it, so the
    # POST and the pause go out back to back from the host (about 50 ms apart,
    # against a 5 s HA cycle).
    local post_code pause_code
    post_code=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -m 10 -o "$TMPD/post.out" -w '%{http_code}' -X POST \
        -H 'Content-Type: application/json' -d '{"lost":["'"$L"'"],"digest":"'"$digest"'"}' "$(qrc_url "$s")/recovery")
    pause_code=$(qrc_patch_config "$s" '{"pause":true}')
    log_info "POST /recovery: HTTP $post_code $(cat "$TMPD/post.out" | cut -c1-200)"
    assert_eq "200" "$post_code" "the apply with the printed digest is accepted (HTTP 200)"
    assert_eq "200" "$pause_code" "paused right after the grant so that it stays unused"
    assert_eq "" "$(qrc_wait_primary 1 "$p" "$L")" "the grant is not used yet (paused): no primary"
    qrc_ctl "$s" recover --lost "$L" --digest "$digest" --no-wait
    assert_eq "4" "$QRC_RC" "a second apply is refused while a grant exists (exit 4)"
    assert_contains "$QRC_OUT" "already authorized" "the second apply says a recovery is already authorized"
    local key i
    for i in 1 2 3 4 5 6 7 8; do
        key=$(qrc_recovery_key "$s")
        [ "$key" != null ] && [ -n "$key" ] && break
        sleep 1
    done
    log_info "/cluster recovery: $key"
    assert_eq "$cand" "$(echo "$key" | jq -r '.candidate' 2>/dev/null)" "/cluster shows the grant's candidate"
    assert_eq "$L" "$(echo "$key" | jq -r '.lost[0]' 2>/dev/null)" "/cluster shows the grant's lost voter"
    qrc_check "$([ "$(echo "$key" | jq -r '.expires_in_s' 2>/dev/null)" -gt 0 ] 2> /dev/null && echo true || echo false)" "/cluster shows a positive expires_in_s"
    qrc_ctl "$s" recover; qrc_show
    qrc_ctl "$s" recover --cancel; qrc_show
    assert_eq "0" "$QRC_RC" "recover --cancel removes the unused grant (exit 0)"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "the grant is gone on every node after the cancel"
    [ "$(qrc_patch_config "$s" '{"pause":false}')" = 200 ] && log_info "resumed"
    qrc_wait_explain "$s" 4 60 1 "needs acceptance"
    D2=$(qrc_digest_of)
    assert_eq "$digest" "$D2" "the digest is stable: the same state gives the same digest"
    # The real apply and the time to the first acknowledged write.
    t_apply=$(qrc_now)
    qrc_ctl "$s" recover --lost "$L" --digest "$digest"; qrc_show
    assert_eq "0" "$QRC_RC" "the apply promotes the candidate and the ctl waits for it (exit 0)"
    assert_contains "$QRC_OUT" "$cand is the primary" "the ctl reports $cand as the primary"
    newp=$(qrc_wait_primary 30 "$p" "$L")
    assert_eq "$cand" "$newp" "the node answering 200 on /primary is the named candidate"
    first_ack=""
    local i
    for i in $(seq 1 100); do
        if docker exec "$(qrc_c "$newp")" timeout 15 psql -U postgres -d postgres -qAtc \
            "create table if not exists ql_new(id int); insert into ql_new values (1)" > /dev/null 2>&1; then
            first_ack=$(echo "$(qrc_now) - $t_apply" | bc)
            break
        fi
        sleep 0.2
    done
    assert_not_empty "$first_ack" "the first write on the new primary was acknowledged (not blocked)"
    log_info "TIME to first acknowledged write: ${first_ack:-n/a} s after the apply command started"
    # Data.
    qrc_ids "$newp" "$TMPD/rows.ids"
    qrc_assert_acked "T3" "$TMPD/acked.ids" "$TMPD/rows.ids" "$TMPD/onlylost.ids" "$TMPD/surv.ids"
    # Observability.
    sleep 3
    assert_eq "0" "$(qrc_no_grant_anywhere)" "the grant is consumed after the lock acquisition"
    local hist
    hist=$(curl "${CURL_OPTS[@]}" -m 5 "$(qrc_url "$newp")/history" 2>/dev/null | jq -c '[.[] | select(.event_type == "quorum_recovery" or .type == "quorum_recovery" or .event == "quorum_recovery")]' 2>/dev/null)
    log_info "/history quorum_recovery entries: $hist"
    assert_contains "$(curl "${CURL_OPTS[@]}" -m 5 "$(qrc_url "$newp")/history" 2>/dev/null)" "quorum_recovery" "/history records a quorum_recovery event"
    assert_contains "$(curl "${CURL_OPTS[@]}" -m 5 "$(qrc_url "$newp")/history" 2>/dev/null)" "accepted loss of $L" "/history names the accepted loss"
    assert_contains "$(docker logs "$(qrc_c "$newp")" 2>&1 | grep -c "won the vote under the operator's grant")" "1" "the candidate logged that it won the vote under the grant"
    assert_contains "$(docker logs "$(qrc_c "$s")" 2>&1 | grep -c "authorized by the operator")" "[1-9]" "the REST node logged the authorization"
    qrc_sampler_stop t3
    # The lost voter and the old primary return: rewound, the rows only they held are gone.
    docker start "$(qrc_c "$L")" "$(qrc_c "$p")" > /dev/null
    local final
    final=$(qrc_ready 420)
    [ -n "$final" ] || qrc_diag
    assert_not_empty "$final" "after the return the cluster is ready (one primary, every standby 200 on /replica)"
    [ -n "$final" ] || final=$(qrc_primary)
    sleep 5
    qrc_ids "$final" "$TMPD/rows-final.ids"
    qrc_ids "$L" "$TMPD/rows-L.ids"
    log_info "returned $L holds $(wc -l < "$TMPD/rows-L.ids" | tr -d ' ') ids, the primary $final $(wc -l < "$TMPD/rows-final.ids" | tr -d ' ')"
    assert_eq "0" "$(diff "$TMPD/rows-L.ids" "$TMPD/rows-final.ids" | wc -l | tr -d ' ')" "the returned $L holds exactly the primary's rows"
    assert_eq "0" "$(comm -12 "$TMPD/onlylost.ids" "$TMPD/rows-L.ids" | comm -23 - "$TMPD/rows-final.ids" | wc -l | tr -d ' ')" "the declared rows only $L held are gone on $L"
    assert_eq "streaming" "$(qrc_psql "$final" "select state from pg_stat_replication where application_name = '$L'")" "$L streams from the primary again"
}

# ── T4a: the lost voter returns between explain and apply ──
test_lost_voter_returns_before_apply() {
    log_section "T4a ($QRC_NODES nodes): the lost voter returns between explain and apply"
    local p L n cand digest s
    p=$(qrc_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    L=$(qrc_last_voter "$p")
    local survs=()
    for n in "${NODES[@]}"; do [ "$n" != "$p" ] && [ "$n" != "$L" ] && survs+=("$n"); done
    s=${survs[0]}
    qrc_start_writer "$p"
    sleep 6
    qrc_sampler_start t4a
    docker kill "$(qrc_c "$p")" > /dev/null
    sleep 3
    for n in "${survs[@]}" "$L"; do qrc_ids "$n" "$TMPD/a-hold-$n.ids"; done
    docker kill "$(qrc_c "$L")" > /dev/null
    docker cp "$(qrc_c "$p"):/tmp/acked" "$TMPD/a-acked" > /dev/null 2>&1
    grep '^[0-9]' "$TMPD/a-acked" | sort -u > "$TMPD/a-acked.ids"
    qrc_wait_explain "$s" 4 120 2
    qrc_show
    assert_eq "4" "$QRC_RC" "the explain needs acceptance (exit 4)"
    digest=$(qrc_digest_of)
    cand=$(qrc_candidate_of)
    docker start "$(qrc_c "$L")" > /dev/null
    log_info "$L is starting again; waiting until the explain no longer asks for $L to be declared lost"
    local start=$SECONDS gone=0
    while [ $((SECONDS - start)) -lt 150 ]; do
        qrc_ctl "$s" recover
        if [ "$QRC_RC" != 4 ] || ! echo "$QRC_OUT" | grep -q "lost: $L"; then gone=1; break; fi
        sleep 1
    done
    qrc_check "$([ "$gone" = 1 ] && echo true || echo false)" "the explain stopped proposing $L as lost after it returned ($((SECONDS - start)) s)"
    qrc_show
    qrc_ctl "$s" recover --lost "$L" --digest "$digest" --no-wait; qrc_show
    assert_eq "4" "$QRC_RC" "the apply with the old digest is refused after the lost voter returned (exit 4)"
    assert_eq "0" "$(qrc_no_grant_anywhere)" "no grant was written"
    local newp
    newp=$(qrc_wait_primary 150 "$p")
    assert_not_empty "$newp" "a primary appeared with no recovery command ($newp)"
    qrc_sampler_stop t4a
    local survs_all="$TMPD/a-surv.all"
    : > "$survs_all"
    for n in "${survs[@]}" "$L"; do cat "$TMPD/a-hold-$n.ids" >> "$survs_all"; done
    sort -u "$survs_all" > "$TMPD/a-all.ids"
    : > "$TMPD/a-none.ids"
    qrc_ids "$newp" "$TMPD/a-rows.ids"
    qrc_assert_acked "T4a" "$TMPD/a-acked.ids" "$TMPD/a-rows.ids" "$TMPD/a-none.ids" "$TMPD/a-all.ids"
    docker start "$(qrc_c "$p")" > /dev/null
    qrc_ready 420 > /dev/null
}

# ── T4b: the candidate is killed right after the apply; the lost voter is destroyed ──
test_candidate_killed_after_apply() {
    log_section "T4b ($QRC_NODES nodes): kill the candidate after the apply; the lost voter is destroyed and rejoins as a new node"
    local p L n cand digest s vols v
    p=$(qrc_prepare)
    assert_not_empty "$p" "cluster ready in quorum mode (primary $p)"
    [ -n "$p" ] || return
    L=$(qrc_last_voter "$p")
    local survs=()
    for n in "${NODES[@]}"; do [ "$n" != "$p" ] && [ "$n" != "$L" ] && survs+=("$n"); done
    s=${survs[0]}
    qrc_start_writer "$p"
    sleep 6
    qrc_sampler_start t4b
    docker kill "$(qrc_c "$p")" > /dev/null
    sleep 4
    for n in "${survs[@]}" "$L"; do qrc_ids "$n" "$TMPD/b-hold-$n.ids"; done
    docker cp "$(qrc_c "$p"):/tmp/acked" "$TMPD/b-acked" > /dev/null 2>&1
    grep '^[0-9]' "$TMPD/b-acked" | sort -u > "$TMPD/b-acked.ids"
    vols=$(docker inspect -f '{{range .Mounts}}{{if .Name}}{{.Name}} {{end}}{{end}}' "$(qrc_c "$L")")
    docker rm -f "$(qrc_c "$L")" > /dev/null
    for v in $vols; do docker volume rm "$v" > /dev/null 2>&1; done
    log_info "destroyed $L (container and volumes: $vols)"
    qrc_wait_explain "$s" 4 120 2
    qrc_show
    assert_eq "4" "$QRC_RC" "the explain needs acceptance (exit 4)"
    digest=$(qrc_digest_of)
    cand=$(qrc_candidate_of)
    assert_not_empty "$digest" "the explain prints a digest"
    qrc_ctl "$s" recover --lost "$L" --digest "$digest" --no-wait
    assert_eq "0" "$QRC_RC" "the apply is accepted (exit 0)"
    docker kill "$(qrc_c "$cand")" > /dev/null
    log_info "killed the candidate $cand right after the apply"
    # Three of five Raft members are gone: nobody can take the lock.
    sleep 20
    local anyp
    anyp=$(qrc_wait_primary 5 "$p" "$L" "$cand")
    log_info "primary among the survivors while the candidate is down: ${anyp:-none}"
    docker start "$(qrc_c "$cand")" > /dev/null
    local newp
    newp=$(qrc_wait_primary 90 "$p" "$L")
    if [ -z "$newp" ]; then
        log_info "no primary after the candidate returned: running the documented flow again"
        qrc_wait_explain "$s" 4 90 2
        qrc_show
        digest=$(qrc_digest_of)
        qrc_ctl "$s" recover --lost "$L" --digest "$digest"; qrc_show
        assert_eq "0" "$QRC_RC" "the second recovery succeeds (exit 0)"
        newp=$(qrc_wait_primary 60 "$p" "$L")
    else
        log_info "a primary ($newp) appeared on its own after the candidate returned"
    fi
    assert_not_empty "$newp" "a primary exists after the candidate returned"
    qrc_sampler_stop t4b
    : > "$TMPD/b-surv.all"
    for n in "${survs[@]}"; do cat "$TMPD/b-hold-$n.ids" >> "$TMPD/b-surv.all"; done
    sort -u "$TMPD/b-surv.all" > "$TMPD/b-surv.ids"
    comm -23 "$TMPD/b-hold-$L.ids" "$TMPD/b-surv.ids" > "$TMPD/b-onlylost.ids"
    qrc_ids "$newp" "$TMPD/b-rows.ids"
    qrc_assert_acked "T4b" "$TMPD/b-acked.ids" "$TMPD/b-rows.ids" "$TMPD/b-onlylost.ids" "$TMPD/b-surv.ids"
    # The destroyed voter comes back as a new node.
    (cd "$E2E_REPO_ROOT" && docker compose -f "$COMPOSE_FILE" up -d --no-build "$L" > /dev/null 2>&1)
    docker start "$(qrc_c "$p")" > /dev/null 2>&1
    local final
    final=$(qrc_ready 480)
    assert_not_empty "$final" "the destroyed $L and the old primary $p joined again (one primary, every standby 200)"
    sleep 5
    qrc_ids "$final" "$TMPD/b-final.ids"
    qrc_ids "$L" "$TMPD/b-L.ids"
    assert_eq "0" "$(diff "$TMPD/b-L.ids" "$TMPD/b-final.ids" | wc -l | tr -d ' ')" "the rebuilt $L holds exactly the primary's rows"
    qrc_ids "$p" "$TMPD/b-p.ids"
    assert_eq "0" "$(diff "$TMPD/b-p.ids" "$TMPD/b-final.ids" | wc -l | tr -d ' ')" "the old primary $p was rewound to exactly the primary's rows"
}

QRC_TESTS="${QRC_TESTS:-t1 t2 t3 t4a t4b}"
qrc_wants() { case " $QRC_TESTS " in *" $1 "*) return 0 ;; esac; return 1; }
log_section "TEST: SB-37 quorum recovery ($QRC_NODES nodes, REST_TLS=$REST_TLS, tests: $QRC_TESTS)"
qrc_wants t1 && test_refusals_and_auth
qrc_wants t2 && test_pause_and_will_heal
if [ "$QRC_NODES" = 5 ]; then
    qrc_wants t3 && test_destroyed_voter_recovery
    qrc_wants t4a && test_lost_voter_returns_before_apply
    qrc_wants t4b && test_candidate_killed_after_apply
else
    log_info "3 nodes: losing the primary and a voter loses the Raft majority, so T3/T4 need QRC_NODES=5"
fi
print_results
