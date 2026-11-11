#!/bin/bash
# tests/e2e/test_tags.sh — Node tags are published to the DCS and honoured
#
# Two replicas get tags (rendered by docker/entrypoint.sh from PG_HA_TAG_*):
#   X: nofailover + noloadbalance
#   Y: nosync
# Tests:
#   - the tags reach the DCS member records (read on another node's raft copy)
#   - own tags show in /patroni; /replica is 503 on the noloadbalance node
#   - synchronous_standby_names excludes the nosync node (sync mode, count 2)
#   - POST /switchover to the nofailover node is rejected with 409, nothing moves
#   - primary killed: the nofailover node never becomes primary, the other
#     replica does (replicatefrom is not used here: a cascading node is not
#     a sync candidate; /cluster upstream is covered by a unit test)
# The two tagged nodes are recreated one by one in place (volumes kept) and
# recreated again without tags on exit; a killed node is always restarted.
# Recreation pins each service to the image ID its container runs now (override
# file), so a moved or rebuilt image tag can never change the code under test.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"
log_section "TEST: Node Tags (DCS publication, candidate checks, sync)"

SCOPE="test-cluster"
PROJECT="${E2E_COMPOSE_PROJECT:-$(docker inspect pg-ha-node1 --format '{{ index .Config.Labels "com.docker.compose.project" }}' 2>/dev/null || true)}"
PROJECT="${PROJECT:-pg-ha}"
case "$REST_TLS" in
    tls)  export PG_HA_RESTAPI_TLS_CERT=/etc/pg-ha/tls/server.crt PG_HA_RESTAPI_TLS_REQUIRE_CLIENT_CERT=false ;;
    mtls) export PG_HA_RESTAPI_TLS_CERT=/etc/pg-ha/tls/server.crt PG_HA_RESTAPI_TLS_REQUIRE_CLIENT_CERT=true ;;
esac
COMPOSE_BASE=(docker compose -p "$PROJECT" -f "$E2E_REPO_ROOT/docker-compose.yml")
OVERRIDE_FILE=""
IMG_node1="" ; IMG_node2="" ; IMG_node3=""
TAGS_APPLIED=false
SYNC_CHANGED=false
KILLED=()
P="" ; X="" ; Y=""

name_to_url() {
    case "$1" in node1) echo "$NODE1_API" ;; node2) echo "$NODE2_API" ;; node3) echo "$NODE3_API" ;; esac
}
name_to_container() { echo "pg-ha-$1"; }
dpsql() { # <node name> <sql>
    docker exec "$(name_to_container "$1")" psql -U "$PG_USER" -d postgres -Atc "$2" 2>/dev/null || echo ""
}
# DCS member record of <member> as stored in <node>'s own raft state machine
dcs_member_record() {
    docker exec "$(name_to_container "$1")" cat /var/lib/pg-ha/raft/state_machine.json 2>/dev/null \
        | jq -r --arg k "/service/$SCOPE/members/$2" '.. | objects | select(has($k)) | .[$k].value' 2>/dev/null | head -n 1
}
# Status code of a POST (body goes to $LAST_BODY)
LAST_BODY=""
post_code() {
    local out
    out=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 15 -X POST -H "Content-Type: application/json" \
        -d "$2" -w '\n%{http_code}' "$1" 2>/dev/null || printf '\n000')
    LAST_BODY="${out%$'\n'*}"
    echo "${out##*$'\n'}"
}
# All three nodes answer, exactly one primary. Replicas are not counted by
# /replica here: a noloadbalance node answers 503 there by design.
wait_nodes_settled() {
    local timeout="${1:-180}" start=$SECONDS n primaries running
    while true; do
        primaries=0; running=0
        for n in node1 node2 node3; do
            [ "$(http_get_status "$(name_to_url $n)/primary")" = "200" ] && primaries=$((primaries + 1))
            [ "$(get_node_state "$(name_to_url $n)")" = "running" ] && running=$((running + 1))
        done
        if [ "$primaries" -eq 1 ] && [ "$running" -eq 3 ]; then return 0; fi
        if [ $((SECONDS - start)) -ge "$timeout" ]; then
            log_fail "Nodes not settled after ${timeout}s (primaries=$primaries running=$running)"
            return 1
        fi
        sleep 2
    done
}
find_primary() {
    local n
    for n in node1 node2 node3; do
        [ "$(http_get_status "$(name_to_url $n)/primary")" = "200" ] && { echo "$n"; return 0; }
    done
    echo ""
}
jget() { jq -r "$1 // empty" 2>/dev/null; }

cleanup() {
    local c
    for c in ${KILLED[@]+"${KILLED[@]}"}; do docker start "$c" > /dev/null 2>&1 || true; done
    if [ "$SYNC_CHANGED" = true ] && [ -n "$(find_primary)" ]; then
        curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -X PATCH -H "Content-Type: application/json" \
            -d '{"synchronous_mode":false}' "$(name_to_url "$(find_primary)")/config" > /dev/null 2>&1 || true
    fi
    if [ "$TAGS_APPLIED" = true ]; then
        log_info "Cleanup: recreating the tagged nodes (${X:-} ${Y:-}) without tags"
        # One at a time: recreating two of three nodes together loses raft quorum.
        local t
        write_override   # same pinned images, no tags
        for t in "$X" "$Y"; do
            [ -n "$t" ] || continue
            "${COMPOSE_BASE[@]}" -f "$OVERRIDE_FILE" up -d --no-deps --no-build --force-recreate "$t" > /dev/null 2>&1 || true
            wait_nodes_settled 240 > /dev/null 2>&1 || true
        done
        wait_ready_after_cleanup
    fi
    [ -n "$OVERRIDE_FILE" ] && rm -f "$OVERRIDE_FILE"
}
wait_ready_after_cleanup() { wait_for_cluster_ready 240 > /dev/null 2>&1 || log_warn "Cluster not fully ready after cleanup"; }
trap cleanup EXIT
# Recreate one service with the current override; compose output is shown on
# failure, and one retry covers a transient daemon error under load.
recreate_tagged() {
    local out
    out=$("${COMPOSE_BASE[@]}" -f "$OVERRIDE_FILE" up -d --no-deps --no-build --force-recreate "$1" 2>&1) && return 0
    log_warn "compose up $1 failed, retrying once: $out"
    sleep 5
    out=$("${COMPOSE_BASE[@]}" -f "$OVERRIDE_FILE" up -d --no-deps --no-build --force-recreate "$1" 2>&1) && return 0
    log_warn "compose up $1 failed again: $out"
    return 1
}

# --- Setup: tag two replicas one by one, then make the untagged node primary ---
# Recreating a node (even a replica) can move the primary for a moment: when it
# was the raft leader the primary loses its lock write once. So roles are chosen
# step by step against the CURRENT primary, and at the end the untagged third
# node is made primary with a plain switchover.
# Pins every service to the image ID of its running container; X gets
# nofailover+noloadbalance and Y nosync when given (empty = no tags).
write_override() { # [X [Y]]
    local n img
    {
        echo "services:"
        for n in node1 node2 node3; do
            eval "img=\$IMG_$n"
            echo "  $n:"
            echo "    image: $img"
            if [ "$n" = "${1:-}" ] || [ "$n" = "${2:-}" ]; then
                echo "    environment:"
                [ "$n" = "${1:-}" ] && { echo "      PG_HA_TAG_NOFAILOVER: \"true\""; echo "      PG_HA_TAG_NOLOADBALANCE: \"true\""; }
                [ "$n" = "${2:-}" ] && echo "      PG_HA_TAG_NOSYNC: \"true\""
            fi
        done
    } > "$OVERRIDE_FILE"
}
setup_tags() {
    wait_for_cluster_ready 120 || return 1
    local cur n
    cur=$(find_primary)
    assert_not_empty "$cur" "Primary exists before tagging" || return 1
    OVERRIDE_FILE=$(mktemp -t pg-ha-tags.XXXXXX)
    for n in node1 node2 node3; do
        eval "IMG_$n=\$(docker inspect \"pg-ha-$n\" --format '{{.Image}}')"
    done
    log_info "Pinned images: node1=$IMG_node1 node2=$IMG_node2 node3=$IMG_node3"
    for n in node1 node2 node3; do [ "$n" != "$cur" ] && { X="$n"; break; }; done
    write_override "$X"
    TAGS_APPLIED=true
    recreate_tagged "$X"
    assert_eq "0" "$?" "Recreated $X with nofailover,noloadbalance" || return 1
    wait_nodes_settled 240 || return 1
    cur=$(find_primary)
    for n in node1 node2 node3; do [ "$n" != "$X" ] && [ "$n" != "$cur" ] && { Y="$n"; break; }; done
    [ -n "$Y" ] || { log_fail "no node left to act as Y (primary is $cur)"; return 1; }
    write_override "$X" "$Y"
    recreate_tagged "$Y"
    assert_eq "0" "$?" "Recreated $Y with nosync" || return 1
    wait_nodes_settled 240 || return 1
    for n in node1 node2 node3; do [ "$n" != "$X" ] && [ "$n" != "$Y" ] && P="$n"; done
    cur=$(find_primary)
    log_info "X(nofailover,noloadbalance)=$X  Y(nosync)=$Y  untagged=$P  primary now=$cur"
    if [ "$cur" != "$P" ]; then
        local code
        code=$(post_code "$(name_to_url "$cur")/switchover" "{\"leader\": \"$cur\", \"candidate\": \"$P\"}")
        log_info "plain switchover $cur -> $P (untagged): HTTP $code"
        wait_for_role "$(name_to_url "$P")" primary 90 || return 1
        wait_nodes_settled 240 || return 1
    fi
    assert_eq "$P" "$(find_primary)" "Untagged node $P is primary; $X and $Y are tagged replicas"
}

# --- Tests ---
test_tags_in_dcs_and_api() {
    local rec own i
    # Raw member records, read from a node that is neither X nor Y (wait for the next heartbeat).
    for i in $(seq 1 30); do
        rec=$(dcs_member_record "$P" "$X")
        [ "$(printf '%s' "$rec" | jget .tags.nofailover)" = true ] && break
        sleep 2
    done
    assert_eq "true" "$(printf '%s' "$rec" | jget .tags.nofailover)" "DCS record of $X carries tags.nofailover (read on $P)"
    assert_eq "true" "$(printf '%s' "$rec" | jget .tags.noloadbalance)" "DCS record of $X carries tags.noloadbalance"
    for i in $(seq 1 30); do
        rec=$(dcs_member_record "$X" "$Y")
        [ "$(printf '%s' "$rec" | jget .tags.nosync)" = true ] && break
        sleep 2
    done
    assert_eq "true" "$(printf '%s' "$rec" | jget .tags.nosync)" "DCS record of $Y carries tags.nosync (read on $X)"
    # Own tags in /patroni
    own=$(http_get "$(name_to_url "$X")/patroni")
    assert_eq "true" "$(printf '%s' "$own" | jget .tags.nofailover)" "/patroni of $X reports its nofailover tag"
    own=$(http_get "$(name_to_url "$Y")/patroni")
    assert_eq "true" "$(printf '%s' "$own" | jget .tags.nosync)" "/patroni of $Y reports its nosync tag"
    # noloadbalance
    assert_eq "503" "$(http_get_status "$(name_to_url "$X")/replica")" "/replica on $X (noloadbalance) is 503"
    assert_eq "200" "$(http_get_status "$(name_to_url "$Y")/replica")" "/replica on $Y (no noloadbalance) is 200"
}

test_nosync_excluded() {
    local code names="" i streaming=""
    for i in $(seq 1 30); do
        streaming=$(dpsql "$P" "select count(*) from pg_stat_replication where state='streaming'")
        [ "$streaming" = "2" ] && break
        sleep 2
    done
    assert_eq "2" "$streaming" "Both standbys stream from $P before sync mode is enabled"
    code=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -o /dev/null -w '%{http_code}' -X PATCH \
        -H "Content-Type: application/json" -d '{"synchronous_mode":true,"synchronous_node_count":2}' \
        "$(name_to_url "$P")/config" 2>/dev/null || echo 000)
    SYNC_CHANGED=true
    assert_eq "200" "$code" "PATCH /config synchronous_mode=true, count=2"
    for i in $(seq 1 40); do
        names=$(dpsql "$P" "show synchronous_standby_names")
        [[ "$names" == *"$X"* ]] && break
        sleep 2
    done
    sleep 7   # one more HA loop: a wrong choice would have been applied by now
    names=$(dpsql "$P" "show synchronous_standby_names")
    log_info "synchronous_standby_names on $P: $names"
    assert_contains "$names" "$X" "$X is a synchronous standby candidate"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ "$names" == *"$Y"* ]]; then
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "nosync node $Y is in synchronous_standby_names ($names)"
    else
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "nosync node $Y is excluded from synchronous_standby_names"
    fi
    curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 10 -o /dev/null -X PATCH -H "Content-Type: application/json" \
        -d '{"synchronous_mode":false}' "$(name_to_url "$P")/config" 2>/dev/null
    SYNC_CHANGED=false
    sleep 10
}

test_switchover_to_nofailover_rejected() {
    local code
    code=$(post_code "$(name_to_url "$P")/switchover" "{\"leader\": \"$P\", \"candidate\": \"$X\"}")
    log_info "switchover to $X -> HTTP $code: $LAST_BODY"
    assert_eq "409" "$code" "POST /switchover to nofailover node $X is rejected with 409"
    sleep 20
    assert_eq "200" "$(http_get_status "$(name_to_url "$P")/primary")" "$P is still primary after the rejected switchover"
    assert_eq "503" "$(http_get_status "$(name_to_url "$X")/primary")" "$X did not promote after the rejected switchover"
}

test_failover_never_picks_nofailover() {
    local c i xprimary=false yprimary=false
    dpsql "$P" "create table if not exists tags_e2e(i int); insert into tags_e2e select generate_series(1,1000); select pg_switch_wal()" > /dev/null
    sleep 3
    log_info "WAL before kill: $X=$(http_get "$(name_to_url "$X")/patroni" | jq -r .wal_position) $Y=$(http_get "$(name_to_url "$Y")/patroni" | jq -r .wal_position)"
    c=$(name_to_container "$P")
    KILLED+=("$c")
    docker kill "$c" > /dev/null
    for i in $(seq 1 60); do
        [ "$(http_get_status "$(name_to_url "$X")/primary")" = "200" ] && xprimary=true
        [ "$(http_get_status "$(name_to_url "$Y")/primary")" = "200" ] && { yprimary=true; break; }
        sleep 2
    done
    if [ "$yprimary" = true ]; then
        for i in 1 2 3 4 5; do
            [ "$(http_get_status "$(name_to_url "$X")/primary")" = "200" ] && xprimary=true
            sleep 2
        done
    fi
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$yprimary" = true ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "$Y became primary after $P died"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "no promotable node became primary within 120s after $P died (election stalled)"
    fi
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$xprimary" = false ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "nofailover node $X never became primary"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "nofailover node $X became primary"
    fi
    docker start "$c" > /dev/null 2>&1 || true
    KILLED=()
    wait_nodes_settled 240 || true
}

if setup_tags; then
    test_tags_in_dcs_and_api
    test_nosync_excluded
    test_switchover_to_nofailover_rejected
    test_failover_never_picks_nofailover
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "setup failed, tag tests not run"
fi
print_results
