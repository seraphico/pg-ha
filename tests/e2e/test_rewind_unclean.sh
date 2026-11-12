#!/bin/bash
# tests/e2e/test_rewind_unclean.sh — SB-28: an ahead replica that is killed
# uncleanly must rejoin by itself (no pg-ha.rewind_failed, no operator action)
#
# Scenario (the SB-28 investigation's scenario e):
#   - node3 is tagged nofailover (PG_HA_TAG_NOFAILOVER, rendered by
#     docker/entrypoint.sh), so it can never be promoted. It is recreated in
#     place (volume kept) with the tag; the other two nodes are not touched.
#   - the other replica (B) gets its walreceiver SIGSTOPped, so it lags; rows
#     are written on the primary (P) and node3 receives them, B does not.
#   - P is `docker kill`ed; B takes over (timeline 2). node3 is now AHEAD of the
#     new primary and its pg_control says "in archive recovery" (checked).
#   - P is started again (it rejoins by itself), keeping 2 of 3 raft voters up.
#   - node3 is SIGKILLed (`docker kill`) and started again. Its restart fails
#     ("requested timeline 2 does not contain minimum recovery point"), pg-ha
#     rejoins it with pg_rewind. On main, pg_rewind dies with "standby mode is
#     not supported by single-user servers" and the node stays Crashed (held).
#   Expected (fixed): node3 ends `running` as a streaming replica on the new
#   timeline, there is no pg-ha.rewind_failed, and its row count equals the new
#   primary's (the unreplicated rows are discarded).
#   - control: a clean `docker stop -t 120` + start of node3 also ends as a
#     running streaming replica with equal row counts.
#
# The expected result is a FAIL on an image built from main and a PASS on the
# fixed image. Always check which image ran: the pinned image IDs are printed.
#
# Run against an already running 3-node dev cluster (one docker task at a time,
# see .agents/CONVENTIONS.md section 7). To keep the project name unique, start
# the cluster under its own compose project, with the images pinned by ID, and
# pass that project:
#   E2E_COMPOSE_PROJECT=sb28e2e bash tests/e2e/test_rewind_unclean.sh
# Recreation pins each service to the image ID its container runs now (override
# file), so a moved or rebuilt image tag can never change the code under test.
# Killed containers are always started again; node3 is recreated without the
# tag on exit.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"
log_section "TEST: SB-28 unclean shutdown of an ahead replica (pg_rewind)"

PROJECT="${E2E_COMPOSE_PROJECT:-$(docker inspect pg-ha-node1 --format '{{ index .Config.Labels "com.docker.compose.project" }}' 2>/dev/null || true)}"
PROJECT="${PROJECT:-pg-ha}"
case "$REST_TLS" in
    tls)  export PG_HA_RESTAPI_TLS_CERT=/etc/pg-ha/tls/server.crt PG_HA_RESTAPI_TLS_REQUIRE_CLIENT_CERT=false ;;
    mtls) export PG_HA_RESTAPI_TLS_CERT=/etc/pg-ha/tls/server.crt PG_HA_RESTAPI_TLS_REQUIRE_CLIENT_CERT=true ;;
esac
COMPOSE_BASE=(docker compose -p "$PROJECT" -f "$E2E_REPO_ROOT/docker-compose.yml")
PGDATA_DIR=/var/lib/postgresql/data
A=node3                      # the ahead replica (nofailover)
OVERRIDE_FILE=""
IMG_node1="" ; IMG_node2="" ; IMG_node3=""
TAG_APPLIED=false
KILLED=()
P="" ; B=""
ROWS_BASE=1000
ROWS_AHEAD=20000

name_to_url() {
    case "$1" in node1) echo "$NODE1_API" ;; node2) echo "$NODE2_API" ;; node3) echo "$NODE3_API" ;; esac
}
dpsql() { # <node name> <sql>
    docker exec "pg-ha-$1" psql -U "$PG_USER" -d postgres -Atc "$2" 2>/dev/null || echo ""
}
find_primary() {
    local n
    for n in node1 node2 node3; do
        [ "$(http_get_status "$(name_to_url "$n")/primary")" = "200" ] && { echo "$n"; return 0; }
    done
    echo ""
}
is_running_replica() { # <node> : case-free probes only
    [ "$(http_get_status "$(name_to_url "$1")/replica")" = "200" ] && [ "$(get_node_state "$(name_to_url "$1")")" = "running" ]
}
# All three nodes answer /patroni as running and exactly one is primary.
wait_nodes_settled() {
    local timeout="${1:-180}" start=$SECONDS n primaries running
    while true; do
        primaries=0; running=0
        for n in node1 node2 node3; do
            [ "$(http_get_status "$(name_to_url "$n")/primary")" = "200" ] && primaries=$((primaries + 1))
            [ "$(get_node_state "$(name_to_url "$n")")" = "running" ] && running=$((running + 1))
        done
        if [ "$primaries" -eq 1 ] && [ "$running" -eq 3 ]; then return 0; fi
        if [ $((SECONDS - start)) -ge "$timeout" ]; then
            log_fail "Nodes not settled after ${timeout}s (primaries=$primaries running=$running)"
            return 1
        fi
        sleep 2
    done
}
has_hold_file() { # <node>
    docker exec "pg-ha-$1" test -e "$PGDATA_DIR/pg-ha.rewind_failed" 2>/dev/null
}
controldata_state() { # <node> : "Database cluster state" of a running container
    docker exec "pg-ha-$1" bash -c 'for b in /usr/lib/postgresql/*/bin/pg_controldata; do "$b" -D '"$PGDATA_DIR"'; done' 2>/dev/null \
        | sed -n 's/^Database cluster state: *//p' | head -n 1
}
# SIGSTOP / SIGCONT the walreceiver of a node (no pkill needed in the image).
walreceiver_signal() { # <node> <STOP|CONT>
    docker exec "pg-ha-$1" bash -c 'n=0; for p in /proc/[0-9]*; do c=$(tr "\0" " " < $p/cmdline 2>/dev/null); case "$c" in "postgres: walreceiver"*) kill -'"$2"' ${p#/proc/} && n=$((n+1));; esac; done; echo $n' 2>/dev/null || echo 0
}
write_override() { # [tagged: yes|no]
    local n img
    {
        echo "services:"
        for n in node1 node2 node3; do
            eval "img=\$IMG_$n"
            echo "  $n:"
            echo "    image: $img"
            if [ "$n" = "$A" ] && [ "${1:-no}" = yes ]; then
                echo "    environment:"
                echo "      PG_HA_TAG_NOFAILOVER: \"true\""
            fi
        done
    } > "$OVERRIDE_FILE"
}
recreate_node() { # <node> : with the current override; one retry for a transient daemon error
    local out
    out=$("${COMPOSE_BASE[@]}" -f "$OVERRIDE_FILE" up -d --no-deps --no-build --force-recreate "$1" 2>&1) && return 0
    log_warn "compose up $1 failed, retrying once: $out"
    sleep 5
    out=$("${COMPOSE_BASE[@]}" -f "$OVERRIDE_FILE" up -d --no-deps --no-build --force-recreate "$1" 2>&1) && return 0
    log_warn "compose up $1 failed again: $out"
    return 1
}
dump_node_log() { # <node>
    log_info "--- last pg-ha log lines of $1 (rewind related) ---"
    docker logs --tail 400 "pg-ha-$1" 2>&1 | grep -E "pg_rewind|rewind|single-user|Crashed|hold|rejoin|Starting PostgreSQL" | tail -n 15
}

cleanup() {
    local c
    walreceiver_signal "${B:-node1}" CONT > /dev/null 2>&1 || true
    for c in ${KILLED[@]+"${KILLED[@]}"}; do docker start "$c" > /dev/null 2>&1 || true; done
    if [ "$TAG_APPLIED" = true ] && [ -n "$OVERRIDE_FILE" ]; then
        log_info "Cleanup: recreating $A without the nofailover tag"
        write_override no
        recreate_node "$A" > /dev/null 2>&1 || true
        wait_nodes_settled 240 > /dev/null 2>&1 || true
    fi
    wait_for_cluster_ready 240 > /dev/null 2>&1 || log_warn "Cluster not fully ready after cleanup"
    [ -n "$OVERRIDE_FILE" ] && rm -f "$OVERRIDE_FILE"
}
trap cleanup EXIT

# --- Setup: node3 tagged nofailover, a lagging peer, P killed, B promoted ---
setup_scenario() {
    wait_for_cluster_ready 120 || return 1
    local n cur
    for n in node1 node2 node3; do
        eval "IMG_$n=\$(docker inspect \"pg-ha-$n\" --format '{{.Image}}')"
    done
    log_info "Pinned images: node1=$IMG_node1 node2=$IMG_node2 node3=$IMG_node3"
    OVERRIDE_FILE=$(mktemp -t pg-ha-rewind-unclean.XXXXXX)
    # node3 must not be the primary: it is the nofailover node.
    cur=$(find_primary)
    assert_not_empty "$cur" "Primary exists before the scenario" || return 1
    if [ "$cur" = "$A" ]; then
        local code
        code=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" --max-time 15 -o /dev/null -w '%{http_code}' -X POST \
            -H "Content-Type: application/json" -d "{\"leader\": \"$A\", \"candidate\": \"node1\"}" \
            "$(name_to_url "$A")/switchover" 2>/dev/null || echo 000)
        log_info "switchover $A -> node1: HTTP $code"
        wait_for_role "$(name_to_url node1)" primary 90 || return 1
        wait_nodes_settled 240 || return 1
    fi
    write_override yes
    TAG_APPLIED=true
    recreate_node "$A"
    assert_eq "0" "$?" "Recreated $A with nofailover" || return 1
    wait_nodes_settled 240 || return 1
    P=$(find_primary)
    if [ "$P" = "$A" ] || [ -z "$P" ]; then log_fail "primary is '$P' after tagging, expected node1 or node2"; return 1; fi
    for n in node1 node2; do [ "$n" != "$P" ] && B="$n"; done
    log_info "primary P=$P, lagging replica B=$B, ahead nofailover replica A=$A"
    return 0
}

make_a_ahead() {
    local i na nb stopped
    dpsql "$P" "drop table if exists sb28; create table sb28(id serial primary key, v text)" > /dev/null
    dpsql "$P" "insert into sb28(v) select 'base' from generate_series(1,$ROWS_BASE)" > /dev/null
    for i in $(seq 1 30); do
        [ "$(dpsql "$A" 'select count(*) from sb28')" = "$ROWS_BASE" ] && [ "$(dpsql "$B" 'select count(*) from sb28')" = "$ROWS_BASE" ] && break
        sleep 2
    done
    stopped=$(walreceiver_signal "$B" STOP | tail -n 1)
    assert_eq "1" "$stopped" "Walreceiver of $B stopped (SIGSTOP)" || return 1
    sleep 1
    dpsql "$P" "insert into sb28(v) select repeat('ahead',50) from generate_series(1,$ROWS_AHEAD)" > /dev/null
    for i in $(seq 1 30); do
        na=$(dpsql "$A" 'select count(*) from sb28')
        [ "$na" = "$((ROWS_BASE + ROWS_AHEAD))" ] && break
        sleep 2
    done
    nb=$(dpsql "$B" 'select count(*) from sb28')
    log_info "rows: $A=$na $B=$nb (primary $P has $((ROWS_BASE + ROWS_AHEAD)))"
    assert_eq "$((ROWS_BASE + ROWS_AHEAD))" "$na" "$A received all rows before the primary dies" || return 1
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$nb" ] && [ "$nb" -lt "$na" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "$B lags behind $A ($nb < $na)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "$B does not lag ($nb vs $na); the scenario precondition is missing"
        return 1
    fi
}

fail_over_to_b() {
    local c i
    c="pg-ha-$P"
    KILLED+=("$c")
    docker kill "$c" > /dev/null
    for i in $(seq 1 60); do
        [ "$(http_get_status "$(name_to_url "$B")/primary")" = "200" ] && break
        sleep 2
    done
    assert_eq "200" "$(http_get_status "$(name_to_url "$B")/primary")" "$B became primary after $P was killed" || return 1
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$(http_get_status "$(name_to_url "$A")/primary")" != "200" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "nofailover node $A did not become primary"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "nofailover node $A became primary"
        return 1
    fi
    # Old primary comes back (keeps 2 of 3 raft voters up for the next kill) and rejoins by itself.
    docker start "$c" > /dev/null 2>&1 || true
    KILLED=()
    sleep 30   # $A's walreceiver now loops on "new timeline 2 forked off ... before current recovery point"
    local na nb
    na=$(dpsql "$A" 'select count(*) from sb28')
    nb=$(dpsql "$B" 'select count(*) from sb28')
    log_info "rows after failover: $A=$na (ahead) $B=$nb (new primary)"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$na" ] && [ -n "$nb" ] && [ "$na" -gt "$nb" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "$A is ahead of the new primary $B ($na > $nb)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "$A is not ahead of $B ($na vs $nb); the scenario precondition is missing"
        return 1
    fi
    return 0
}

# After a restart of A: running replica, no hold file, streaming on B's timeline, equal row count.
check_a_rejoined() { # <label>
    local label="$1" i url state new_tli a_tli nb na stream
    url="$(name_to_url "$A")"
    for i in $(seq 1 90); do
        is_running_replica "$A" && break
        sleep 2
    done
    state=$(get_node_state "$url")
    TESTS_RUN=$((TESTS_RUN + 1))
    if is_running_replica "$A"; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "[$label] $A is a running replica without operator action"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "[$label] $A is not a running replica after 180s (state='$state', /patroni: $(http_get "$url/patroni" | tr -d '\n' | cut -c1-200))"
        dump_node_log "$A"
    fi
    TESTS_RUN=$((TESTS_RUN + 1))
    if has_hold_file "$A"; then
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "[$label] pg-ha.rewind_failed exists on $A: $(docker exec "pg-ha-$A" head -c 400 "$PGDATA_DIR/pg-ha.rewind_failed" 2>/dev/null | head -n 1)"
    else
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "[$label] no pg-ha.rewind_failed on $A"
    fi
    for i in $(seq 1 30); do
        stream=$(dpsql "$B" "select count(*) from pg_stat_replication where application_name='$A' and state='streaming'")
        [ "$stream" = "1" ] && break
        sleep 2
    done
    assert_eq "1" "$stream" "[$label] $A streams from the primary $B"
    new_tli=$(dpsql "$B" "select timeline_id from pg_control_checkpoint()")
    a_tli=$(dpsql "$A" "select received_tli from pg_stat_wal_receiver")
    log_info "[$label] timeline: primary=$new_tli $A receiver=$a_tli"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$new_tli" ] && [ "$new_tli" -ge 2 ] && [ "$a_tli" = "$new_tli" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "[$label] $A follows the new timeline $new_tli"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "[$label] $A is not on the new timeline (primary '$new_tli', receiver '$a_tli')"
    fi
    for i in $(seq 1 30); do
        na=$(dpsql "$A" 'select count(*) from sb28'); nb=$(dpsql "$B" 'select count(*) from sb28')
        [ -n "$na" ] && [ "$na" = "$nb" ] && break
        sleep 2
    done
    assert_eq "$nb" "$na" "[$label] row count of $A equals the new primary's"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ -n "$nb" ] && [ "$nb" -lt "$((ROWS_BASE + ROWS_AHEAD))" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "[$label] the unreplicated rows were discarded ($nb < $((ROWS_BASE + ROWS_AHEAD)))"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "[$label] the ahead rows are still there ($nb)"
    fi
}

test_unclean_kill() {
    local state
    state=$(controldata_state "$A")
    log_info "pg_control state of $A before the kill: '$state'"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$state" = "in archive recovery" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_pass "$A is not cleanly shut down before the kill ($state)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "$A state is '$state', expected 'in archive recovery'; the scenario precondition is missing"
        return 1
    fi
    KILLED+=("pg-ha-$A")
    docker kill "pg-ha-$A" > /dev/null
    docker start "pg-ha-$A" > /dev/null
    KILLED=()
    check_a_rejoined "SIGKILL"
}

test_clean_stop_control() {
    local s=$SECONDS
    # pg-ha drains connections for up to 30 s before it stops PostgreSQL: allow it.
    KILLED+=("pg-ha-$A")
    docker stop -t 120 "pg-ha-$A" > /dev/null
    log_info "clean stop took $((SECONDS - s))s"
    docker start "pg-ha-$A" > /dev/null
    KILLED=()
    check_a_rejoined "clean stop"
}

if setup_scenario && make_a_ahead && fail_over_to_b; then
    test_unclean_kill
    test_clean_stop_control
    log_info "images at the end: $(docker inspect --format '{{.Name}} {{.Image}}' pg-ha-node1 pg-ha-node2 pg-ha-node3 | tr '\n' ' ')"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); log_fail "setup failed, the SB-28 checks were not run"
fi
print_results
