#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/rest_env.sh"
PASS=0; FAIL=0
check() { if [ "$1" = "$2" ]; then echo "  ✅ $3"; PASS=$((PASS+1)); else echo "  ❌ $3 (expected=$2, got=$1)"; FAIL=$((FAIL+1)); fi; }

echo "═══════════════════════════════════════"
echo " pg-ha E2E Quick Tests"
echo "═══════════════════════════════════════"
echo ""

echo "── Bootstrap ──"
PRIMARY_PORT=""
for p in 8008 8009 8010; do
  CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w "%{http_code}" $SCHEME://localhost:$p/primary)
  if [ "$CODE" = "200" ]; then PRIMARY_PORT=$p; fi
done
# Bootstrap leader is whichever node wins the Raft election; do not assume node3.
check "$([ -n "$PRIMARY_PORT" ] && echo yes || echo none)" "yes" "a primary is elected"
case "$PRIMARY_PORT" in
  8008) PRIMARY_CTR=pg-ha-node1; REPLICA_PORT=8009 ;;
  8009) PRIMARY_CTR=pg-ha-node2; REPLICA_PORT=8010 ;;
  *)    PRIMARY_CTR=pg-ha-node3; REPLICA_PORT=8008; PRIMARY_PORT=${PRIMARY_PORT:-8010} ;;
esac
echo "  primary: $PRIMARY_CTR (:$PRIMARY_PORT)"

REPLICA_COUNT=0
for p in 8008 8009 8010; do
  CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w "%{http_code}" $SCHEME://localhost:$p/replica)
  if [ "$CODE" = "200" ]; then REPLICA_COUNT=$((REPLICA_COUNT+1)); fi
done
check "$REPLICA_COUNT" "2" "2 replicas"

PG_OK=$(docker exec $PRIMARY_CTR bash -c 'PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c "SELECT 1"' 2>/dev/null || echo "fail")
check "$PG_OK" "1" "Primary PG accepting connections"
echo ""

echo "── Metrics ──"
METRICS=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:$PRIMARY_PORT/metrics)
HAS_ROLE=$(echo "$METRICS" | grep -c '^pg_ha_node_role' || echo "0")
HAS_LAG=$(echo "$METRICS" | grep -c '^pg_ha_replication_lag_bytes' || echo "0")
check "$HAS_ROLE" "1" "/metrics has pg_ha_node_role"
check "$HAS_LAG" "1" "/metrics has replication_lag"
echo ""

echo "── Dynamic Config ──"
curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X PUT $SCHEME://localhost:$PRIMARY_PORT/config -H "Content-Type: application/json" -d '{}' > /dev/null
sleep 1
R=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" $SCHEME://localhost:$PRIMARY_PORT/config)
check "$R" "{}" "config reset to empty"

curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X PUT $SCHEME://localhost:$PRIMARY_PORT/config -H "Content-Type: application/json" \
  -d '{"loop_wait":10,"ttl":30}' > /dev/null
sleep 2
LW=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" $SCHEME://localhost:$REPLICA_PORT/config | python3 -c "import sys,json; print(json.load(sys.stdin).get('loop_wait',0))")
check "$LW" "10" "config visible on replica"

curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X PATCH $SCHEME://localhost:$PRIMARY_PORT/config -H "Content-Type: application/json" \
  -d '{"loop_wait":null}' > /dev/null
sleep 1
# Remove loop_wait (not ttl): dropping ttl back to 15 with loop_wait=10 is rightly
# rejected by the ttl >= loop_wait + retry_timeout + safety_margin check.
HAS_TTL=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" $SCHEME://localhost:$PRIMARY_PORT/config | python3 -c "import sys,json; print('loop_wait' in json.load(sys.stdin))")
check "$HAS_TTL" "False" "PATCH null removes key"
echo ""

echo "── Switchover ──"
# Find primary port
PRI_PORT=""
for p in 8008 8009 8010; do
  CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w "%{http_code}" $SCHEME://localhost:$p/primary)
  if [ "$CODE" = "200" ]; then PRI_PORT=$p; fi
done
# Pick a candidate that isn't the current primary
CANDIDATE=""
case "$PRI_PORT" in
  8008) CANDIDATE="node2" ;;
  8009) CANDIDATE="node1" ;;
  8010) CANDIDATE="node1" ;;
esac
echo "  Primary at :$PRI_PORT, switching to $CANDIDATE"
RESP=$(curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X POST $SCHEME://localhost:$PRI_PORT/switchover -H "Content-Type: application/json" \
  -d "{\"candidate\":\"$CANDIDATE\"}")
echo "  Response: $RESP"
echo "  waiting 15s for switchover..."
sleep 15
NEW_PRIMARY=""
for p in 8008 8009 8010; do
  CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w "%{http_code}" $SCHEME://localhost:$p/primary)
  if [ "$CODE" = "200" ]; then NEW_PRIMARY=$p; fi
done
if [ -n "$NEW_PRIMARY" ] && [ "$NEW_PRIMARY" != "$PRI_PORT" ]; then
  echo "  ✅ Switchover to :$NEW_PRIMARY"; PASS=$((PASS+1))
else
  echo "  ❌ Switchover failed (primary=$NEW_PRIMARY, was $PRI_PORT)"; FAIL=$((FAIL+1))
fi
echo ""

echo "═══════════════════════════════════════"
echo " Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════"
