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
check "${PRIMARY_PORT:-none}" "8010" "node3 is primary"

REPLICA_COUNT=0
for p in 8008 8009 8010; do
  CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w "%{http_code}" $SCHEME://localhost:$p/replica)
  if [ "$CODE" = "200" ]; then REPLICA_COUNT=$((REPLICA_COUNT+1)); fi
done
check "$REPLICA_COUNT" "2" "2 replicas"

PG_OK=$(docker exec pg-ha-node3 bash -c 'PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c "SELECT 1"' 2>/dev/null || echo "fail")
check "$PG_OK" "1" "Primary PG accepting connections"
echo ""

echo "── Metrics ──"
METRICS=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:8010/metrics)
HAS_ROLE=$(echo "$METRICS" | grep -c '^pg_ha_node_role' || echo "0")
HAS_LAG=$(echo "$METRICS" | grep -c '^pg_ha_replication_lag_bytes' || echo "0")
check "$HAS_ROLE" "1" "/metrics has pg_ha_node_role"
check "$HAS_LAG" "1" "/metrics has replication_lag"
echo ""

echo "── Dynamic Config ──"
curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X PUT $SCHEME://localhost:8010/config -H "Content-Type: application/json" -d '{}' > /dev/null
sleep 1
R=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:8010/config)
check "$R" "{}" "config reset to empty"

curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X PUT $SCHEME://localhost:8010/config -H "Content-Type: application/json" \
  -d '{"loop_wait":10,"ttl":30}' > /dev/null
sleep 2
LW=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:8008/config | python3 -c "import sys,json; print(json.load(sys.stdin).get('loop_wait',0))")
check "$LW" "10" "config visible on replica"

curl "${CURL_OPTS[@]}" "${REST_AUTH[@]}" -X PATCH $SCHEME://localhost:8010/config -H "Content-Type: application/json" \
  -d '{"ttl":null}' > /dev/null
sleep 1
HAS_TTL=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:8010/config | python3 -c "import sys,json; print('ttl' in json.load(sys.stdin))")
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
