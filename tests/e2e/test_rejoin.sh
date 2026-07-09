#!/bin/bash
# Test: Old primary rejoin after failover via pg_rewind

echo "═══════════════════════════════════════════════"
echo " 旧 Primary 重新加入测试 (pg_rewind)"
echo "═══════════════════════════════════════════════"
echo ""

psql_node() {
  local node="$1"; local query="$2"
  docker exec "$node" bash -c "PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c \"$query\"" 2>/dev/null | tr -d '[:space:]'
}

# Detect current primary
PRIMARY_CONTAINER=""
PRIMARY_PORT=""
for port in 8008 8009 8010; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary)
  if [ "$CODE" = "200" ]; then
    PRIMARY_PORT=$port
    case $port in
      8008) PRIMARY_CONTAINER="pg-ha-node1" ;;
      8009) PRIMARY_CONTAINER="pg-ha-node2" ;;
      8010) PRIMARY_CONTAINER="pg-ha-node3" ;;
    esac
  fi
done

echo "Step 1: Current primary = $PRIMARY_CONTAINER (:$PRIMARY_PORT)"
echo ""

echo "Step 2: Writing test data to primary..."
psql_node "$PRIMARY_CONTAINER" "CREATE TABLE IF NOT EXISTS rejoin_test(id serial, val text)" > /dev/null
psql_node "$PRIMARY_CONTAINER" "INSERT INTO rejoin_test(val) VALUES('before_failover')" > /dev/null
R=$(psql_node "$PRIMARY_CONTAINER" "SELECT count(*) FROM rejoin_test")
echo "  Rows in rejoin_test: $R"
echo ""

echo "Step 3: Stopping primary ($PRIMARY_CONTAINER)..."
docker stop "$PRIMARY_CONTAINER" > /dev/null 2>&1
echo "  Stopped."
echo ""

echo "Step 4: Waiting for new primary election (max 30s)..."
NEW_PRIMARY=""
for i in $(seq 1 6); do
  sleep 5
  for port in 8008 8009 8010; do
    if [ "$port" = "$PRIMARY_PORT" ]; then continue; fi
    CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary 2>/dev/null)
    if [ "$CODE" = "200" ]; then
      NEW_PRIMARY=$port
      break 2
    fi
  done
  echo "  ${i}x5s: still waiting..."
done

if [ -z "$NEW_PRIMARY" ]; then
  echo "  ❌ No new primary elected after 30s"
  docker start "$PRIMARY_CONTAINER" > /dev/null 2>&1
  exit 1
fi

NEW_PRIMARY_CONTAINER=""
case $NEW_PRIMARY in
  8008) NEW_PRIMARY_CONTAINER="pg-ha-node1" ;;
  8009) NEW_PRIMARY_CONTAINER="pg-ha-node2" ;;
  8010) NEW_PRIMARY_CONTAINER="pg-ha-node3" ;;
esac
echo "  ✅ New primary: $NEW_PRIMARY_CONTAINER (:$NEW_PRIMARY)"
echo ""

echo "Step 5: Writing more data to NEW primary..."
psql_node "$NEW_PRIMARY_CONTAINER" "INSERT INTO rejoin_test(val) VALUES('after_failover')" > /dev/null
R=$(psql_node "$NEW_PRIMARY_CONTAINER" "SELECT count(*) FROM rejoin_test")
echo "  Rows now: $R"
echo ""

echo "Step 6: Restarting old primary ($PRIMARY_CONTAINER)..."
docker start "$PRIMARY_CONTAINER" > /dev/null 2>&1
echo "  Started. Waiting for rejoin (max 60s)..."
echo ""

REJOINED=false
for i in $(seq 1 12); do
  sleep 5
  STATUS=$(curl -s http://localhost:$PRIMARY_PORT/patroni 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['role']} ({d['state']})\")" 2>/dev/null || echo "not responding")
  echo "  ${i}x5s: old primary = $STATUS"
  if echo "$STATUS" | grep -qi "replica.*running\|Replica.*Running"; then
    REJOINED=true
    echo "  ✅ Old primary rejoined as Replica!"
    break
  fi
done
echo ""

if [ "$REJOINED" = "false" ]; then
  echo "  ⚠️  Old primary has NOT fully rejoined as replica yet."
  echo "  Checking logs..."
  docker logs "$PRIMARY_CONTAINER" 2>&1 | grep -E "rejoin|rewind|basebackup|standby|follow" | tail -10
  echo ""
fi

echo "Step 7: Verifying data consistency..."
sleep 3
OLD_COUNT=$(psql_node "$PRIMARY_CONTAINER" "SELECT count(*) FROM rejoin_test")
NEW_COUNT=$(psql_node "$NEW_PRIMARY_CONTAINER" "SELECT count(*) FROM rejoin_test")
echo "  Old primary rows: $OLD_COUNT"
echo "  New primary rows: $NEW_COUNT"

AFTER_VAL=$(psql_node "$PRIMARY_CONTAINER" "SELECT val FROM rejoin_test WHERE val='after_failover'")
echo "  Old primary sees 'after_failover': $AFTER_VAL"
echo ""

echo "═══════════════════════════════════════════════"
if [ "$OLD_COUNT" = "$NEW_COUNT" ] && [ "$AFTER_VAL" = "after_failover" ]; then
  echo " ✅ PASS: 旧 Primary 成功重新加入并同步了所有数据"
elif [ "$REJOINED" = "true" ]; then
  echo " ⚠️  PARTIAL: 重新加入了但数据不一致 (old=$OLD_COUNT, new=$NEW_COUNT)"
else
  echo " ❌ FAIL: 旧 Primary 未能重新加入为 Replica"
fi
echo "═══════════════════════════════════════════════"

echo ""
echo "Final cluster state:"
for port in 8008 8009 8010; do
  STATUS=$(curl -s http://localhost:$port/patroni 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['name']}: {d['role']} ({d['state']})\")" 2>/dev/null || echo "not responding")
  echo "  :$port -> $STATUS"
done
