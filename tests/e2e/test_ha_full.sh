#!/bin/bash
# 完整高可用测试：
# 1. 集群启动 + 数据写入
# 2. 确认复制正常
# 3. 停止 Primary → 自动 failover
# 4. 新 Primary 可写
# 5. 旧 Primary 重启 → 自动 rejoin
# 6. 数据全部同步
# 7. 再次手动 switchover
# 8. 最终全集群一致

PASS=0; FAIL=0
check() {
  if [ "$1" = "$2" ]; then echo "  ✅ $3"; PASS=$((PASS+1))
  else echo "  ❌ $3 (expected='$2', got='$1')"; FAIL=$((FAIL+1)); fi
}

psql_node() {
  docker exec "$1" bash -c "PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c \"$2\"" 2>/dev/null | tr -d '[:space:]'
}

find_primary() {
  for port in 8008 8009 8010; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary 2>/dev/null)
    if [ "$CODE" = "200" ]; then
      case $port in 8008) echo "pg-ha-node1";; 8009) echo "pg-ha-node2";; 8010) echo "pg-ha-node3";; esac
      return
    fi
  done
}

port_of() {
  case "$1" in pg-ha-node1) echo "8008";; pg-ha-node2) echo "8009";; pg-ha-node3) echo "8010";; esac
}

echo "═══════════════════════════════════════════════════"
echo "  高可用完整测试"
echo "═══════════════════════════════════════════════════"
echo ""

# ── Step 1: 确认集群就绪 ──
echo "── Step 1: 集群就绪 ──"
PRIMARY=$(find_primary)
check "${PRIMARY:-none}" "$(find_primary)" "Primary found: $PRIMARY"
echo ""

# ── Step 2: 写入数据并确认复制 ──
echo "── Step 2: 写入数据 + 确认复制 ──"
psql_node "$PRIMARY" "DROP TABLE IF EXISTS ha_test" > /dev/null 2>&1
psql_node "$PRIMARY" "CREATE TABLE ha_test(id serial, val text, ts timestamp default now())" > /dev/null 2>&1
for i in $(seq 1 5); do
  psql_node "$PRIMARY" "INSERT INTO ha_test(val) VALUES('row_$i')" > /dev/null 2>&1
done
PC=$(psql_node "$PRIMARY" "SELECT count(*) FROM ha_test")
check "$PC" "5" "Primary has 5 rows"

sleep 3
# Check replicas
REPLICAS=()
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  if [ "$node" != "$PRIMARY" ]; then REPLICAS+=("$node"); fi
done

RC1=$(psql_node "${REPLICAS[0]}" "SELECT count(*) FROM ha_test")
RC2=$(psql_node "${REPLICAS[1]}" "SELECT count(*) FROM ha_test")
check "$RC1" "5" "${REPLICAS[0]} has 5 rows (replicated)"
check "$RC2" "5" "${REPLICAS[1]} has 5 rows (replicated)"
echo ""

# ── Step 3: 停止 Primary → 自动 failover ──
echo "── Step 3: 停止 Primary ($PRIMARY) ──"
docker stop "$PRIMARY" > /dev/null 2>&1
echo "  Waiting for failover (max 30s)..."
NEW_PRIMARY=""
for i in $(seq 1 6); do
  sleep 5
  NEW_PRIMARY=$(find_primary)
  if [ -n "$NEW_PRIMARY" ] && [ "$NEW_PRIMARY" != "$PRIMARY" ]; then
    break
  fi
  NEW_PRIMARY=""
done
if [ -n "$NEW_PRIMARY" ]; then
  echo "  ✅ Failover完成! 新Primary: $NEW_PRIMARY"; PASS=$((PASS+1))
else
  echo "  ❌ Failover超时"; FAIL=$((FAIL+1))
  docker start "$PRIMARY" > /dev/null 2>&1
  echo ""; echo "Results: $PASS passed, $FAIL failed"; exit 1
fi
echo ""

# ── Step 4: 新 Primary 可写 ──
echo "── Step 4: 新 Primary 可写 ──"
psql_node "$NEW_PRIMARY" "INSERT INTO ha_test(val) VALUES('after_failover_1')" > /dev/null 2>&1
psql_node "$NEW_PRIMARY" "INSERT INTO ha_test(val) VALUES('after_failover_2')" > /dev/null 2>&1
NC=$(psql_node "$NEW_PRIMARY" "SELECT count(*) FROM ha_test")
check "$NC" "7" "New primary has 7 rows"
echo ""

# ── Step 5: 重启旧 Primary → 自动 rejoin ──
echo "── Step 5: 重启旧 Primary ($PRIMARY) ──"
docker start "$PRIMARY" > /dev/null 2>&1
echo "  Waiting for rejoin (max 60s)..."
REJOINED=false
OLD_PORT=$(port_of "$PRIMARY")
for i in $(seq 1 12); do
  sleep 5
  ROLE=$(curl -s http://localhost:$OLD_PORT/patroni 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['role'])" 2>/dev/null)
  if [ "$ROLE" = "Replica" ]; then
    REJOINED=true
    echo "  ✅ 旧Primary已rejoin为Replica (${i}x5s)"; PASS=$((PASS+1))
    break
  fi
done
if [ "$REJOINED" = "false" ]; then
  echo "  ❌ 旧Primary未能rejoin"; FAIL=$((FAIL+1))
fi
echo ""

# ── Step 6: 数据全部同步 ──
echo "── Step 6: 数据一致性验证 ──"
sleep 5
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  COUNT=$(psql_node "$node" "SELECT count(*) FROM ha_test")
  check "$COUNT" "7" "$node has 7 rows"
done
echo ""

# ── Step 7: 手动 switchover ──
echo "── Step 7: 手动 switchover ──"
CURRENT_PRIMARY=$(find_primary)
CURRENT_PORT=$(port_of "$CURRENT_PRIMARY")
# Pick a candidate
CANDIDATE=""
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  if [ "$node" != "$CURRENT_PRIMARY" ]; then
    CANDIDATE="${node#pg-ha-}"
    break
  fi
done
echo "  switchover: $CURRENT_PRIMARY → $CANDIDATE"
curl -s -u admin:secret -X POST http://localhost:$CURRENT_PORT/switchover \
  -H "Content-Type: application/json" \
  -d "{\"candidate\":\"$CANDIDATE\"}" > /dev/null

sleep 20
SWITCHED_PRIMARY=$(find_primary)
if [ -n "$SWITCHED_PRIMARY" ] && [ "$SWITCHED_PRIMARY" != "$CURRENT_PRIMARY" ]; then
  echo "  ✅ Switchover成功: 新Primary=$SWITCHED_PRIMARY"; PASS=$((PASS+1))
else
  echo "  ❌ Switchover失败 (primary=$SWITCHED_PRIMARY)"; FAIL=$((FAIL+1))
fi
echo ""

# ── Step 8: 最终写入 + 全集群一致 ──
echo "── Step 8: 最终验证 ──"
FINAL_PRIMARY=$(find_primary)
psql_node "$FINAL_PRIMARY" "INSERT INTO ha_test(val) VALUES('final_write')" > /dev/null 2>&1
sleep 3
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  FINAL_VAL=$(psql_node "$node" "SELECT val FROM ha_test WHERE val='final_write'")
  check "$FINAL_VAL" "final_write" "$node sees final_write"
done
echo ""

TOTAL_ROWS=$(psql_node "$FINAL_PRIMARY" "SELECT count(*) FROM ha_test")
echo "  总行数: $TOTAL_ROWS"
echo ""

# ── Final state ──
echo "── 最终集群状态 ──"
for port in 8008 8009 8010; do
  STATUS=$(curl -s http://localhost:$port/patroni 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['name']}: {d['role']} ({d['state']})\")" 2>/dev/null || echo "not responding")
  echo "  :$port -> $STATUS"
done
echo ""

echo "═══════════════════════════════════════════════════"
echo "  结果: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════"
exit $FAIL
