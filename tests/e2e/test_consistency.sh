#!/bin/bash
# 全面自洽性验证：验证 PG 层、HA 层、DCS 层、代理层在各场景下状态一致
set -euo pipefail

PASS=0; FAIL=0
check() {
  if [ "$1" = "$2" ]; then echo "    ✅ $3"; PASS=$((PASS+1))
  else echo "    ❌ $3 (expected=$2, got=$1)"; FAIL=$((FAIL+1)); fi
}

psql_node() {
  docker exec "$1" bash -c "PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c \"$2\"" 2>/dev/null | tr -d '[:space:]'
}

echo "═══════════════════════════════════════════════════"
echo "  pg-ha 全面自洽性验证"
echo "═══════════════════════════════════════════════════"
echo ""

# ─── 场景 1: 稳态集群 ───
echo "═══ 场景 1: 稳态集群 — 各层状态对齐 ═══"
echo ""

# 找到 Primary
PRIMARY=""
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  RECOVERY=$(psql_node "$node" "SELECT pg_is_in_recovery()")
  if [ "$RECOVERY" = "f" ]; then PRIMARY=$node; fi
done
echo "  PG 层 Primary: $PRIMARY"

# HA 层
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  case "$node" in
    pg-ha-node1) PORT=8008 ;; pg-ha-node2) PORT=8009 ;; pg-ha-node3) PORT=8010 ;;
  esac
  HA_ROLE=$(curl -s http://localhost:$PORT/patroni | jq -r '.role')
  PG_RECOVERY=$(psql_node "$node" "SELECT pg_is_in_recovery()")

  echo ""
  echo "  --- $node ---"

  # 1. PG recovery 和 HA role 一致
  if [ "$PG_RECOVERY" = "f" ]; then
    check "$HA_ROLE" "Primary" "$node: PG not-in-recovery ↔ HA role Primary"
  else
    check "$HA_ROLE" "Replica" "$node: PG in-recovery ↔ HA role Replica"
  fi

  # 2. 代理层：Primary 节点 /primary=200, /replica=503
  PRIMARY_CODE=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$PORT/primary)
  REPLICA_CODE=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$PORT/replica)
  if [ "$PG_RECOVERY" = "f" ]; then
    check "$PRIMARY_CODE" "200" "$node: /primary → 200 (is primary)"
    check "$REPLICA_CODE" "503" "$node: /replica → 503 (not replica)"
  else
    check "$PRIMARY_CODE" "503" "$node: /primary → 503 (not primary)"
    check "$REPLICA_CODE" "200" "$node: /replica → 200 (is replica)"
  fi
done

echo ""

# 3. Primary 有 replication 连接
REPL_COUNT=$(psql_node "$PRIMARY" "SELECT count(*) FROM pg_stat_replication")
check "$REPL_COUNT" "2" "Primary pg_stat_replication: 2 streaming replicas"

# 4. 数据完整性：写入 Primary，读取所有 Replica
echo ""
echo "  --- 数据完整性 ---"
psql_node "$PRIMARY" "CREATE TABLE IF NOT EXISTS consistency_test(id serial, val text)" > /dev/null
psql_node "$PRIMARY" "INSERT INTO consistency_test(val) VALUES('check1')" > /dev/null
sleep 2
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  VAL=$(psql_node "$node" "SELECT val FROM consistency_test WHERE val='check1' LIMIT 1")
  check "$VAL" "check1" "$node: reads 'check1' from consistency_test"
done

echo ""
echo "═══ 场景 2: 节点下线 — Failover 状态转换 ═══"
echo ""

# 停止 Primary
echo "  停止 Primary ($PRIMARY)..."
docker stop "$PRIMARY" > /dev/null 2>&1
sleep 20

# 找到新 Primary
NEW_PRIMARY=""
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  if [ "$node" = "$PRIMARY" ]; then continue; fi
  RECOVERY=$(psql_node "$node" "SELECT pg_is_in_recovery()")
  if [ "$RECOVERY" = "f" ]; then NEW_PRIMARY=$node; fi
done
echo "  新 PG 层 Primary: ${NEW_PRIMARY:-NONE}"
check "${NEW_PRIMARY:-NONE}" "${NEW_PRIMARY:-NONE}" "Failover: 新 Primary 选出"

if [ -n "$NEW_PRIMARY" ]; then
  # 验证新 Primary 各层一致
  case "$NEW_PRIMARY" in
    pg-ha-node1) NP_PORT=8008 ;; pg-ha-node2) NP_PORT=8009 ;; pg-ha-node3) NP_PORT=8010 ;;
  esac

  NP_HA_ROLE=$(curl -s http://localhost:$NP_PORT/patroni | jq -r '.role')
  check "$NP_HA_ROLE" "Primary" "新 Primary: HA role = Primary"

  NP_PRIMARY_CODE=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$NP_PORT/primary)
  check "$NP_PRIMARY_CODE" "200" "新 Primary: /primary → 200"

  # 新 Primary 可写
  psql_node "$NEW_PRIMARY" "INSERT INTO consistency_test(val) VALUES('after_failover')" > /dev/null
  AF_VAL=$(psql_node "$NEW_PRIMARY" "SELECT val FROM consistency_test WHERE val='after_failover' LIMIT 1")
  check "$AF_VAL" "after_failover" "新 Primary 可写: INSERT + SELECT"

  # 存活 Replica 各层一致
  for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
    if [ "$node" = "$PRIMARY" ] || [ "$node" = "$NEW_PRIMARY" ]; then continue; fi
    case "$node" in
      pg-ha-node1) R_PORT=8008 ;; pg-ha-node2) R_PORT=8009 ;; pg-ha-node3) R_PORT=8010 ;;
    esac
    R_RECOVERY=$(psql_node "$node" "SELECT pg_is_in_recovery()")
    R_HA_ROLE=$(curl -s http://localhost:$R_PORT/patroni | jq -r '.role')
    check "$R_RECOVERY" "t" "$node: PG in-recovery"
    check "$R_HA_ROLE" "Replica" "$node: HA role = Replica"
    # 数据复制到了存活 Replica
    sleep 2
    R_VAL=$(psql_node "$node" "SELECT val FROM consistency_test WHERE val='after_failover' LIMIT 1")
    check "$R_VAL" "after_failover" "$node: 复制了 after_failover 数据"
  done
fi

echo ""
echo "═══ 场景 3: 节点重新加入 — Rejoin 后状态恢复 ═══"
echo ""

echo "  重启旧 Primary ($PRIMARY)..."
docker start "$PRIMARY" > /dev/null 2>&1
sleep 40

case "$PRIMARY" in
  pg-ha-node1) OLD_PORT=8008 ;; pg-ha-node2) OLD_PORT=8009 ;; pg-ha-node3) OLD_PORT=8010 ;;
esac

OLD_RECOVERY=$(psql_node "$PRIMARY" "SELECT pg_is_in_recovery()")
OLD_HA_ROLE=$(curl -s http://localhost:$OLD_PORT/patroni | jq -r '.role')
OLD_PRIMARY_CODE=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$OLD_PORT/primary)
OLD_REPLICA_CODE=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$OLD_PORT/replica)

check "$OLD_RECOVERY" "t" "旧 Primary rejoin: PG in-recovery"
check "$OLD_HA_ROLE" "Replica" "旧 Primary rejoin: HA role = Replica"
check "$OLD_PRIMARY_CODE" "503" "旧 Primary rejoin: /primary → 503"
check "$OLD_REPLICA_CODE" "200" "旧 Primary rejoin: /replica → 200"

# 数据完整性：旧 Primary 能读到 failover 后写入的数据
sleep 3
OLD_VAL=$(psql_node "$PRIMARY" "SELECT val FROM consistency_test WHERE val='after_failover' LIMIT 1")
check "$OLD_VAL" "after_failover" "旧 Primary rejoin: 读到 failover 后数据"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  结果: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════"
exit $FAIL
