#!/bin/bash
# Database connection verification tests
# Tests: direct PG access, replication, read-only enforcement, proxy routing

PASS=0; FAIL=0
check() {
  if [ "$1" = "$2" ]; then
    echo "  ✅ $3"
    PASS=$((PASS+1))
  else
    echo "  ❌ $3 (expected='$2', got='$1')"
    FAIL=$((FAIL+1))
  fi
}

psql_node() {
  local node="$1"
  local query="$2"
  local port="${3:-5432}"
  docker exec "$node" bash -c "PGPASSWORD=secret psql -h localhost -p $port -U postgres -d postgres -t -A -c \"$query\"" 2>/dev/null | tr -d '[:space:]'
}

psql_node_raw() {
  local node="$1"
  local query="$2"
  docker exec "$node" bash -c "PGPASSWORD=secret psql -h localhost -U postgres -d postgres -c \"$query\"" 2>&1
}

echo "═══════════════════════════════════════════════"
echo " PostgreSQL 数据库连接验证"
echo "═══════════════════════════════════════════════"
echo ""

# Detect which node is primary
PRIMARY_CONTAINER=""
REPLICA_CONTAINERS=()
for node in pg-ha-node1 pg-ha-node2 pg-ha-node3; do
  ROLE=$(docker exec "$node" bash -c 'PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c "SELECT pg_is_in_recovery()"' 2>/dev/null | tr -d '[:space:]')
  if [ "$ROLE" = "f" ]; then
    PRIMARY_CONTAINER="$node"
  else
    REPLICA_CONTAINERS+=("$node")
  fi
done

# If no container reports recovery=f, check who the HA engine says is primary
if [ -z "$PRIMARY_CONTAINER" ]; then
  for port in 8008 8009 8010; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary)
    if [ "$CODE" = "200" ]; then
      case "$port" in
        8008) PRIMARY_CONTAINER="pg-ha-node1" ;;
        8009) PRIMARY_CONTAINER="pg-ha-node2" ;;
        8010) PRIMARY_CONTAINER="pg-ha-node3" ;;
      esac
    fi
  done
fi

echo "  Primary: $PRIMARY_CONTAINER"
echo "  Replicas: ${REPLICA_CONTAINERS[*]}"
echo ""

REPLICA1="${REPLICA_CONTAINERS[0]:-}"
REPLICA2="${REPLICA_CONTAINERS[1]:-}"

echo "── 1. Primary 直连 ──"
R=$(psql_node "$PRIMARY_CONTAINER" "SELECT 1")
check "$R" "1" "Primary SELECT 1"

echo ""
echo "── 2. Primary 可写 ──"
psql_node "$PRIMARY_CONTAINER" "CREATE TABLE IF NOT EXISTS e2e_test(id serial, val text)" > /dev/null 2>&1
psql_node "$PRIMARY_CONTAINER" "INSERT INTO e2e_test(val) VALUES('hello_pg_ha')" > /dev/null 2>&1
R=$(psql_node "$PRIMARY_CONTAINER" "SELECT val FROM e2e_test ORDER BY id DESC LIMIT 1")
check "$R" "hello_pg_ha" "Primary INSERT + SELECT"

echo ""
echo "── 3. Replica 直连 ──"
R=$(psql_node "$REPLICA1" "SELECT 1")
check "$R" "1" "Replica 1 SELECT 1"

if [ -n "$REPLICA2" ]; then
  R=$(psql_node "$REPLICA2" "SELECT 1")
  check "$R" "1" "Replica 2 SELECT 1"
fi

echo ""
echo "── 4. Replica 只读验证 ──"
R=$(psql_node "$REPLICA1" "SELECT pg_is_in_recovery()")
check "$R" "t" "Replica 1 is in recovery (read-only)"

if [ -n "$REPLICA2" ]; then
  R=$(psql_node "$REPLICA2" "SELECT pg_is_in_recovery()")
  check "$R" "t" "Replica 2 is in recovery (read-only)"
fi

echo ""
echo "── 5. 复制验证 ──"
sleep 2
R=$(psql_node "$REPLICA1" "SELECT val FROM e2e_test ORDER BY id DESC LIMIT 1")
check "$R" "hello_pg_ha" "Replica 1 reads replicated data"

if [ -n "$REPLICA2" ]; then
  R=$(psql_node "$REPLICA2" "SELECT val FROM e2e_test ORDER BY id DESC LIMIT 1")
  check "$R" "hello_pg_ha" "Replica 2 reads replicated data"
fi

echo ""
echo "── 6. Replica 拒绝写入 ──"
WRITE_ERR=$(psql_node_raw "$REPLICA1" "INSERT INTO e2e_test(val) VALUES('fail')")
if echo "$WRITE_ERR" | grep -q "read-only\|cannot execute"; then
  echo "  ✅ Replica rejects writes"; PASS=$((PASS+1))
else
  echo "  ❌ Replica did NOT reject write (output: $WRITE_ERR)"; FAIL=$((FAIL+1))
fi

echo ""
echo "── 7. 代理端口 (RW=6432, RO=6433) ──"
R=$(psql_node "$PRIMARY_CONTAINER" "SELECT 1" 6432)
check "$R" "1" "RW proxy connects"

R=$(psql_node "$REPLICA1" "SELECT 1" 6433)
check "$R" "1" "RO proxy connects"

echo ""
echo "── 8. 跨节点网络连接 ──"
# Get primary hostname from container name
PRIMARY_HOST="${PRIMARY_CONTAINER#pg-ha-}"
R=$(docker exec "$REPLICA1" bash -c "PGPASSWORD=secret psql -h $PRIMARY_HOST -U postgres -d postgres -t -A -c \"SELECT 1\"" 2>/dev/null | tr -d '[:space:]')
check "$R" "1" "$REPLICA1 → $PRIMARY_HOST cross-node"

echo ""
echo "── 9. 批量写入 + 复制一致性 ──"
for i in $(seq 1 10); do
  psql_node "$PRIMARY_CONTAINER" "INSERT INTO e2e_test(val) VALUES('row_$i')" > /dev/null 2>&1
done
sleep 3
PC=$(psql_node "$PRIMARY_CONTAINER" "SELECT count(*) FROM e2e_test")
RC=$(psql_node "$REPLICA1" "SELECT count(*) FROM e2e_test")
check "$PC" "$RC" "Replication consistent (primary=$PC, replica=$RC)"

echo ""
echo "── 10. Primary 不在 recovery 模式 ──"
R=$(psql_node "$PRIMARY_CONTAINER" "SELECT pg_is_in_recovery()")
check "$R" "f" "Primary NOT in recovery"

echo ""
echo "═══════════════════════════════════════════════"
echo " 结果: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════"
exit $FAIL
