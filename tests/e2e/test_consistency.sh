#!/bin/bash
# 全面自洽性验证：验证 PG 层、HA 层、DCS 层、代理层在各场景下状态一致
# errexit/pipefail OFF so a single failing check (or a transient 503 during a
# scenario transition) accumulates into FAIL and the script still reaches its
# summary and `exit $FAIL`, rather than aborting mid-scenario.
set +e +o pipefail
set -u

source "$(dirname "${BASH_SOURCE[0]}")/rest_env.sh"
PASS=0; FAIL=0
check() {
  if [ "$1" = "$2" ]; then echo "    ✅ $3"; PASS=$((PASS+1))
  else echo "    ❌ $3 (expected=$2, got=$1)"; FAIL=$((FAIL+1)); fi
}

psql_node() {
  docker exec "$1" bash -c "PGPASSWORD=secret psql -h localhost -U postgres -d postgres -t -A -c \"$2\"" 2>/dev/null | tr -d '[:space:]'
}

# Wait for all three API endpoints to answer and for a primary to exist, so we
# never assert against a just-bootstrapped replica that still answers /replica
# with 503. Polls instead of a fixed sleep.
wait_cluster_ready() {
  local timeout="${1:-120}"
  local start=$SECONDS
  echo "  Waiting for cluster to be ready (timeout ${timeout}s)..."
  while true; do
    local ready=0 primary=0
    for port in 8008 8009 8010; do
      [ "$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$port/patroni 2>/dev/null)" = "200" ] && ready=$((ready+1))
      [ "$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$port/primary 2>/dev/null)" = "200" ] && primary=1
    done
    if [ "$ready" -eq 3 ] && [ "$primary" -eq 1 ]; then
      echo "  Cluster ready in $((SECONDS - start))s"
      return 0
    fi
    if [ "$((SECONDS - start))" -ge "$timeout" ]; then
      echo "  ⚠️  Cluster not ready after ${timeout}s (${ready}/3 responding)"
      return 1
    fi
    sleep 2
  done
}

# Poll until a node's /replica reports 200 (caught-up replica) or timeout.
wait_replica_ready() {
  local port="$1"; local timeout="${2:-60}"; local start=$SECONDS
  while [ "$((SECONDS - start))" -lt "$timeout" ]; do
    [ "$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$port/replica 2>/dev/null)" = "200" ] && return 0
    sleep 2
  done
  return 1
}

# Poll until some node (other than $1) becomes primary; echo its container name.
wait_new_primary() {
  local old="$1"; local timeout="${2:-60}"; local start=$SECONDS
  while [ "$((SECONDS - start))" -lt "$timeout" ]; do
    for port in 8008 8009 8010; do
      if [ "$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$port/primary 2>/dev/null)" = "200" ]; then
        local ctr
        case $port in 8008) ctr=pg-ha-node1;; 8009) ctr=pg-ha-node2;; 8010) ctr=pg-ha-node3;; esac
        if [ "$ctr" != "$old" ]; then echo "$ctr"; return 0; fi
      fi
    done
    sleep 2
  done
  return 1
}

echo "═══════════════════════════════════════════════════"
echo "  pg-ha 全面自洽性验证"
echo "═══════════════════════════════════════════════════"
echo ""

# ─── 场景 1: 稳态集群 ───
echo "═══ 场景 1: 稳态集群 — 各层状态对齐 ═══"
echo ""

# 先等集群就绪，避免对刚 bootstrap 未就绪的 replica 断言
wait_cluster_ready 120 || true

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
  HA_ROLE=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:$PORT/patroni | jq -r '.role')
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
  PRIMARY_CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$PORT/primary)
  REPLICA_CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$PORT/replica)
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

# 轮询等待新 Primary 选出（替代固定 sleep）
NEW_PRIMARY=$(wait_new_primary "$PRIMARY" 60 || true)
echo "  新 PG 层 Primary: ${NEW_PRIMARY:-NONE}"
check "${NEW_PRIMARY:-NONE}" "${NEW_PRIMARY:-NONE}" "Failover: 新 Primary 选出"

if [ -n "$NEW_PRIMARY" ]; then
  # 验证新 Primary 各层一致
  case "$NEW_PRIMARY" in
    pg-ha-node1) NP_PORT=8008 ;; pg-ha-node2) NP_PORT=8009 ;; pg-ha-node3) NP_PORT=8010 ;;
  esac

  NP_HA_ROLE=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:$NP_PORT/patroni | jq -r '.role')
  check "$NP_HA_ROLE" "Primary" "新 Primary: HA role = Primary"

  NP_PRIMARY_CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$NP_PORT/primary)
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
    R_HA_ROLE=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:$R_PORT/patroni | jq -r '.role')
    check "$R_RECOVERY" "t" "$node: PG in-recovery"
    check "$R_HA_ROLE" "Replica" "$node: HA role = Replica"
    # 数据复制到了存活 Replica：轮询等待复制追上（替代固定 sleep 2）
    R_VAL=""
    for _ in $(seq 1 15); do
      R_VAL=$(psql_node "$node" "SELECT val FROM consistency_test WHERE val='after_failover' LIMIT 1")
      [ "$R_VAL" = "after_failover" ] && break
      sleep 2
    done
    check "$R_VAL" "after_failover" "$node: 复制了 after_failover 数据"
  done
fi

echo ""
echo "═══ 场景 3: 节点重新加入 — Rejoin 后状态恢复 ═══"
echo ""

echo "  重启旧 Primary ($PRIMARY)..."
docker start "$PRIMARY" > /dev/null 2>&1

case "$PRIMARY" in
  pg-ha-node1) OLD_PORT=8008 ;; pg-ha-node2) OLD_PORT=8009 ;; pg-ha-node3) OLD_PORT=8010 ;;
esac

# 轮询等待旧 Primary rejoin 为 replica（替代固定 sleep 40）
echo "  等待旧 Primary rejoin 为 replica (max 120s)..."
wait_replica_ready "$OLD_PORT" 120 || echo "  ⚠️  旧 Primary 未在 120s 内变为 replica"

OLD_RECOVERY=$(psql_node "$PRIMARY" "SELECT pg_is_in_recovery()")
OLD_HA_ROLE=$(curl "${CURL_OPTS[@]}" $SCHEME://localhost:$OLD_PORT/patroni | jq -r '.role')
OLD_PRIMARY_CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$OLD_PORT/primary)
OLD_REPLICA_CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' $SCHEME://localhost:$OLD_PORT/replica)

check "$OLD_RECOVERY" "t" "旧 Primary rejoin: PG in-recovery"
check "$OLD_HA_ROLE" "Replica" "旧 Primary rejoin: HA role = Replica"
check "$OLD_PRIMARY_CODE" "503" "旧 Primary rejoin: /primary → 503"
check "$OLD_REPLICA_CODE" "200" "旧 Primary rejoin: /replica → 200"

# 数据完整性：旧 Primary 能读到 failover 后写入的数据（轮询等待复制追上）
OLD_VAL=""
for _ in $(seq 1 15); do
  OLD_VAL=$(psql_node "$PRIMARY" "SELECT val FROM consistency_test WHERE val='after_failover' LIMIT 1")
  [ "$OLD_VAL" = "after_failover" ] && break
  sleep 2
done
check "$OLD_VAL" "after_failover" "旧 Primary rejoin: 读到 failover 后数据"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  结果: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════"
exit $FAIL
