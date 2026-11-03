# pg-ha TODO — 生产就绪路线图

本文对照当前代码（2026-11，`main` `eb0f8ec`，标签 `v0.1.19`）。已落地的项标为完成；仍开放的项按代码里的实际缺口描述，不沿用过时现状。

## 项目概述

pg-ha 是一个用 Rust 编写的 PostgreSQL 高可用方案，对标 Patroni 那类能力（自动故障切换、同步复制、健康检查），同时去掉 etcd/ZooKeeper/Consul 和 HAProxy。不逐项照搬 Patroni 的外部依赖和全部开关。

**核心架构**：每个节点运行单个 `pg-ha` 二进制进程，内含：

- **HA Engine** — 周期性决策循环，管理 failover 和 promotion
- **Raft DCS** — 基于 openraft 0.9 的嵌入式共识，替代 etcd
- **REST API** — axum 实现的健康检查和管理端点（默认端口 8008，默认明文 HTTP；配置 `restapi.tls` 后为 HTTPS，可选 mTLS）
- **TCP Proxy** — 读写分离代理（RW:6432, RO:6433），带主动 HTTP 健康检查、fall/rise 和每后端 `maxconn`

**当前版本**：0.1.19（与 git tag `v0.1.19` 一致）  
**代码结构**：Cargo workspace，`edition = "2024"`。核心 crate：`pg-ha-core`（HA 逻辑）、`pg-ha-dcs`（Raft）、`pg-ha-proxy`（代理）、`pg-ha-api`（REST）、`pg-ha-ctl`（CLI）

**已实现的关键能力**：

- 自动 failover、手动 switchover / failover（同步复制开启时强制候选属于 DCS `/sync`）
- Raft RPC TLS（`raft.tls`；开发用 `docker-compose.yml` 已挂载证书）。`require_client_cert: true` 时服务端强制校验客户端证书（针对 `ca_cert`），拒绝未提供或不受信任证书的连接；默认 `false` 为仅服务端认证 TLS
- REST API TLS（`restapi.tls`，SB-03）：`cert` + `key` 启用 HTTPS，加 `ca_cert` + `require_client_cert: true` 启用 mTLS（fail-closed，拒绝无证书/外部 CA）；省略即明文 HTTP。与 Raft TLS 共用同一套服务端 mTLS 机制
- Raft 日志 append-only WAL（`raft_log.wal`，超过 4 MiB 压缩）+ `hard_state.json` / `state_machine.json` / `membership.json`
- Watchdog（Linux `/dev/watchdog`；仅可写主 + 有效租约时喂狗，超时 = TTL − `safety_margin`；默认 `mode: off`）
- 优雅停机：等待 `shutdown.drain_timeout_secs`（期间续锁）→ Leader 上 CHECKPOINT + 停库（`-m fast`，失败再 `-m immediate`），受 leader 租约限制 → 停库后才释放 leader lock；停不掉时不退出
- Prometheus metrics、文本或 JSON 日志（`log_format` 或 `PG_HA_LOG_FORMAT`）
- Docker Compose 3 节点开发集群、`make up` / `make test`
- 动态同步复制（Priority 模式）、`/sync` `/async` 健康检查、级联复制（`tags.replicatefrom`）
- Standby cluster：动态配置 `standby_cluster` 已接入 HA 循环的远端跟随；`Ha::cascade_promote` 无 REST/CLI 入口

**还没接到运行路径上的接口**：`GET /history` 有路由，HA 循环不调用 `History::record_event`，运行中通常是空数组。（Raft 快照落盘已由 SB-06 闭合——快照持久化到 `snapshot.json`、重启读回，见下文 #14；`/failover` 无 TTL 已由 SB-05 闭合——非空意图带集群 `ttl` 自过期，见下文 #13；成员 `conn_url` 含密码已由 SB-07 Option A 闭合——密码不再写入 DCS；Failsafe 已由 SB-04 裁定为 NO-GO 并移除表面代码，见下文 #4）。同步侧的 `maximum_lag_on_syncnode` 已接入；仍开放的是选举侧的 `maximum_lag_on_failover` 和 REST `replication_lag` 填充。

---

## P0 — 阻塞上线（必须在生产部署前完成）

编号 13、14 仍开放；编号 15（成员 `conn_url` 含密码）已由 SB-07 Option A 闭合。编号 1（REST API TLS，SB-03）与编号 2 已完成；编号 2 留在这里是因为同步复制的 Quorum、选举延迟阈值和 `replication_lag` 还挂在它下面。13–15 使用文末编号，避免打乱已经写在别处的「TODO #1」。

### 1. REST API TLS 支持 ✅ 已完成（SB-03）

**已实现**：走路线 A（内建 HTTPS），`src/main.rs` 对 REST 的监听分支与 Raft 对称：`restapi.tls` 省略时保持 `axum::serve` 明文 HTTP（现有部署不受影响）；设置 `cert` + `key` 时用 `axum_server::bind_rustls` + `RustlsConfig::from_pem_file` 提供仅服务端认证的 HTTPS；再设置 `ca_cert` 并把 `require_client_cert` 设为 `true` 时走共享的 `build_mtls_server_config`（`WebPkiClientVerifier`，fail-closed），`require_client_cert=true` 而 `ca_cert` 缺失/不可读时启动即报错退出。`build_mtls_server_config` 已泛化为同时服务 Raft 与 REST 两个平面。配置形状与 `raft.tls` 的服务端字段一致（`cert`/`key`/`ca_cert`/`require_client_cert`）。

docker-compose.yml + docker/entrypoint.sh 新增 `PG_HA_RESTAPI_TLS_*` 变量，默认关闭（空 cert = 明文）。文档见 [部署指南](deployment-guide.md) 的「REST API TLS / mTLS」。Docker 验收证据（含 mTLS alert 116 / 48）见命令中心任务目录的 `docker-evidence.md`。

**已实现文件**：

- `src/main.rs` — REST 与 Raft 的监听分支；共享的 `build_mtls_server_config`
- `crates/pg-ha-core/src/config.rs` — `RestApiTlsConfig`、`RestApiConfig.tls`、`RaftTlsConfig`
- `crates/pg-ha-api/src/lib.rs` — 路由组装，不含传输层（不变）

**遗留（非 SB-03 范围）**：TCP 代理的节点间健康检查（`/primary`、`/replica`）仍走明文 `http://`；若全集群开启 REST TLS，需要为这些健康检查加 HTTPS 客户端支持，留待后续。

---

### 13. `/failover` 键没有 TTL —— ✅ 已修复（SB-05）

**现状（已闭合）**：`RaftDcs::set_failover_value` 现在对**非空**意图写入 `ttl: Some(lock_ttl())`，复用集群 `ttl`（leader-lease 的同一个 TTL，默认 30s，随动态 `ttl` 变化；未新增配置项）。指定的候选节点死了或从成员列表里消失时，`/failover` key 在约 `ttl` 秒后按状态机墙钟过期、被过滤出集群视图，选举回到正常「谁最健康」——不再被死 candidate 永久卡成无主。清除语义不变：抢锁成功后写空串仍清掉 key（空值不带 TTL）。  
**落点**：

- `crates/pg-ha-dcs/src/raft_dcs.rs` — `set_failover_value`（非空值带 `ttl: Some(lock_ttl())`）
- 单测 `raft_dcs::tests::failover_intent_expires_and_clear_still_works`
- Docker 验收 `tests/e2e/test_failover_ttl.sh`
- 文档 `docs/operations-guide.md`「`/failover` key 的 TTL」、`docs/invariants.md` S3

---

### 14. Raft 快照不落盘 — ✅ 已完成（SB-06）

**已实现**：Raft 快照现在落盘到 `snapshot.json`，`new_persistent` 重启时连同 `hard_state.json`、`state_machine.json`、`raft_log.wal`、`membership.json` 一起读回。`get_current_snapshot` 因此在重启后仍能返回快照——重启之后、下一次自动快照之前，落后很多需要快照才能追上的节点不再触发 snapshot not found、不再停掉领导者的 Raft。  
**相关文件**：

- `crates/pg-ha-dcs/src/store.rs` — `new_persistent`、`build_snapshot`、`get_current_snapshot`

---

### 15. 成员 `conn_url` 把超级用户密码写进 Raft 日志 — ✅ 已完成（SB-07 Option A）

**裁定（Option A，彻底修复）**：成员 `conn_url` **不再写入超级用户密码**，`touch_member` 组装的 `conn_url` 只含 `host/port/dbname/user`，各字段过 `libpq_quote` 转义——因此密码不再进入 Raft 日志 / `raft_log.wal` / `state_machine.json` / snapshot。灾备路径（`pg_rewind`、`pg_basebackup`、rewind 前 `psql` 探测）改从本节点本地 `postgresql.superuser.password` 取对端凭据，经子进程 `PGPASSWORD` 传入（不落盘、不进 argv、日志脱敏）。要求集群所有节点 superuser 密码一致。docker 集群验证：三节点 Raft 状态 grep `password=secret` 全部 CLEAN；pg_rewind / pg_basebackup / 探测在无密码 `conn_url` 下端到端成功重新加入流复制。详见 invariants.md R3、operations-guide.md、deployment-guide.md。  
**相关文件**：

- `crates/pg-ha-core/src/ha/helpers.rs` — 组装 `conn_url`（已去除 `password=`）
- `crates/pg-ha-core/src/postgresql.rs` — `basebackup` / `rewind` / `source_answers_as_primary` 注入 `PGPASSWORD`

---

### 2. 同步复制 Failover 约束 — ✅ 已完成（S5）

**已实现**：`synchronous_mode` 启用时，自动选举与指定 `candidate` 的 Failover/Switchover 均强制候选属于 DCS `/sync.sync_standby`；名单缺失、空或为 `*` 时不晋升；不合格候选返回 409，`status` 为 `rejected`。权威来源仅为 DCS `/sync`。`synchronous_mode_strict` 只影响写阻塞，不参与选举。

**SB-01 已补齐（同步侧）**：`maximum_lag_on_syncnode`（动态配置，单位字节，`None`=关闭）已接入——`enforce_synchronous_replication` 从 `pg_stat_replication` 读实时 lag，只有实际在流复制且 lag 在阈值内的备库才算合格并被加进 `/sync`。两阶段写入（DCS `/sync` 始终是 PG `synchronous_standby_names` 的子集，加/删对称顺序）落地；strict 用不可满足的占位名单真正阻塞写入（不是 `'*'`）。详见 [invariants.md](invariants.md) S5 与 [operations-guide.md](operations-guide.md)「两阶段过渡窗口」。

**仍开放**：

- Quorum 模式：`SyncMode::Quorum` 能生成 `ANY N (...)`，但 `enforce_synchronous_replication` 固定使用 `SyncMode::Priority`，动态配置没有模式开关（仅 Priority / `FIRST N` 可用）
- **SB-11**：`maximum_lag_on_failover` 仍不被选举读取——动态配置能写入该值，但 `ha/election.rs` 侧不用它过滤候选（与已接入的 `maximum_lag_on_syncnode` 不同）
- **SB-10**：REST `/patroni` 的 `replication_lag` 字段从不被填充，始终为 `0`，没有代码计算并写入它

**相关文件**：

- `crates/pg-ha-core/src/ha/election.rs` — `sync_failover_allowed`
- `crates/pg-ha-core/src/ha/commands.rs` — switchover/failover 早拒
- `crates/pg-ha-core/src/sync.rs` — `SyncManager`
- `crates/pg-ha-core/src/ha/mod.rs` — `enforce_synchronous_replication`
- `docs/invariants.md` — S5（持有）

---

## P1 — 高优先级改进

### 3. Raft 日志增量持久化 — ✅ 已完成（含失败处理）

**已实现**：`data_dir` 存在时，Raft 日志走 append-only [`WalWriter`](../crates/pg-ha-dcs/src/wal.rs)（`raft_log.wal`，bincode 记录，超过 4 MiB 压缩）。`hard_state.json`、`state_machine.json`、`membership.json` 仍是原子 JSON 写。不再把整份 log `BTreeMap` 序列化成 `log_entries.json`。

**失败处理**（不变量 P4，已持有）：WAL 与 JSON 落盘失败时，存储 trait 方法返回 `StorageError`，openraft 停止该节点的 Raft，不再把未落盘的投票、commit、日志当成已持久化。降级语义与恢复方式见 [invariants.md](invariants.md) 第 6 节。

**后续可选优化**：目前仍是先改内存再写盘（WAL 压缩需要完整内存日志）。改为严格的先写盘、成功后再改内存，需要调整压缩的输入，不影响 P4 的成立。

**相关文件**：

- `crates/pg-ha-dcs/src/store.rs` — 持久化 helper 与存储 trait 方法
- `crates/pg-ha-dcs/src/wal.rs` — `COMPACTION_THRESHOLD`

---

### 4. Failsafe —— 已裁定 NO-GO，表面代码已移除（SB-04）✅

**裁定**：failsafe 在 pg-ha 的内嵌 RaftDcs 架构下**不适用**，已删除其误导性表面代码。

Patroni 的 failsafe 为**外部 DCS**（etcd/consul）设计：DCS 挂了但 PostgreSQL 集群健康时，primary 通过 REST 向所有成员确认拓扑而不是降级。pg-ha 用**与进程同处一地的内嵌 Raft DCS**，所以：

- `RaftDcs::get_cluster` 是本地读，永不返回 `Err` → failsafe 入口 `handle_dcs_error` 不可达；
- 「无法到达共识」已由**租约到期 fencing**（见 [invariants.md](invariants.md) L5 的时间界与「租约到期围栏」小节，SB-04 称 R19）更保守地处理（primary 在 `ttl - safety_margin` 内未能续约即自我 fence，不等 DCS）。

pg-ha 明确不支持外部 DCS，保留 failsafe 只会误导，故移除。

**已删除**：`crates/pg-ha-core/src/failsafe.rs`（整文件及模块声明）、HA 中的 `Failsafe` 字段与接线、`Cluster.failsafe` 字段、`raft_dcs.rs` 的 `failsafe: None // TODO` 与 `failsafe_path()`、`pg_ha_failsafe_active` 指标与 `AppState.failsafe_active`。`handle_dcs_error` 简化为「leader 读 DCS 出错即降级防脑裂」（仅为满足 `DcsAdapter::get_cluster` 的可错 trait 契约而保留，RaftDcs 下不可达）。

**向后兼容**：`GlobalConfig.failsafe_mode` 字段仍可解析（旧配置不报错），但为 **no-op**（不再参与任何行为，不进 `detect_changes`）。

详见 [invariants.md](invariants.md) P3 裁定记录。

---

### 5. Proxy 连接池

**现状**：`crates/pg-ha-proxy` 在健康检查（GET `/primary`、GET `/replica`，fall/rise，标记 down 时断开已有会话，默认每后端 300 连接）之后，仍是 accept → connect → 双向拷贝。没有 PgBouncer 式的事务级连接复用。  
**目标**：支持连接复用，降低后端连接数。若前面已经有 PgBouncer，可降为 P2。

**相关文件**：

- `crates/pg-ha-proxy/src/proxy.rs`
- `crates/pg-ha-proxy/src/lib.rs`

---

## P2 — 中期改进

### 6. CI 中的测试

**现状**：`.github/workflows/release.yml` 只在 `v*` tag 上做四平台 release 构建，不跑测试。本地有 `make test`（`cargo test --workspace`）。E2E 入口是 `tests/e2e/run_tests.sh`，默认 7 个套件：bootstrap、metrics、dynamic config、switchover、failover、network partition、pause。仓库里还有 `test_rejoin.sh`、`test_consistency.sh`、`test_ha_full.sh`、`test_db_connections.sh`、`quick_test.sh`，没有注册进 runner。Makefile 没有 `e2e` 目标。  
**目标**：

- CI 中运行 `cargo test`（含 proptest）
- CI 中启动 compose 并运行 `tests/e2e/run_tests.sh`
- 增加 Raft TLS 路径的集成测试

---

### 7. TLS 证书自动轮换

**现状**：证书手动生成后写入 `raft.tls` 路径，进程启动时加载一次，无热重载。  
**目标**：watch 文件变更并重载 TLS，或由外部（cert-manager / Vault）轮换后重启进程的操作说明。

---

### 8. REST API 增强

- 请求限流
- 管理操作审计日志（谁、何时、哪个端点）
- CORS（若要做 dashboard）
- `pg-ha-ctl` 发送 Basic Auth（当前 CLI 不带头，Docker 默认 `admin` / `secret` 时管理子命令会 401）

---

### 9. 备份/恢复集成

**现状**：无内建备份。  
**目标**：集成 pgBackRest 或 WAL-G，或在 switchover/failover 提供 callback hook。

---

## P3 — 长期规划

### 10. Standby cluster / 多区域

**已有**：`StandbyClusterConfig`（`host`、`port`、`primary_slot_name`、`restore_command`、`create_replica_methods`）放在动态配置 `standby_cluster` 下。HA 循环在持锁节点上调用远端跟随；`Ha::cascade_promote` 能从 DCS 去掉该配置并 promote。  
**未做**：没有 REST 或 `pg-ha-ctl` 触发 `cascade_promote`（运维指南已经写了这个配置和这个限制）。跨区域网络、槽位和归档的运行手册仍缺。

### 11. Web Dashboard

提供集群可视化管理界面。

### 12. 性能基准测试

- failover 耗时基准
- Proxy 吞吐 / 延迟
- Raft 写入吞吐（WAL 路径）

---

## 已知代码 TODO 汇总

| 位置 | 内容 | 关联任务 |
|------|------|----------|
| ~~`crates/pg-ha-dcs/src/raft_dcs.rs`（`get_cluster`）~~ | ~~`TODO: read /failsafe key`~~ → 已移除（#4，SB-04 NO-GO） | #4 ✅ |
| ~~`crates/pg-ha-dcs/src/raft_dcs.rs`（`set_failover_value`）~~ | ~~`ttl: None`~~ → 非空意图带集群 `ttl`（#13，SB-05） | #13 ✅ |
| ~~`crates/pg-ha-dcs/src/store.rs`（`build_snapshot` / `new_persistent`）~~ | ~~快照只在内存，重启不加载~~ → 落盘到 `snapshot.json`、重启读回（#14，SB-06） | #14 ✅ |
| ~~`crates/pg-ha-core/src/ha/helpers.rs`（组装 `conn_url`）~~ | ~~`password=` 进入 Raft 日志~~ → 已移除（#15，SB-07） | #15 ✅ |
| `crates/pg-ha-core/src/ha/election.rs` | `maximum_lag_on_failover` 未参与候选过滤 | #2 剩余 |
| REST `/patroni` `replication_lag` | 字段始终为 0，未计算填充 | #2 剩余 |

---

## 上下文提示

1. **指定要做的 TODO 编号**（如「实现 TODO #1 REST API TLS」）
2. **关键入口文件**：
   - 总入口：`src/main.rs`
   - HA 循环：`crates/pg-ha-core/src/ha/mod.rs`
   - 配置定义：`crates/pg-ha-core/src/config.rs`
   - Raft DCS：`crates/pg-ha-dcs/src/raft_dcs.rs`
   - Raft 存储：`crates/pg-ha-dcs/src/store.rs`、`crates/pg-ha-dcs/src/wal.rs`
3. **构建和测试**：
   - `cargo build` / `make build` — 本机或面向 Linux 容器的发布二进制（Mac 上走 `cargo-zigbuild`、`cross` 或 `docker-compose.build.yml`）
   - `make test` — `cargo test --workspace`
   - `make up` — 编译并启动 3 节点 Docker 集群（默认 PostgreSQL 16；`PG_VERSION` 传给官方 `postgres` 镜像）
   - `./tests/e2e/run_tests.sh` — E2E（不是 `make e2e`）
4. **工具链**：
   - Rust **1.99.1**（`rust-toolchain.toml`、Docker 构建镜像 `rust:1.99.1-bookworm`、CI release 使用 `dtolnay/rust-toolchain@1.99.1`）。`edition = "2024"`，代码使用已稳定的 if-let chains。不跟浮动的 `stable`
   - openraft 0.9
   - 日志用 `tracing`；错误用 `thiserror` / `anyhow`
