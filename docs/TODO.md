# pg-ha TODO — 生产就绪路线图

本文对照当前代码（2026-10）。已落地的项标为完成；仍开放的项按代码里的实际缺口描述，不沿用过时现状。

## 项目概述

pg-ha 是一个用 Rust 编写的 PostgreSQL 高可用方案，目标是提供与 Patroni 功能对等的能力，同时消除外部依赖（无需 etcd/ZooKeeper/Consul、无需 HAProxy）。

**核心架构**：每个节点运行单个 `pg-ha` 二进制进程，内含：

- **HA Engine** — 周期性决策循环，管理 failover 和 promotion
- **Raft DCS** — 基于 openraft 0.9 的嵌入式共识，替代 etcd
- **REST API** — axum 实现的健康检查和管理端点（默认端口 8008，明文 HTTP）
- **TCP Proxy** — 读写分离代理（RW:6432, RO:6433），带主动 HTTP 健康检查、fall/rise 和每后端 `maxconn`

**当前版本**：0.1.17（与 git tag `v0.1.17` 一致）  
**代码结构**：Cargo workspace，`edition = "2024"`。核心 crate：`pg-ha-core`（HA 逻辑）、`pg-ha-dcs`（Raft）、`pg-ha-proxy`（代理）、`pg-ha-api`（REST）、`pg-ha-ctl`（CLI）

**已实现的关键能力**：

- 自动 failover、手动 switchover / failover（同步复制开启时强制候选属于 DCS `/sync`）
- Raft RPC TLS（`raft.tls`；开发用 `docker-compose.yml` 已挂载证书）。服务端目前只使用 `cert`/`key`，`require_client_cert` 未接入
- Raft 日志 append-only WAL（`raft_log.wal`，超过 4 MiB 压缩）+ `hard_state.json` / `state_machine.json` / `membership.json`
- Watchdog（Linux `/dev/watchdog`，防 zombie primary）
- 优雅停机：等待 `shutdown.drain_timeout_secs`（期间续锁）→ Leader 上 CHECKPOINT + 停库（`-m fast`，失败再 `-m immediate`），受 leader 租约限制 → 停库后才释放 leader lock；停不掉时不退出
- Prometheus metrics、文本或 JSON 日志（`log_format` 或 `PG_HA_LOG_FORMAT`）
- Docker Compose 3 节点开发集群、`make up` / `make test`
- 动态同步复制（Priority 模式）、`/sync` `/async` 健康检查、级联复制（`tags.replicatefrom`）
- Standby cluster：动态配置 `standby_cluster` 已接入 HA 循环的远端跟随；`Ha::cascade_promote` 无 REST/CLI 入口

**还没接到运行路径上的接口**：`GET /history` 有路由，HA 循环不调用 `History::record_event`，运行中通常是空数组。Failsafe、REST TLS、`max_lag_on_syncnode` 见下文。

---

## P0 — 阻塞上线（必须在生产部署前完成）

### 1. REST API TLS 支持

**现状**：管理端点（switchover、failover、restart、reinitialize、config）是 HTTP + 可选 Basic Auth。`src/main.rs` 对 REST 使用 `axum::serve` 明文监听。`axum-server` 的 `tls-rustls` 只用于 Raft RPC（配置了 `raft.tls` 时 `bind_rustls`）。  
**目标**：REST 内建 HTTPS，或在部署文档中把「前置 TLS 终结」写成强制要求并给出可复制的反代示例。当前 [部署指南](deployment-guide.md) 只说明了 Raft 证书，没有 REST TLS。  
**实现方向**：

- 在 `restapi` 增加可选的 `tls_cert` / `tls_key`（及 CA）
- 配置了 TLS 时用 `axum_server::bind_rustls()`，否则保持 HTTP（开发模式）

**相关文件**：

- `src/main.rs` — REST 与 Raft 的监听分支
- `crates/pg-ha-core/src/config.rs` — `RestApiConfig`（尚无 TLS 字段）、`RaftTlsConfig`
- `crates/pg-ha-api/src/lib.rs` — 路由组装，不含传输层

---

### 2. 同步复制 Failover 约束 — ✅ 已完成（S5）

**已实现**：`synchronous_mode` 启用时，自动选举与指定 `candidate` 的 Failover/Switchover 均强制候选属于 DCS `/sync.sync_standby`；名单缺失、空或为 `*` 时不晋升；不合格候选返回 409，`status` 为 `rejected`。权威来源仅为 DCS `/sync`。`synchronous_mode_strict` 只影响写阻塞，不参与选举。

**仍开放**：

- `max_lag_on_syncnode`：`SyncManager::set_max_lag` 存在，但 `compute_sync_standby_names` 在配置了阈值时仍把任意 `wal_position` 都算作合格（`sync.rs` 的 `TODO: compute actual lag`）。HA 循环也没有调用 `set_max_lag`
- Quorum 模式：`SyncMode::Quorum` 能生成 `ANY N (...)`，但 `enforce_synchronous_replication` 固定使用 `SyncMode::Priority`，动态配置没有模式开关

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

### 4. Failsafe 未接通

**现状**（比「只缺 DCS 持久化」更靠前）：

- `GlobalConfig.failsafe_mode` 能通过 `PATCH /config` 写入 DCS，但 `process_dynamic_config` 从不调用 `Failsafe::set_enabled`。默认 `enabled = false`，DCS 不可达时走 `FailsafeResult::Disabled` 并 demote
- `Failsafe::check_topology` 会 `POST {api_url}/failsafe`，路由表里没有该端点
- `raft_dcs.rs` 的 `get_cluster()` 仍是 `failsafe: None // TODO: read /failsafe key`，不读 `/failsafe`
- `pg_ha_failsafe_active` 在 `main` 的状态更新里没有赋值，进程内保持初始值 `0`

**目标**：`failsafe_mode` 真正开关内存状态；成员间有可响应的确认端点；启用状态与成员列表在重启后仍可从 DCS 恢复；指标反映 `Failsafe::is_active()`。

**相关文件**：

- `crates/pg-ha-core/src/failsafe.rs`
- `crates/pg-ha-core/src/ha/mod.rs` — `handle_dcs_error`、`process_dynamic_config`
- `crates/pg-ha-dcs/src/raft_dcs.rs` — `get_cluster()`
- `crates/pg-ha-api/src/routes/mod.rs` — 尚无 `/failsafe`
- `src/main.rs` — API 状态刷新

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
**未做**：没有 REST/CLI 触发 `cascade_promote`；运维文档此前未描述该配置。跨区域网络、槽位和归档的运行手册仍缺。

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
| `crates/pg-ha-core/src/sync.rs`（lag 过滤） | `TODO: compute actual lag` | #2 剩余：`max_lag_on_syncnode` |
| `crates/pg-ha-dcs/src/raft_dcs.rs`（`get_cluster`） | `TODO: read /failsafe key` | #4 |

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
