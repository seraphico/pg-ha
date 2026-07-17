# pg-ha TODO — 生产就绪路线图

## 项目概述

pg-ha 是一个用 Rust 编写的 PostgreSQL 高可用方案，目标是提供与 Patroni 功能对等的能力，同时消除外部依赖（无需 etcd/ZooKeeper/Consul、无需 HAProxy）。

**核心架构**：每个节点运行单个 `pg-ha` 二进制进程，内含：
- **HA Engine** — 周期性决策循环，管理 failover 和 promotion
- **Raft DCS** — 基于 openraft 的嵌入式共识，替代 etcd
- **REST API** — axum 实现的健康检查和管理端点（端口 8008）
- **TCP Proxy** — 读写分离代理（RW:6432, RO:6433），带主动健康检查

**当前版本**：0.1.0  
**代码结构**：Cargo workspace，核心 crate 包括 `pg-ha-core`（HA逻辑）、`pg-ha-dcs`（Raft）、`pg-ha-proxy`（代理）、`pg-ha-api`（REST）、`pg-ha-ctl`（CLI）

**已实现的关键能力**：
- 自动 failover（3-10 秒）、手动 switchover
- Failsafe 模式（DCS 临时不可用时防误降级）
- TLS/mTLS Raft RPC 通信
- Watchdog（Linux 防 zombie primary）
- 优雅停机（CHECKPOINT + leader lock release）
- Prometheus metrics、结构化 JSON 日志
- Docker Compose 3 节点部署、systemd 服务文件
- E2E 测试（12 个场景脚本）

---

## P0 — 阻塞上线（必须在生产部署前完成）

### 1. REST API TLS 支持

**现状**：管理端点（switchover、failover、pause 等）仅支持 HTTP + Basic Auth，密码明文传输。  
**目标**：内建 HTTPS 支持，或者至少在文档中明确要求前置 TLS 终结代理。  
**实现方向**：
- `axum-server` 已在 workspace 依赖中且启用了 `tls-rustls` feature
- 在 `restapi` 配置段增加可选的 `tls_cert` / `tls_key` 字段
- 配置了 TLS 时使用 `axum_server::bind_rustls()`，否则退回 HTTP（开发模式）

**相关文件**：
- `crates/pg-ha-api/src/lib.rs` — API 服务启动逻辑
- `crates/pg-ha-core/src/config.rs` — `RestApiConfig` 结构体
- `src/main.rs` — API server spawn 位置

---

### 2. 同步复制 Failover 约束

**现状**：`sync.rs` 能计算 `synchronous_standby_names` 并通过 ALTER SYSTEM 设置，但选举逻辑（`ha/election.rs`）在 failover 时**不检查**候选节点是否为当前 sync standby。开启 `synchronous_mode` 后，failover 可能选出数据落后的节点导致已确认事务丢失。  
**目标**：选举时强制要求候选节点是当前 sync standby 列表中的成员（或 lag 在 `maximum_lag_on_failover` 内）。  
**实现方向**：
- 在 `is_healthiest_node()` 中增加 sync standby 约束检查
- DCS 中持久化当前 sync state（谁是 sync standby）
- strict 模式下若无合格候选者则不 failover（保数据不保可用性）

**相关文件**：
- `crates/pg-ha-core/src/ha/election.rs` — `is_healthiest_node()` 方法
- `crates/pg-ha-core/src/sync.rs` — `SyncManager`、`compute_sync_standby_names()`
- `crates/pg-ha-core/src/cluster.rs` — `SyncState` 结构体
- `crates/pg-ha-core/src/ha/mod.rs` — `run_cycle()` 中 sync state 的使用

**代码中的 TODO**：
- `sync.rs:67` — `// TODO: compute actual lag`（max_lag_on_syncnode 过滤不完整）

---

## P1 — 高优先级改进

### 3. Raft 持久化层优化

**现状**：`crates/pg-ha-dcs/src/store.rs` 使用 JSON 文件持久化 Raft 状态。每次 `persist_log` 序列化整个 log BTreeMap 为 JSON → atomic write。日志量增长后性能会退化。  
**目标**：改为增量 append-only 写入，仅序列化新增 entries。  
**实现方向**：
- 方案 A：使用 append-only binary log file + 定期 compaction
- 方案 B：引入 `sled` 或 `rocksdb` 作为嵌入式 KV 存储
- 保留 `atomic_write_json` 用于 `hard_state.json`（体积小、写入频率低）

**相关文件**：
- `crates/pg-ha-dcs/src/store.rs` — `persist_log()`、`MemStore`
- `crates/pg-ha-dcs/Cargo.toml` — 添加新的存储依赖

---

### 4. Failsafe Key 持久化到 DCS

**现状**：`raft_dcs.rs:442` 有 `// TODO: read /failsafe key`。Failsafe 状态仅在内存中维护，节点重启后丢失。  
**目标**：将 failsafe 启用状态和成员列表持久化到 DCS 的 `/failsafe` key。  
**实现方向**：
- Leader 每次 HA cycle 将 failsafe 成员列表写入 DCS
- `get_cluster()` 时读取 `/failsafe` key 并反序列化

**相关文件**：
- `crates/pg-ha-dcs/src/raft_dcs.rs` — `get_cluster()` 方法，第 442 行附近
- `crates/pg-ha-core/src/failsafe.rs` — `Failsafe` 结构体

---

### 5. Proxy 连接池

**现状**：`crates/pg-ha-proxy/src/proxy.rs` 是纯 TCP pipe（accept → connect → bidirectional copy）。每个客户端连接对应一个后端连接。  
**目标**：支持连接复用（类似 PgBouncer transaction mode），降低后端连接数。  
**优先级说明**：如果用户已在使用 PgBouncer，可以将此项降为 P2。

**相关文件**：
- `crates/pg-ha-proxy/src/proxy.rs` — 核心代理逻辑
- `crates/pg-ha-proxy/src/lib.rs` — `PgProxy` 公开接口

---

## P2 — 中期改进

### 6. CI/CD 集成测试

**现状**：E2E 测试是 shell 脚本（`tests/e2e/`），GitHub Actions 仅做 release 构建。无自动化回归测试。  
**目标**：
- CI 中运行 `cargo test`（含 proptest）
- CI 中启动 docker-compose 并运行 E2E shell 脚本
- 增加 TLS 路径的集成测试

**相关文件**：
- `.github/workflows/release.yml` — 现有 CI 配置
- `tests/e2e/` — 所有 E2E 脚本
- `Makefile` — `make test`、`make e2e` 目标

---

### 7. TLS 证书自动轮换

**现状**：证书手动生成和部署，无自动轮换机制。  
**目标**：支持证书热重载（watch 文件变更 → reload TLS context），或集成 cert-manager/Vault。

---

### 8. REST API 增强

- 请求限流（rate limiting）— 防止管理端点被滥用
- Audit log — 记录谁在什么时间执行了什么管理操作
- CORS 配置（如果前端 dashboard 需要）

---

### 9. 备份/恢复集成

**现状**：无内建备份功能。  
**目标**：集成 pgBackRest 或 WAL-G，支持 PITR。至少提供 callback hook 让外部备份工具在 switchover/failover 时得到通知。

---

## P3 — 长期规划

### 10. 多区域 / Standby Cluster

**现状**：`crates/pg-ha-core/src/standby_cluster.rs` 已有框架代码，但未完整实现。  
**目标**：支持跨区域级联复制集群（DR 场景）。

### 11. Web Dashboard

提供集群可视化管理界面（拓扑图、实时监控、一键 switchover）。

### 12. 性能基准测试

- 建立 failover 时间基准（当前 E2E 测试验证 < 30s，目标 < 5s）
- Proxy 吞吐量/延迟基准
- Raft 写入吞吐量基准

---

## 已知代码 TODO 汇总

| 位置 | 内容 | 关联任务 |
|------|------|----------|
| `crates/pg-ha-core/src/sync.rs:67` | `// TODO: compute actual lag` | #2 同步复制约束 |
| `crates/pg-ha-dcs/src/raft_dcs.rs:442` | `// TODO: read /failsafe key` | #4 Failsafe 持久化 |

---

## 上下文提示

1. **指定要做的 TODO 编号**（如 "实现 TODO #1 REST API TLS"）
2. **关键入口文件**：
   - 总入口：`src/main.rs`
   - HA 循环：`crates/pg-ha-core/src/ha/mod.rs`
   - 配置定义：`crates/pg-ha-core/src/config.rs`
   - Raft DCS：`crates/pg-ha-dcs/src/raft_dcs.rs`
3. **构建和测试**：
   - `cargo build` — 编译
   - `cargo test` — 单元测试 + proptest
   - `make up` — 启动 3 节点 Docker 集群
   - `make e2e` — 运行 E2E 测试
4. **注意事项**：
   - 使用 Rust 2024 edition（`edition = "2024"`）
   - 启用了 `let-else`、`if-let chains` 等 nightly 特性
   - openraft 版本 0.9，API 与旧版差异较大
   - 日志使用 `tracing` crate，统一用 `info!`/`warn!`/`error!`
   - 错误处理用 `thiserror` 定义，函数返回 `Result<T>` 或 `anyhow::Result<T>`
