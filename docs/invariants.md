# pg-ha 闭环不变量

> **定位**：本文件是控制面语义的**契约源**。实现与文档冲突时，先裁定本文件（或同步修订本文件），再改代码。  
> **配套**：状态机与数据流见 [pg-ha-architecture.md](pg-ha-architecture.md)；稳态如何手工验证见 [operations-guide.md](operations-guide.md)。

## 元规则

| ID | 陈述 |
|----|------|
| M0 | 设计文档是语义源；代码与文档冲突时先裁定文档再改实现。 |
| M1 | 任一行为变更必须声明影响面：DCS / PG 角色 / 复制与配置 / 健康检查 / 重启恢复 / 代理路由。任一非空列须写清旧语义 → 新语义 → 如何证明边界仍闭合。 |
| M2 | 「行为等价」改动（如 Raft 日志 WAL 化）不得改变本文件对外可观察语义；有意改变必须先修订本文件。 |
| M3 | 验收不以「单测绿」为充分条件；必须以对应故事的多平面交叉检查为准（PG + DCS + HTTP 健康检查，必要时含 Proxy）。 |

每条不变量建议附带：

- **稳态检查**：如何在运行中验证  
- **相关文档**：架构 / 运维章节  
- **状态**：`持有`（实现应对齐） / `目标`（文档已承诺、实现未闭合） / `待裁定`（实现与文档意图不一致，需先定语义）

---

## 1. Leader Lock ↔ PostgreSQL 角色

| ID | 陈述 | 状态 |
|----|------|------|
| L1 | 集群在任意稳态至多一个**未过期**的 `/leader`（leader lock）。 | 持有 |
| L2 | 持有 leader lock 的节点上，PostgreSQL **不在** recovery（可写 Primary）。 | 持有 |
| L3 | 未持有 lock、且对外以副本身份服务的节点，PostgreSQL **处于** recovery；或处于文档定义的过渡态（rewind / rejoin / bootstrap），过渡态不得对外宣称 Primary。 | 持有 |
| L4 | DCS `members/{name}.role`（及 HA 内存视图）与该节点真实 PG 角色一致；允许至多约一个 HA cycle 的传播延迟，延迟上限应可观测。 | 持有 |
| L5 | 节点失去 leader lock 后，不得继续以可写 Primary 对外服务（须停库、拒绝写、或进入 rejoin 路径之一；具体策略以实现为准，但结果必须满足本条）。 | 持有 |

**稳态检查（示例）**

- DCS / `GET /cluster`：leader 唯一  
- 持锁节点：`SELECT pg_is_in_recovery()` → `f`  
- 非持锁副本：`pg_is_in_recovery()` → `t`

**相关**：架构图「HA 决策循环」「选举流程」；运维「集群状态检查」。

---

## 2. 健康检查 ↔ 身份

健康检查是对外叙事；必须与 L\* 同一故事结局。

| ID | 陈述 | 状态 |
|----|------|------|
| H1 | `GET /primary` 返回 200 ⟺ 本节点持有 leader lock，且 PG 为健康 Primary。 | 持有 |
| H2 | `GET /replica` 返回 200 ⟺ 本节点为健康 Replica（可选 `lag` 阈值满足时）。 | 持有 |
| H3 | 同一时刻，集群中至多一个节点对 `/primary` 返回 200。 | 持有 |
| H4 | TCP Proxy：RW 端口健康探测依赖 `/primary`；RO 依赖 `/replica`（及文档规定的 fallback）。 | 持有 |
| H5 | 当 `synchronous_mode` 为 false，或 DCS `/sync` 为空（等价未启用同步名单）时：所有健康 Replica 对 `/async` 为 200、对 `/sync` 为 503；Primary 对二者均为 503。 | 持有 |
| H6 | 当 `synchronous_mode` 为 true 且 `/sync` 已发布时：`/sync` 返回 200 的节点集合 = `/sync.sync_standby` 解析结果与健康 Replica 的交集；其余健康 Replica 对 `/async` 为 200。 | 持有 |
| H7 | Switchover / Failover 完成后，在约定时限内（建议：≤ `2 * loop_wait` + promote/重配耗时）重新满足 H1–H4；若启用同步复制，同时满足 H6。 | 持有 |

**稳态检查**：运维指南中 Primary / Replica / sync / async 的 curl 示例；三节点交叉探测 H3。

**相关**：架构「认证与安全 / 开放端点」；运维「同步 / 异步备库检查」；架构「同步复制数据流」「TCP Proxy 健康检查」。

---

## 3. 切换故事（Switchover / Failover）

| ID | 陈述 | 状态 |
|----|------|------|
| S1 | **Switchover**：在候选可接位的前提下，旧主降级并释放 lock → 新主获取 lock 并 promote → 副本 upstream 指向新主；完成后 L\* 与 H\* 成立。 | 持有 |
| S2 | **Failover**：在 lock 过期或主库确认不可用后，仅合格候选参与选举；胜出者 promote 并写入 lock；完成后 L\* 与 H\* 成立。 | 持有 |
| S3 | 切换完成后，`GET /cluster`、成员 role/state、以及 history 中的事件与真实拓扑一致。 | 持有 |
| S4 | 旧 Primary 回归：不得形成双主；须走检测 → rewind / basebackup / rejoin 故事，最终成为指向当前 leader 的 Replica（或维护态）。 | 持有 |
| S5 | 当 `synchronous_mode` 启用时，Failover / Switchover 候选必须属于 DCS `/sync.sync_standby`；`/sync` 缺失、空或为 `*` 时任何节点均不得晋升。`synchronous_mode_strict` 仅影响写阻塞，不参与选举。 | 持有 |

**相关**：架构「Failover 时序」「选举流程」；运维「手动 Switchover / Failover」；架构同步复制章节「当前范围与限制」。

---

## 4. 动态配置 ↔ PostgreSQL ↔ DCS

| ID | 陈述 | 状态 |
|----|------|------|
| C1 | DCS `/config` 的变更按文档「生效规则」被 HA cycle 消费（HA 参数下一 cycle；PG reload / restart pending 分类正确）。 | 持有 |
| C2 | 仅**持有 leader lock 的 Primary** 将同步复制计算结果应用到 PG，并写入 DCS `/sync`。 | 持有 |
| C3 | `synchronous_mode=false`：Primary 使 `synchronous_standby_names` 为空（或等价禁用），且 `/sync` 为空语义。 | 持有 |
| C4 | `synchronous_mode=true`：`SyncManager` 输出、PG `synchronous_standby_names`、DCS `/sync` 三者语义一致；目标未变则不重复 `ALTER SYSTEM` + reload。 | 持有 |
| C5 | 需 restart 才生效的 PG 参数：只设置 `pending_restart`（或等价标志），不得对外表现为「已生效」。 | 持有 |

**相关**：架构「动态配置流程」「同步复制数据流」；运维「动态配置变更」「同步复制」。

---

## 5. 复制拓扑与连接信息

| ID | 陈述 | 状态 |
|----|------|------|
| R1 | 稳态下，Replica 的复制上游（`primary_conninfo` 或级联上游）指向当前 leader，或文档明确允许的 cascade 源。 | 持有 |
| R2 | 成员发布的 timeline / wal_position 与选举、lag 判断所依据的规则一致（同一套度量，禁止各读各的）。 | 持有 |
| R3 | 跨节点 clone / rewind / basebackup 所需的地址信息来自 DCS 成员（至少 host/port）；**密码是否写入 DCS** 必须在文档中唯一裁定，并与实现一致。 | **待裁定** |

**说明（R3）**：当前实现由 `touch_member` 将 superuser 密码写入 `conn_url` 并进入 Raft 日志；多数运行路径（Proxy、重配 upstream）仅需 host/port，密码来自本地配置。产品语义应二选一写死，避免「磁盘有密、代码有时又不用」的双故事。

**相关**：架构 DCS KV `members/*`；运维集群成员字段。

---

## 6. 控制面持久化与进程恢复

| ID | 陈述 | 状态 |
|----|------|------|
| P1 | 进程崩溃重启后，从本地磁盘恢复的 Raft 日志与状态机，须能重建与崩溃前**已提交**语义一致的集群视图（未提交日志可丢，符合 Raft）。 | 持有 |
| P2 | 日志存储实现从全量 JSON 改为 append-only WAL 等优化时，必须保持 P1 与对外 HA 语义不变（M2）。 | 持有 |
| P3 | Failsafe：文档中的 `/failsafe` 与「重启后是否仍生效」必须一致。 | **待裁定** |
| P4 | 本地持久化（含 WAL）若失败，不得长期呈现「内存/Raft 已成功、磁盘未成功」且无 degraded 语义。 | **目标** |

**相关**：架构 Raft / DCS；`raft_log.wal` 与 `hard_state.json` 等；代码中 failsafe / WAL 写失败 TODO。

---

## 7. 暂停、维护与 Watchdog

| ID | 陈述 | 状态 |
|----|------|------|
| W1 | `pause`（或等价维护模式）下：不自动抢锁 / 不自动 failover（与运维「暂停」语义一致）。 | 持有 |
| W2 | Watchdog 仅在「本节点自认可写 Primary」路径喂狗；失去 lock 或不再是 Primary 时必须停止喂狗，防止僵尸主。 | 持有 |

**相关**：运维暂停与维护；架构 / 代码 `watchdog.rs`。

---

## 8. 变更时的影响面检查表（强制习惯）

提交或开始实现前，复制下表并勾选：

```text
变更简述：

[ ] DCS keys / Raft 日志语义
[ ] PostgreSQL 角色或恢复态
[ ] 复制拓扑 / synchronous_* / primary_conninfo
[ ] HTTP 健康检查 (/primary /replica /sync /async)
[ ] Proxy 路由后果
[ ] 进程重启后是否仍成立（P1–P4）
[ ] 是否需要修订本文件或 architecture / operations

交叉验收故事（至少一条）：
[ ] 稳态三节点
[ ] Switchover
[ ] Failover
[ ] 旧主回归
[ ] 开关 synchronous_mode
[ ] 其它：________
```

---

## 9. 已知不闭合项（排期用）

| 项 | 相关 ID | 说明 |
|----|---------|------|
| Failsafe 持久化 | P3 | 架构列出 `/failsafe`；实现仍有「未读 key」类缺口 |
| Member `conn_url` 含密码 | R3 | 落盘与最小密钥面目标冲突，需先裁定再改 |
| WAL 写失败仅告警 | P4 | 与「提交成功」语义未完全闭合 |

---

## 10. 修订记录

| 日期 | 说明 |
|------|------|
| 2026-09-11 | S5 闭合为持有：sync 模式下 Failover/Switchover 强制 `/sync` 成员资格；空名单不晋升 |
| 2026-07-18 | 初稿：从架构 / 运维文档抽取闭环不变量，并标注持有 / 目标 / 待裁定 |
