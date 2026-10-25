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

### 1a. 身份判定（L2–L5 的依据）

「本节点 PG 是不是主」只由观测决定，不由内存里记录的 role 决定（role 只记录 pg-ha 之前做过什么）。实现：`crates/pg-ha-core/src/ha/identity.rs`。

**观测**：每个 HA cycle 在确认 PG 运行后执行一次 `pg_is_in_recovery()`（超时 5 秒）。pg-ha 自己执行 start / stop / promote 后，观测作废，需重新观测。查询失败、超时或进程未运行都记为「未观测」。

**身份** = 观测 × `standby.signal` 是否存在：

| 身份 | 观测 | `standby.signal` |
|------|------|------------------|
| Primary | 不在 recovery | 不看 |
| ManagedStandby（pg-ha 配置的备库） | 在 recovery | 有 |
| UnmanagedRecovery（非 pg-ha 造成的恢复） | 在 recovery | 无 |
| Unknown | 未观测 | — |

`standby.signal` 只说明 PG 下次怎样启动，不说明它现在是什么，所以观测为「不在 recovery」时一律是 Primary。pg-ha 在以下场合写 `standby.signal`：clone、rejoin、switchover 降级、所有降级路径（停库之前），以及每一次不持锁时的启动（见下文「启动规则」）；正常选举路径上 PG promote 后自行删除它。因此「持锁且在 recovery 却没有 `standby.signal`」不会由 pg-ha 自身产生，只来自 `recovery.signal`（归档 / PITR）或运行中被删掉的 `standby.signal`。

**规则**：pg-ha 只 promote 它自己配置为备库的节点。非 pg-ha 造成的恢复交给运维：不 promote、不 rewind、不参选；若持锁则释放锁。

| 身份 | 持锁 | 他人持锁 | 无主（选举） |
|------|------|----------|--------------|
| Primary | 保持主 | rejoin（S4）；leader 还没有以主库运行时先停库 | 参选（有 rewind 检查标记时同样受 `/timeline` 约束，不能参选时先停库）；非最健康则按陈旧主停库，下次以备库启动 |
| ManagedStandby | promote，重新观测确认后才记为主 | 跟随；有 rewind 检查标记时先对 leader 跑 pg_rewind | 参选；有 rewind 检查标记时须 timeline ≥ `/timeline` |
| UnmanagedRecovery | 释放锁，交运维（pause 下保留锁，但不宣称主） | 交运维 | 不参选 |
| Unknown | 本轮不动作，不宣称主 | 有 `standby.signal`：等待；无：停库围栏（L5）。降级停库失败后，直到确认已停，一律按围栏处理（即使有 `standby.signal`） | 本轮不参选；降级停库失败后先停库 |

降级停库失败后，直到确认 PG 已停，以主库运行的节点在选举入口也先停库，不参选（见下文「降级」）。

**对外发布**：成员 `state` 在 DCS 与 REST 中一致。`running` 表示进程在运行且身份为 Primary 或 ManagedStandby；UnmanagedRecovery 与 Unknown 发布为 `unknown`，其他节点因此不把它当作候选、同步备或 switchover 目标。`role` 只在观测确认后才记为 `primary`；观测到在 recovery 时，记录的 `primary` 降为 `replica`。

#### 启动规则

**pg-ha 只在持有 leader lock 时以主库方式启动 PostgreSQL；其他情况一律以备库启动。**

- HA 循环只在一处启动 PG：`handle_postgres_not_running`。启动前若本节点不持锁（包括刚在这里因 PG 未运行而释放锁之后），先写 `standby.signal` 并 fsync 文件和目录；leader 已知、不是自己、且它的成员 `conn_url` 带 `host=` 时，同时把 `primary_conninfo` 指向它；leader 未知时保留 `postgresql.auto.conf` 里已有的 `primary_conninfo`。日志 `Starting PostgreSQL as a standby (not holding the leader lock)`。
- 写不成就不启动，返回错误，下一轮重试：停着的 PG 不丢数据，以可写主起来的 PG 违反 L5。日志 `Not starting PostgreSQL: could not write the standby configuration (<错误>). pg-ha starts PostgreSQL as a primary only while holding the leader lock; retrying next cycle`。
- 启动前读 `pg_controldata` 的 `Database cluster state`，判断上次是否以主库运行，决定是否写 rewind 检查标记（见下文）。写标记失败同样不启动。

  | `Database cluster state` | rewind 检查标记 |
  |--------------------------|----------------|
  | `in production`、`shut down`、`shutting down`、`in crash recovery`、`starting up` | 写（`Created rewind-check marker: pg_controldata reports '<state>' (last ran as a primary)`） |
  | 无法识别的值、`pg_controldata` 读取失败 | 写（读取失败时 WARN `Could not read the cluster state from pg_controldata (...); creating the rewind-check marker to be safe`） |
  | `shut down in recovery`、`in archive recovery` | 不写 |

  `shutting down` 是主库关机中途崩溃；`in crash recovery` 只出现在不带 `standby.signal` 的启动。不确定的状态按保守方向处理：多写一次标记只多一次 pg_rewind 检查（没有分叉时它立即报告 `no rewind required`）。clone 后第一次启动失败的节点，`pg_controldata` 显示的是从源端复制来的 `in production`，下次启动也会带上标记，代价同样只是一次检查。
- 其他启动入口：clone 后启动、pg_rewind 之后启动、重配上游后的重启都先写备库配置，写失败不启动。`POST /restart` 在持锁时行为不变；不持锁时先做与上面相同的准备（含 `pg_controldata` 标记），写失败则拒绝，PG 不动；处于 rewind 失败保持状态时拒绝。唯一例外是 bootstrap 中 initdb 的节点：它赢得了 `/initialize` 的原子竞争，是集群里唯一有数据的节点，initdb 出来的库没有可跟随的上游，启动后紧接着抢锁。
- 代价：主库崩溃或短暂 DCS 中断后，节点以备库重启，只能经选举 + promote 再成为主库（timeline +1）；整个集群冷启动也要经过选举。
- 节点停着且有 `recovery.signal` 时，pg-ha 启动它会同时写 `standby.signal`，备库模式优先，归档 / PITR 的恢复目标不会按预期执行。做归档恢复或 PITR 前先停 pg-ha。

#### 降级：先写 `standby.signal`，再停库

- `demote()`（续锁失败）、`handle_dcs_error` 的两个降级分支、Fence、switchover 在**停库之前**写 `standby.signal`，fsync 文件和目录；前两者以主库运行，同时写 rewind 检查标记。写失败仍然停库（L5 优先），记 ERROR、返回错误；之后启动规则拒绝在没有 `standby.signal` 时启动。
- 这条规则适用于所有停掉「可能是不持锁可写主」的 PG 的路径，不只上面几条：rejoin 前停掉以主库运行的 PG、rewind 源未就绪时停库、陈旧主停库、带标记且不能参选的主停库、rewind 失败保持中被手工启动的主、`POST /reinitialize` 停掉以主库运行的 PG、`POST /restart`（不持锁时）、优雅关停（释放锁之后停掉以主库运行或观测不到的 PG）、降级停库失败后重新围栏（含选举入口），都先写 `standby.signal`（以主库运行过的还写标记）再停库。`POST /reinitialize` 遇到这种 PG 且停库失败时，不删除数据目录。只读备库（观测在 recovery 且有 `standby.signal`）可以直接停库。
- 停库失败时，直到确认 PG 已停（`pg_ctl stop` 成功，或某一轮观察到 PG 不再运行），观测不到的 PG 一律按围栏处理：每轮重试停库，即使已有 `standby.signal`。原因是这个 signal 写在一个可能可写的 PG 上，不能再据此认为它不可写。
- 降级停库成功后，节点立即发布一次成员状态（`state=stopped`），让其他节点尽快看到它已停。发布有时限（不超过 `loop_wait`），DCS 不可达时不会拖住 HA 循环或关停。
- 降级时 leader 未知（`demote`、`handle_dcs_error`、无 candidate 的 switchover、优雅关停）就删除 `postgresql.auto.conf` 里的 `primary_conninfo`，不保留猜测。上游只由跟随路径按观测到的 leader 配置，包括 `postgresql.auto.conf` 里没有 `primary_conninfo` 的情况。
- 写备库配置时，`postgresql.auto.conf` 只替换 `primary_conninfo` 和 `recovery_target_timeline` 两项，其他行（`ALTER SYSTEM` 写入的内容，如 `synchronous_standby_names`）一律保留；文件经临时文件 + rename 写入并 fsync。成员 `conn_url` 没有 `host=` 时按 leader 未知处理，不写指向 `127.0.0.1` 的 `primary_conninfo`。节点每次离开主库角色或重写备库配置后，pg-ha 都忘掉上次应用的 `synchronous_standby_names`，下一个主任期第一次一定重新 `ALTER SYSTEM` 并写 `/sync`（C4）。

#### rewind 检查标记（`PGDATA/pg-ha.rewind_check`）

- 含义：本节点可能有分叉点之后的 WAL，跟随前须用 pg_rewind 验证。标记只决定是否做 rewind 检查，不决定身份，也不改变健康判定。
- 谁写：降级路径（上一节）、启动规则（`pg_controldata` 显示上次以主库运行）。谁删：pg_rewind 成功（含 `no rewind required`）之后（日志 `pg_rewind succeeded; removed the rewind-check marker`）；clone 和 pg_rewind 从源端复制过来的 `pg-ha.rewind_check`、`pg-ha.rewind_failed` 也会被删，它们描述的是源端；`POST /reinitialize` 随数据目录一起删除。
- 带标记的 ManagedStandby 在他人持锁且 leader 就绪时走 rejoin：停本地 PG，对 leader 跑 pg_rewind，再以备库启动跟随（日志 `Rewind-check marker present and <leader> holds the leader lock — checking with pg_rewind before following`）。leader 未就绪时保持只读备库运行等待（`rewind-check marker present; waiting for <leader> to run as a primary before pg_rewind`）。
- 「leader 就绪」：DCS 视图中它持锁，且成员记录为 `state=running`、`role=primary`；**并且**用它的 `conn_url` 实时连接执行 `SELECT pg_is_in_recovery()` 得到 `f`（5 秒连接超时，10 秒总超时；连不上、超时或答 `t` 都算未就绪）。实时确认做两次：停本地 PG 之前一次，停库之后、跑 pg_rewind 之前再一次。这样源端刚停库而 DCS 里的成员记录还没更新的几秒内，以及本地停库（大库上要刷盘，可能很慢）期间源端被降级时，都不会对它跑 pg_rewind。
- 所有 pg_rewind 都只以就绪的锁持有者为源。源未就绪时不跑 pg_rewind、不算失败、不进入「rewind 失败」保持：日志 INFO `Not running pg_rewind: <leader> <原因>`；本地 PG 若是主库身份先停库（先写 `standby.signal` 和标记）；第二次确认失败时 PG 已停，保持停止，下一轮以备库启动后再等。
- 没有锁持有者时发现的陈旧主（以主库运行、不是最健康节点）只停库，不 rewind；下次以备库启动（带标记），等 leader 出现后再 rewind。如果它是唯一可当选的节点，就按带标记节点的规则参选。
- 节点在成员数据里发布 `rewind_check`（标记文件是否存在），供其他节点在选举和 switchover 中使用（见下一小节）。老版本节点不发布这个字段，按未带标记处理；滚动升级期间老节点仍可能让位给不会参选的新版本带标记节点。
- **已知窗口（不在任务 3 修改，登记在第 9 节）**：带标记的备库在等待 rewind 检查期间（leader 未就绪、分区期间、恢复网络后 pg_rewind 之前）仍以只读备库运行，`/patroni` state 为 `running`，满足 `/replica` 200 的判定（H2）。经 RO 端口的只读客户端因此可能读到只存在于分叉历史上的数据，例如旧主失锁前接受、新主上没有的写入。

#### 集群 timeline 记录（`/timeline`）与带标记节点的选举

- key：`/<namespace>/<scope>/timeline`，值为十进制字符串（如 `"3"`），**无 TTL**。
- 单调不减：写入用 CAS，只在新值大于当前值时写。
- 写入者：只有持锁节点写。
- 写入时机：
  1. promote 之前写 `max(自身成员 timeline, 当前值) + 1`。同一个持锁任期内 promote 重试复用这个值，不再加 1。写不成就不 promote（返回错误，下一轮重试；DCS 写抖动会让 failover 推迟一轮）。
  2. 确认 promote（观测不在 recovery）后写实际 timeline，取主库上的精确值（`pg_walfile_name(pg_current_wal_lsn())` 的前 8 位十六进制）。
  3. Hold（持锁且为主）时，key 缺失或小于实际 timeline 则补写。
  
  key 可能因 promote 失败而高于实际 timeline（Hold 时 WARN `DCS /timeline (<k>) is higher than this primary's timeline (<tl>); a promote attempt recorded it in advance`，同一对值只打一次）。偏高只会让带标记节点更严格。
- 选举规则：带 rewind 检查标记的节点，只有在**自身成员记录的 timeline ≥ `/timeline`** 时才参选；否则不参选，等看到 leader 后 rewind（INFO `Not taking part in the election: rewind-check marker present and this node's timeline <own> is behind the cluster timeline <cluster>; waiting for a leader to rewind against`）。key 不存在（升级来的集群）时改为与其他成员发布的 timeline 比较，并 WARN `No /timeline key in DCS; comparing this node's timeline with the timelines members published. The evidence is incomplete`。自身 timeline 还没发布时本轮不参选。
- 成员记录的 timeline 来自 `pg_control_checkpoint()`，在副本上、以及 promote 后 checkpoint 完成之前的主库上可能落后于实际 timeline，所以这个比较只会更严格。用成员发布值而不是本地值，是因为其他节点判断这个节点能否参选时只能看到发布值，两边必须得出同一结论。
- 带标记、不能参选的节点如果正以主库运行，先停库（L5，先写 `standby.signal` 和标记）。
- 其他节点用同一规则判断带标记的 peer：不能参选的 peer 不计入「谁最健康」的比较，避免所有节点都让位给一个不会参选的节点。`POST /switchover`、`POST /failover` 拒绝带标记的 candidate（409，`Candidate '<c>' has a rewind-check marker (not yet verified with pg_rewind)`）。`/failover` 里已经指定的 candidate 之后才带上标记、且按此规则不能参选时，其他节点 WARN `Designated candidate <c> has a rewind-check marker and may not take part in the election; proceeding as if no candidate were designated`，按没有指定 candidate 继续选举。
- 不带标记的节点自己不加 timeline 检查（一般的 timeline 感知选举见第 9 节）。
- 后果：如果只剩带标记且 timeline 落后的节点，集群不会自动选出主库。运维确认接受数据丢失后，可删除该节点的 `pg-ha.rewind_check` 让它参选。

#### pg_rewind 失败

- pg_rewind 必须执行，并且自动执行。
- pg_rewind 失败，或 pg_rewind 成功但随后 PG 启动失败时，默认（`postgresql.remove_data_directory_on_rewind_failure: false`）：PG 保持停止；数据目录不删除也不重建；写 `PGDATA/pg-ha.rewind_failed`；不参选；不自动重试。rewind 检查标记：pg_rewind 失败时保留；pg_rewind 成功后 PG 启动失败时，标记已在 rewind 成功后删除，只留 `pg-ha.rewind_failed`。进入保持时 pg-ha 先确认 PG 已停，再记一条 ERROR，之后每 60 秒最多重复一次：

  ```text
  <pg_rewind | Starting PostgreSQL after pg_rewind> failed; PostgreSQL is stopped and its data directory is kept (pg-ha did not delete or rebuild it). Cause: <原因>. Data directory: <dir> (<大小>). pg-ha will not start, rewind or rebuild this node on its own. Operator action required: call POST /reinitialize on this node to delete the local data and copy it again from the primary with pg_basebackup (a full copy, about <大小>); or set postgresql.remove_data_directory_on_rewind_failure: true and restart pg-ha to let it rebuild automatically. If the cause was temporary, delete <dir>/pg-ha.rewind_failed to let pg-ha retry pg_rewind once (no pg-ha restart needed).
  ```

  `<原因>` 取自 pg_rewind（或 `pg_ctl start`）的错误输出，只保留 `error:` / `fatal:` 行（没有时保留全部），多行用 ` | ` 连接；`<大小>` 是数据目录下文件大小之和，限时 2 秒统计，超时为 `size unknown`，有读不了的子目录时为 `at least <大小>`。

  日志写「data directory is kept」而不是「unchanged」：pg_rewind 在复制阶段失败时可能已经改动了目录，pg-ha 只能保证自己没有删除或重建它。这也是保持期间不自动启动的原因。
- 处理：`POST /reinitialize`；或打开该选项后重启 pg-ha；原因是暂时性的、且 pg_rewind 在改动目录之前就失败（例如连接错误）时，删除 `pg-ha.rewind_failed`：不需要重启 pg-ha（日志 `Rewind hold file <dir>/pg-ha.rewind_failed was removed; pg-ha will retry pg_rewind`），下一轮以备库启动该节点。pg_rewind 失败的情况（标记还在）：看到 leader 就绪后再跑一次 pg_rewind，再失败就重新进入保持。pg_rewind 成功后启动失败的情况（标记已删）：先以备库重试启动，连续 3 次启动失败后才对 leader 再跑一次 pg_rewind。
- 保持期间 PG 被人手工启动也会被 pg-ha 停掉。保持状态记在 `pg-ha.rewind_failed` 里，跨 pg-ha 重启有效；这个文件写失败时保持只在内存里，持续到 pg-ha 重启（ERROR `Could not write <dir>/pg-ha.rewind_failed (...); the hold lasts until pg-ha restarts`）。
- 打开选项时恢复旧行为：删除数据目录后 pg_basebackup（WARN `pg_rewind failed: <错误> — remove_data_directory_on_rewind_failure is enabled, rebuilding with pg_basebackup`）。
- 不算失败的情况：源未就绪（上一小节）；pg_rewind 成功后 `pg_ctl start -w` 等待超时、但 postmaster 仍在运行（备库回放到一致点之前不接受连接，大库上常见），只 WARN `PostgreSQL is still starting after pg_rewind (...); not treating this as a failure`，下一轮按观测继续。「PG 启动失败」只指 postmaster 没有起来。

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

**H1 / H2 的逐条判定**（`crates/pg-ha-api/src/state.rs`，数据最多滞后一个 HA cycle）：

- `/primary` 200 ⟺ 持有 leader lock ∧ `state = running` ∧ `role = primary` ∧ 观测不在 recovery。观测缺失时为 503。
- `/replica` 200 ⟺ `state = running` ∧ `role = replica` ∧ 观测在 recovery（再叠加 `lag` 阈值）。观测为不在 recovery 的节点，不论记录的 role 是什么，都不作为副本对外服务。
- `/health` 只反映 PG 进程是否运行，与 HA 身份无关。

**稳态检查**：运维指南中 Primary / Replica / sync / async 的 curl 示例；三节点交叉探测 H3；`GET /patroni` 的 `pg_in_recovery` 字段即本节点最近一次观测。

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

**例外（R2，任务 3）**：带 rewind 检查标记的节点参选时，比较的是成员发布的 timeline（来自 `pg_control_checkpoint()`，可能落后）与 `/timeline`（持锁主库写入的精确值，`pg_walfile_name(pg_current_wal_lsn())`）。偏差只会让带标记节点更严格，不会放宽；所有节点判定同一个带标记节点时用的都是它发布的同一个值（见 1a）。

**说明（R3）**：当前实现由 `touch_member` 将 superuser 密码写入 `conn_url` 并进入 Raft 日志；多数运行路径（Proxy、重配 upstream）仅需 host/port，密码来自本地配置。产品语义应二选一写死，避免「磁盘有密、代码有时又不用」的双故事。

**相关**：架构 DCS KV `members/*`；运维集群成员字段。

---

## 6. 控制面持久化与进程恢复

| ID | 陈述 | 状态 |
|----|------|------|
| P1 | 进程崩溃重启后，从本地磁盘恢复的 Raft 日志与状态机，须能重建与崩溃前**已提交**语义一致的集群视图（未提交日志可丢，符合 Raft）。 | 持有 |
| P2 | 日志存储实现从全量 JSON 改为 append-only WAL 等优化时，必须保持 P1 与对外 HA 语义不变（M2）。 | 持有 |
| P3 | Failsafe：文档中的 `/failsafe` 与「重启后是否仍生效」必须一致。 | **待裁定** |
| P4 | 本地持久化（含 WAL）若失败，不得长期呈现「内存/Raft 已成功、磁盘未成功」且无 degraded 语义。 | 持有 |

**P4 的 degraded 语义**（`crates/pg-ha-dcs/src/store.rs`）：

- 存储 trait 方法（`save_vote`、`save_committed`、`append_to_log`、`delete_conflict_logs_since`、`purge_logs_upto`、`apply_to_state_machine`、`install_snapshot`）在落盘失败时返回 `StorageError`，不再返回 `Ok`。openraft 因此不会把这次写当成已持久化去推进 commit 或应答客户端。
- openraft 收到 `StorageError` 后停止该节点的 Raft core（`Fatal`），之后所有 Raft 写都失败。进程不退出，日志有 `ERROR Raft storage persistence failed (...)`。
- 对 HA 的后果：`get_cluster` 读本地状态机，会返回冻结的旧视图，但所有改变身份的动作（抢锁、续锁）都要经过 Raft 写，因此该节点既不能成为 leader，也不能保持 leader。若它正持锁，下一次续锁失败（`Ok(false)` 或 `Err`）即降级：先写 `standby.signal` 和 rewind 检查标记、再停库；之后 pg-ha 只把它作为备库启动（1a 启动规则）。本地视图里 leader key 按 TTL 过期后，它最多以只读备库参选，抢锁需要 Raft 写而必然失败，所以不会再以可写主出现。集群其余多数派继续服务。
- 恢复需要人工：修复磁盘后重启 pg-ha。内存先于磁盘更新这一顺序未变（WAL 压缩需要完整内存日志），但分歧不会被报告为已持久化。

**相关**：架构 Raft / DCS；`raft_log.wal` 与 `hard_state.json` 等；代码中 failsafe TODO。

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
| switchover 释放锁失败被忽略 | S1 | `delete_leader` 结果被忽略；连续失败时旧主带 `standby.signal` 重启后仍持锁，会 promote 自己，撤销 switchover |
| switchover 与优雅退出先释放锁再停库 | L5 / H3 | `ha/commands.rs` `handle_switchover_command` 与 `src/main.rs` 关停 Phase 3 → Phase 4：释放锁后到停库完成之间旧主仍可写，候选可能已 promote，存在短暂双可写窗口。（任务 3 起两者都在停库前写 `standby.signal`，停库失败时持续围栏或记 ERROR，但释放锁仍在停库之前。） |
| 无 candidate 的 switchover 不排除旧 leader | S1 | failover key 为 `{"leader": old, "candidate": null}`，选举只看 `candidate`（`ha/election.rs` `process_unhealthy_cluster`），不看 `Failover.leader`（`cluster.rs`）；旧主带 `standby.signal` 参选，WAL 往往打平或最高，可能重新当选，switchover 被悄悄撤销 |
| 选举不比较 timeline | S2 / S4 | `is_healthiest_node` 只比较 WAL 位置（`ha/election.rs` 中 peer 的 timeline 未使用）；任务 3 只对带 rewind 检查标记的节点按 `/timeline` 把关，一般节点的 timeline 感知选举另开项 |
| 带标记、等待 rewind 检查的备库对外是健康副本 | H2 / S4 | 带 `pg-ha.rewind_check` 的 ManagedStandby 在 leader 未就绪、分区期间、恢复后 pg_rewind 之前以只读备库运行，`/replica` 200，RO 客户端可能读到只存在于分叉历史上的数据（见 1a「rewind 检查标记」）。候选修法：带标记的节点 `/replica` 返回 503 |
| 分区节点重新加入 Raft 会引发 Raft 重选 | L4 / L5 | openraft 0.9 没有 pre-vote：分区节点的 term 在分区期间上涨，重新连上后迫使集群重选 Raft leader；重选期间当前锁持有者续锁失败而被降级（多一次 failover，timeline +1）。任务 3 在 docker 集群上复现分区场景时每次都出现，不在任务 3 处理 |

---

## 10. 修订记录

| 日期 | 说明 |
|------|------|
| 2026-10-25 | 任务 3：1a 增加启动规则、降级先写 `standby.signal`（适用于所有停掉不持锁可写主的路径）、rewind 检查标记、`/timeline` 与带标记节点的选举规则、rewind 失败处理；R2 增加带标记节点 timeline 比较的例外说明；修正 P4 的 degraded 语义；第 9 节删除已闭合的「降级不写 standby.signal」，登记 5 项。实现分 5 个提交，完成前以本文为目标语义；第 5 个提交补充 rewind 失败日志里原因与数据目录大小的取法；并写明 pg_rewind 成功后 PG 启动失败进入保持时 rewind 检查标记已删除、删除保持文件后先以备库重试启动（连续 3 次失败后再 pg_rewind） |
| 2026-10-21 | 新增 1a 身份判定：「是否为主」以 `pg_is_in_recovery()` 观测为准；只 promote pg-ha 配置的备库，非 pg-ha 造成的恢复交运维；写明 H1 / H2 逐条判定与 `/health` 语义；第 9 节登记两个已知缺口 |
| 2026-10-21 | P4 闭合为持有：Raft 存储落盘失败返回 `StorageError`，节点 Raft 停止而非继续推进；写明 degraded 语义与人工恢复方式 |
| 2026-09-11 | S5 闭合为持有：sync 模式下 Failover/Switchover 强制 `/sync` 成员资格；空名单不晋升 |
| 2026-07-18 | 初稿：从架构 / 运维文档抽取闭环不变量，并标注持有 / 目标 / 待裁定 |
