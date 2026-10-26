# pg-ha 系统架构图

## 整体架构

```mermaid
graph TB
    subgraph Agent["pg-ha Agent Process"]
        HA[HA Engine - 决策循环]
        RAFT[Raft DCS - 嵌入式共识]
        API[REST API :8008]
        PROXY[TCP Proxy RW:6432 RO:6433]
    end

    PG[(PostgreSQL :5432)]
    CLIENT[Client Connections]

    HA -->|pg_ctl| PG
    HA -->|read/write lock| RAFT
    HA -->|update backends| PROXY
    API -->|commands| HA
    PROXY -->|TCP pipe| PG
    CLIENT --> PROXY
    CLIENT --> API
```

## HA 决策循环 (run_cycle)

```mermaid
flowchart TD
    START[run_cycle] --> CMD[处理管理命令]
    CMD --> DCS{从 DCS 加载集群状态}
    DCS -->|失败| ERR[handle_dcs_error]
    DCS -->|成功| DYNCONF[检测动态配置变更]
    DYNCONF --> DATADIR{数据目录为空?}
    DATADIR -->|是| BOOTSTRAP[Bootstrap initdb/clone]
    DATADIR -->|否| PGRUN{PG 运行中?}
    PGRUN -->|否| PGSTART[有 rewind 保持文件: 不启动<br/>否则写 standby.signal 和按 pg_controldata 写标记<br/>以备库启动; 写失败不启动<br/>只有持锁时以主库启动]
    PGRUN -->|是| OBSERVE[观测 pg_is_in_recovery<br/>得出身份]
    OBSERVE --> LOCKED{集群有 Leader?}
    LOCKED -->|unlocked| ELECTION[选举流程]
    LOCKED -->|locked| OWNER{我持有 lock?}
    OWNER -->|是| RENEW[续约 enforce_primary]
    OWNER -->|否| FOLLOW[follow_upstream]
```

之后的每个分支都按身份（Primary / ManagedStandby / UnmanagedRecovery / Unknown）决策，不依据内存里记录的 role。完整决策表见 [invariants.md](invariants.md) 1a 节。

**时间界**：
- 主循环每 `loop_wait` 秒启动一轮；一轮超时后下一轮立即开始，错过的 tick 不连发（`MissedTickBehavior::Delay`）。
- 每次 DCS 写最多等 `retry_timeout` 秒，超时按写失败处理（openraft 0.9 的 Raft leader 失去多数派后不退位，不设上限时续锁会一直挂到网络恢复）。
- 持锁节点维护 leader 租约：每次抢锁 / 续锁开始前记录锚点（单调时钟和墙钟），成功后截止时间 = 锚点 + 写入锁的 TTL − `watchdog.safety_margin`。每轮周期与这个截止时间赛跑（`Ha::run_cycle_bounded`）：截止时间先到就放弃本轮，先写 `standby.signal` 和 rewind 检查标记，再 `pg_ctl stop -m immediate`。`pg_ctl promote` 之前和保持主之前也检查租约。pause 下不围栏。
- 参数约束 `ttl >= loop_wait + retry_timeout + safety_margin` 保证健康的主库能在租约内完成一轮周期和一次续锁。完整规则见 [invariants.md](invariants.md) L5 与 1a「leader 租约」。

## 选举流程 (process_unhealthy_cluster)

```mermaid
flowchart TD
    START[集群 unlocked] --> PAUSE{暂停模式?}
    PAUSE -->|是| NOOP[不操作]
    PAUSE -->|否| IDENT{身份?}
    IDENT -->|UnmanagedRecovery| ASIDE[不参选 交运维]
    IDENT -->|Unknown| SKIP[本轮不参选]
    IDENT -->|降级停库失败未确认| FENCE[先停库 不参选]
    IDENT -->|Primary / ManagedStandby| MARK{带 rewind 检查标记且 timeline < /timeline?}
    MARK -->|是| ASIDE2[不参选 以主库运行则先停库<br/>等 leader 后 rewind]
    MARK -->|否| STALE{观测为主且非最健康?}
    STALE -->|是| STOPSTALE[停库 下次以备库启动]
    STALE -->|否| FAILOVER{有 failover key 指定 candidate?}
    FAILOVER -->|是 我是candidate| ACQUIRE[尝试获取 leader lock]
    FAILOVER -->|是 我不是| DEFER[让步给 candidate<br/>candidate 带标记且不能参选时按无 candidate 处理]
    FAILOVER -->|否| HEALTH{is_healthiest_node?<br/>先比 timeline 再比 LSN<br/>不计入不能参选的带标记 peer<br/>也不计入 switchover 点名退位的旧主}
    HEALTH -->|是| ACQUIRE
    HEALTH -->|否| WAIT[等待选举结果]
    ACQUIRE -->|成功| PROMOTE[ManagedStandby: promote 前写 /timeline<br/>pg_ctl promote<br/>重新观测确认后记为主]
    ACQUIRE -->|失败| WAIT
```

带 rewind 检查标记的节点能否参选、`/timeline` 的写入时机见 [invariants.md](invariants.md) 1a「集群 timeline 记录」。

## Failover + Rejoin 完整流程

```mermaid
sequenceDiagram
    participant P as Primary (node1)
    participant R1 as Replica (node2)
    participant R2 as Replica (node3)
    participant DCS as Raft DCS

    Note over P: Primary 宕机
    P->>P: 进程终止

    Note over DCS: Leader lock TTL 过期 (15s)
    R1->>DCS: get_cluster() → unlocked
    R1->>R1: is_healthiest_node() = true
    R1->>DCS: attempt_to_acquire_leader()
    DCS-->>R1: OK (获取成功)
    R1->>DCS: /timeline = tl+1 (promote 前, 写不成不 promote)
    R1->>R1: pg_ctl promote
    R1->>R1: 重新观测 pg_is_in_recovery() = f
    R1->>DCS: /timeline = 实际 timeline
    Note over R1: 成为新 Primary

    R2->>DCS: get_cluster() → leader=node2
    R2->>R2: follow_upstream(node2)
    R2->>R2: 检测 primary_conninfo 变化
    R2->>R2: reconfigure → 重启 PG 指向 node2

    Note over P: 旧 Primary 重启
    P->>DCS: get_cluster() → leader=node2
    P->>P: 不持锁: 写 standby.signal<br/>pg_controldata = in production → 写 rewind 检查标记
    P->>P: pg_ctl start (standby 模式)
    P->>DCS: node2 持锁且 running/primary
    P->>R1: SELECT pg_is_in_recovery() → f (实时确认)
    P->>P: pg_ctl stop
    P->>R1: SELECT pg_is_in_recovery() → f (停库后再确认)
    P->>P: pg_rewind (从 node2)
    alt pg_rewind 成功
        P->>P: 删除标记, 写入 standby.signal + primary_conninfo
        P->>P: pg_ctl start (standby 模式)
        Note over P: 自动 streaming 追赶数据
    else pg_rewind 失败
        P->>P: 停库, 保留数据目录, 写 pg-ha.rewind_failed
        Note over P: 等运维: POST /reinitialize 或打开<br/>remove_data_directory_on_rewind_failure
    end
```

## 动态配置流程

```mermaid
flowchart LR
    subgraph REST_API["REST API"]
        GET[GET /config]
        PUT[PUT /config 全量替换]
        PATCH[PATCH /config 部分更新]
    end

    subgraph DCS_STORE["DCS Raft"]
        CONFIG["/config key JSON"]
    end

    subgraph HA_LOOP["HA Loop 每 cycle"]
        DETECT[检测配置变更]
        HA_APPLY[HA 参数立即应用]
        PG_RELOAD[PG reload 参数]
        PG_RESTART[PG restart pending]
    end

    PUT --> CONFIG
    PATCH --> CONFIG
    GET --> CONFIG
    CONFIG --> DETECT
    DETECT --> HA_APPLY
    DETECT --> PG_RELOAD
    DETECT --> PG_RESTART
```

## TCP Proxy 健康检查

```mermaid
flowchart TD
    subgraph HealthChecker["Health Checker 每3s"]
        HC[HTTP HEAD 请求]
        RW_CHECK[HEAD /primary]
        RO_CHECK[HEAD /replica]
        FALL[连续3次失败 = DOWN]
        RISE[连续2次成功 = UP]
    end

    subgraph Routing["Connection Routing"]
        RW_PORT[RW :6432]
        RO_PORT[RO :6433]
        PRIMARY[(Primary PG)]
        REPLICA1[(Replica 1)]
        REPLICA2[(Replica 2)]
    end

    HC --> RW_CHECK
    HC --> RO_CHECK
    RW_CHECK --> FALL
    RW_CHECK --> RISE
    RO_CHECK --> FALL
    RO_CHECK --> RISE

    RW_PORT -->|primary| PRIMARY
    RO_PORT -->|round-robin| REPLICA1
    RO_PORT -->|round-robin| REPLICA2
    RO_PORT -.->|fallback| PRIMARY

    FALL -->|on-marked-down| SHUTDOWN[断开活跃连接]
```

## Raft 共识层

```mermaid
flowchart LR
    subgraph Node1["Node 1"]
        R1[Raft Node]
        SM1[KV StateMachine]
        DISK1[raft_log.wal hard_state.json state_machine.json membership.json]
    end
    subgraph Node2["Node 2"]
        R2[Raft Node]
        SM2[KV StateMachine]
        DISK2[持久化文件]
    end
    subgraph Node3["Node 3"]
        R3[Raft Node]
        SM3[KV StateMachine]
        DISK3[持久化文件]
    end

    R1 <-->|HTTPS :2380 TLS| R2
    R2 <-->|HTTPS :2380 TLS| R3
    R1 <-->|HTTPS :2380 TLS| R3

    R1 --> SM1
    R2 --> SM2
    R3 --> SM3
    SM1 --> DISK1
    SM2 --> DISK2
    SM3 --> DISK3
```

## DCS KV 存储结构

```
/service/{scope}/
├── leader          → "node1"              (TTL=15s, CAS 原子操作)
├── members/
│   ├── node1       → {conn_url, api_url, state, role, wal_position, timeline, rewind_check}  (TTL)
│   ├── node2       → {conn_url, api_url, state, role, wal_position, timeline, rewind_check}  (TTL)
│   └── node3       → {conn_url, api_url, state, role, wal_position, timeline, rewind_check}  (TTL)
├── timeline        → "3"                  (无 TTL, 单调不减, 持锁主写)
├── initialize      → "system_id"          (原子创建, 初始化竞争)
├── config          → {loop_wait, ttl, synchronous_mode, postgresql: {...}}
├── failover        → {leader, candidate}  (switchover 请求)
├── sync            → {leader, sync_standby, quorum}
├── failsafe        → {node1: api_url, ...}
└── history         → [{timestamp, event_type, old_leader, new_leader}]
```

## 同步复制数据流

```mermaid
flowchart LR
    CFG["DCS /config\nsynchronous_mode"]
    HA[Primary HA cycle]
    SM[SyncManager]
    PG["PostgreSQL\nsynchronous_standby_names"]
    SYNC["DCS /sync"]
    API["/sync /async health"]

    CFG --> HA
    HA --> SM
    SM --> PG
    SM --> SYNC
    SYNC --> API
```

Primary 在持有 Leader Lock 时读取动态配置：

- `synchronous_mode=false`（默认）：清空 `synchronous_standby_names`，写空 `/sync`
- `synchronous_mode=true`：按 `sync_priority` / `nosync` 选出同步备库，设置 `FIRST N (...)`，并写入 `/sync`

目标值未变化时跳过重复 `ALTER SYSTEM` + reload。`/sync`、`/async` 健康检查读取 `/sync.sync_standby` 判断节点角色。

当前范围：同步复制配置与状态发布已实现；Failover / Switchover 在 `synchronous_mode` 启用时强制候选节点属于 DCS `/sync.sync_standby`（S5）。Quorum 模式下的选举/切换约束尚未实现。

## 模块依赖关系

```
pg-ha (binary)
├── pg-ha-core      HA 引擎 + PG 生命周期 + 配置 + 类型
│   ├── ha/             决策循环 (mod / election / commands / …)
│   ├── postgresql.rs   pg_ctl start/stop/promote/rewind/reload
│   ├── bootstrap.rs    initdb / clone / custom bootstrap
│   ├── dynamic_config.rs  GlobalConfig + 变更检测 + patch
│   ├── failsafe.rs     DCS 故障时的安全模式
│   ├── slots.rs        复制槽管理
│   ├── sync.rs         同步复制管理
│   ├── cascading.rs    级联复制
│   ├── standby_cluster.rs  备库集群
│   ├── watchdog.rs     硬件看门狗
│   ├── callbacks.rs    事件回调
│   └── history.rs      集群历史
├── pg-ha-dcs       Raft 共识 + KV 状态机
│   ├── raft_dcs.rs     DcsAdapter 实现
│   ├── store.rs        Raft 存储 (持久化)
│   ├── state_machine.rs KV + TTL + CAS
│   └── raft_server.rs  HTTP RPC
├── pg-ha-api       REST API (axum)
│   ├── routes/         健康检查 + 管理端点 + /metrics
│   └── state.rs        共享状态 (AppState)
├── pg-ha-proxy     TCP 负载均衡
│   └── proxy.rs        RW/RO 路由 + 主动健康检查
└── pg-ha-ctl       CLI 工具 (clap + reqwest)
```


## TLS 加密通信

Raft RPC 层使用 TLS 加密所有节点间通信，防止集群共识数据在网络上被窃取或篡改。REST API 端口保持 HTTP，仅在内部网络暴露。

```mermaid
flowchart TB
    subgraph External["外部网络"]
        CLIENT[Client]
    end

    subgraph InternalNet["内部集群网络"]
        subgraph Node1["Node 1"]
            API1[REST API :8008 HTTP]
            RAFT1[Raft RPC :2380 HTTPS/TLS]
        end
        subgraph Node2["Node 2"]
            API2[REST API :8008 HTTP]
            RAFT2[Raft RPC :2380 HTTPS/TLS]
        end
        subgraph Node3["Node 3"]
            API3[REST API :8008 HTTP]
            RAFT3[Raft RPC :2380 HTTPS/TLS]
        end
    end

    RAFT1 <-->|mTLS 双向认证| RAFT2
    RAFT2 <-->|mTLS 双向认证| RAFT3
    RAFT1 <-->|mTLS 双向认证| RAFT3

    CLIENT -->|HTTP 内网| API1
    CLIENT -->|HTTP 内网| API2
    CLIENT -->|HTTP 内网| API3
```

**TLS 配置要点：**

| 组件 | 端口 | 协议 | 说明 |
|------|------|------|------|
| Raft RPC | 2380 | HTTPS (TLS 1.2+) | 节点间共识通信，支持 mTLS |
| REST API | 8008 | HTTP | 内网管理端点，依赖网络隔离 |
| PostgreSQL | 5432 | TCP | PG 原生协议，可独立配置 `ssl` |
| Proxy RW/RO | 6432/6433 | TCP | 透传 PG 连接 |

- Raft RPC 启用 TLS 后，所有 `AppendEntries`、`RequestVote`、`InstallSnapshot` RPC 均通过加密通道传输
- mTLS（双向 TLS）可选启用：每个节点同时验证对端证书，防止未授权节点加入集群
- REST API 保持 HTTP 是因为它仅在内部网络暴露，由 Basic Auth 保护管理端点

## 认证与安全

```mermaid
flowchart LR
    subgraph Endpoints["REST API 端点"]
        direction TB
        OPEN[开放端点 无需认证]
        PROTECTED[保护端点 需要认证]
    end

    subgraph OpenEndpoints["健康检查 - 负载均衡器使用"]
        GET_PRIMARY["GET /primary"]
        GET_REPLICA["GET /replica"]
        GET_SYNC["GET /sync"]
        GET_ASYNC["GET /async"]
        GET_HEALTH["GET /health"]
        GET_LIVENESS["GET /liveness"]
        GET_CLUSTER["GET /cluster"]
        GET_METRICS["GET /metrics"]
    end

    subgraph ProtectedEndpoints["管理操作 - Basic Auth"]
        POST_SWITCHOVER["POST /switchover"]
        POST_FAILOVER["POST /failover"]
        POST_RESTART["POST /restart"]
        POST_REINIT["POST /reinitialize"]
        CONFIG["GET/PUT/PATCH /config"]
    end

    OPEN --> OpenEndpoints
    PROTECTED --> ProtectedEndpoints
```

**认证机制：**

- **Basic Auth**：配置 `restapi.username` 和 `restapi.password` 后自动启用
- 认证未配置时，所有端点开放（适用于完全隔离的内网环境）
- 健康检查端点始终开放，供负载均衡器和 TCP Proxy 健康探测使用
- 管理端点（switchover、failover、config 变更等）受 Basic Auth 保护

```yaml
# 配置示例
restapi:
  listen: 0.0.0.0
  port: 8008
  username: admin      # 设置后启用 Basic Auth
  password: s3cr3t     # 管理端点需要此凭证
```

**请求示例：**

```bash
# 健康检查（无需认证）
curl http://localhost:8008/health
curl http://localhost:8008/primary
curl http://localhost:8008/replica

# 管理操作（需要 Basic Auth）
curl -u admin:s3cr3t -X POST http://localhost:8008/switchover \
  -H 'Content-Type: application/json' \
  -d '{"leader": "node1", "candidate": "node2"}'

# 未认证会返回 401
curl -X POST http://localhost:8008/failover
# → {"error": "Unauthorized"}
```

## 优雅关停流程

pg-ha 收到第一个 `SIGTERM` 或 `SIGINT` 后调用 `Ha::graceful_shutdown`（`crates/pg-ha-core/src/ha/shutdown.rs`，`src/main.rs` 只调用它）。规则：drain 期间续锁；CHECKPOINT + 停库受 leader 租约限制；停库之后才释放锁；PostgreSQL 还可能可写时绝不带着锁退出。

```mermaid
sequenceDiagram
    participant OS as 操作系统
    participant PGHA as pg-ha Agent
    participant PG as PostgreSQL
    participant DCS as Raft DCS
    participant OTHER as 其他节点

    OS->>PGHA: SIGTERM / SIGINT
    PGHA->>PGHA: HA 循环停止；REST API、Proxy 继续服务
    Note over PGHA: /primary 一律 503，不再启动 PG，管理命令被拒绝
    alt 持锁但租约已到期
        PGHA->>PG: standby.signal + 标记，pg_ctl stop -m immediate
        Note over PGHA: 不 drain、不 CHECKPOINT、不释放锁
    else 租约有效
        loop drain（drain_timeout_secs），每个 loop_wait
            PGHA->>DCS: update_leader（续锁）
            Note over PGHA: 续锁失败：immediate 围栏，不释放锁（pause 下只 WARN）
        end
        par 与最后一次续锁的租约赛跑
            PGHA->>PG: CHECKPOINT（持锁时）
            PGHA->>PG: standby.signal + 标记，pg_ctl stop -m fast
            PGHA->>PG: 失败时再试一次 pg_ctl stop -m immediate
        end
        alt PG 已停
            PGHA->>DCS: delete_leader（CAS，最多等 retry_timeout）
            PGHA->>OS: exit(0)
        else 租约先到期
            PGHA->>PG: standby.signal + 标记，pg_ctl stop -m immediate
            Note over PGHA: 不释放锁，锁按 TTL 过期
            PGHA->>OS: exit(0)
        else PG 可能可写且停不掉
            loop 每个 loop_wait：正常 HA 循环
                PGHA->>DCS: 续锁（持锁时）
                PGHA->>PG: 重试 pg_ctl stop（持锁 -m fast，不持锁 -m immediate）
                Note over PGHA: ERROR：给出手工停库命令；再来的信号只让下一轮立即运行
            end
            Note over PG: 运维手工停库
            PGHA->>DCS: delete_leader
            PGHA->>OS: exit(0)
        end
    end

    Note over OTHER: 锁释放或过期之后
    OTHER->>DCS: attempt_to_acquire_leader()
    OTHER->>OTHER: pg_ctl promote
```

**关键设计决策：**

1. **停库之后才释放锁**：锁一直在本节点手里，直到确认 PostgreSQL 已停。先删锁会让别的节点在本节点还可写时 promote。
2. **drain 期间续锁**：`drain_timeout_secs`（默认 30 秒）长于默认租约（25 秒），不续锁的话锁会在 drain 期间过期。续锁失败就按失去锁围栏（pause 下除外，W1）。
3. **收尾受租约限制**：最后一次续锁在 drain 结束时，它的租约（`ttl − safety_margin`）限住 CHECKPOINT 和停库，赛跑方式与 `run_cycle_bounded` 相同。截止时间先到就按租约到期围栏，不释放锁，所以锁过期、别的节点 promote 之前本节点已经不可写（L5 时间界）。
4. **fast，失败再 immediate**：`-m fast` 断开客户端、做关机检查点；它失败且 PG 还在跑时再试一次 `-m immediate`。两次都在租约赛跑之内。
5. **停不掉就不退出**：退出后锁会过期，没人再围栏这台可能可写的 PostgreSQL。pg-ha 继续跑正常 HA 循环（续锁、按 `stop_pending` 每轮重试停库），每轮 ERROR 给出手工停库命令，确认停库后再释放锁退出。再收到 SIGTERM / SIGINT 也不退出。观测在 recovery 的 PostgreSQL 也算可能可写，除非它不持锁且有 `standby.signal`：持锁的 recovery（pg-ha 会 promote 它）和没有 `standby.signal` 的 recovery（`recovery.signal`，结束时自己 promote）停不掉时同样不退出，HA 循环只重试停库，不 promote；关停中不持锁时每轮只再 `-m immediate` 停库，不参选、不跟随。只有不持锁、有 `standby.signal` 的备库停不掉时记 ERROR 后退出。REST API 由单独的任务服务，这段时间一直可用，`/primary` 保持 503。
6. **SIGKILL 不在覆盖范围内**：systemd `TimeoutStopSec` 或 `docker stop -t` 到期后 pg-ha 被杀，上述规则无法执行，所以超时必须大于 drain + CHECKPOINT + 停库的时间（见运维指南）。停主库前先 switchover。

**时间线（默认配置；docker 开发集群 `ttl` 15、`loop_wait` 5）：**

| 阶段 | 耗时 | 说明 |
|------|------|------|
| 信号 | < 1ms | HA 循环停止，`/primary` 变 503 |
| drain | `drain_timeout_secs`（30s） | 只等待，每个 `loop_wait` 续锁 |
| CHECKPOINT + 停库 | 通常数秒，上限 `ttl − safety_margin`（25s，docker 10s） | 超过上限即围栏 |
| 释放锁 | ≤ `retry_timeout` | 只在停库之后 |
| 故障转移 | ≤ `loop_wait` + promote | 其他节点看到 unlocked 后选举、promote |
| **写不可用合计** | **约 30-45s** | 从信号到新 Primary 就绪；先 switchover 可避免 |

## 日志系统

pg-ha 使用 `tracing` + `tracing-subscriber` 提供 text/json 双模式日志输出。

**日志模式切换：**

| 环境变量 | 值 | 效果 |
|---------|---|------|
| `PG_HA_LOG_FORMAT` | `text`（默认） | 人类可读的彩色日志 |
| `PG_HA_LOG_FORMAT` | `json` | 结构化 JSON 日志（生产推荐） |
| `RUST_LOG` | tracing 过滤表达式 | 控制日志级别和模块过滤 |

**Text 模式示例（默认）：**

```
2024-01-15T10:30:00.123Z  INFO pg_ha: Starting pg-ha agent name="node1" scope="prod-cluster"
2024-01-15T10:30:02.456Z  INFO pg_ha_dcs: Raft cluster bootstrapped with 3 members
2024-01-15T10:30:05.789Z  INFO pg_ha_core::ha: run_cycle completed role=Primary leader=true
```

**JSON 模式示例（`PG_HA_LOG_FORMAT=json`）：**

```json
{"timestamp":"2024-01-15T10:30:00.123Z","level":"INFO","target":"pg_ha","message":"Starting pg-ha agent","name":"node1","scope":"prod-cluster"}
{"timestamp":"2024-01-15T10:30:02.456Z","level":"INFO","target":"pg_ha_dcs","message":"Raft cluster bootstrapped with 3 members"}
{"timestamp":"2024-01-15T10:30:05.789Z","level":"INFO","target":"pg_ha_core::ha","message":"run_cycle completed","role":"Primary","leader":true}
```

**RUST_LOG 环境变量控制：**

```bash
# 默认级别（内置）
RUST_LOG="pg_ha=info,openraft::replication=off"

# 调试 HA 决策循环
RUST_LOG="pg_ha_core::ha=debug,pg_ha=info"

# 调试 Raft 共识
RUST_LOG="pg_ha_dcs=debug,openraft=debug,pg_ha=info"

# 调试网络通信
RUST_LOG="pg_ha_dcs::network=trace,pg_ha=info"

# 全部 trace（大量输出，仅调试用）
RUST_LOG="trace"
```

**日志格式优先级：**

1. 环境变量 `PG_HA_LOG_FORMAT` 覆盖默认值
2. `RUST_LOG` 环境变量覆盖内置的过滤表达式
3. 内置默认：`pg_ha=info` + `openraft::replication=off`（抑制高频复制日志）
