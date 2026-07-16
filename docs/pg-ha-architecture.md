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
    PGRUN -->|否| PGSTART[尝试启动 PG]
    PGRUN -->|是| LOCKED{集群有 Leader?}
    LOCKED -->|unlocked| ELECTION[选举流程]
    LOCKED -->|locked| OWNER{我持有 lock?}
    OWNER -->|是| RENEW[续约 enforce_primary]
    OWNER -->|否| FOLLOW[follow_upstream]
```

## 选举流程 (process_unhealthy_cluster)

```mermaid
flowchart TD
    START[集群 unlocked] --> PAUSE{暂停模式?}
    PAUSE -->|是| NOOP[不操作]
    PAUSE -->|否| STALE{是旧 Primary?}
    STALE -->|是| REJOIN[rejoin_as_replica]
    STALE -->|否| FAILOVER{有 failover key 指定 candidate?}
    FAILOVER -->|是 我是candidate| ACQUIRE[尝试获取 leader lock]
    FAILOVER -->|是 我不是| DEFER[让步给 candidate]
    FAILOVER -->|否| HEALTH{is_healthiest_node?}
    HEALTH -->|是| ACQUIRE
    HEALTH -->|否| WAIT[等待选举结果]
    ACQUIRE -->|成功| PROMOTE[pg_ctl promote]
    ACQUIRE -->|失败| WAIT
```

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
    R1->>R1: pg_ctl promote
    Note over R1: 成为新 Primary

    R2->>DCS: get_cluster() → leader=node2
    R2->>R2: follow_upstream(node2)
    R2->>R2: 检测 primary_conninfo 变化
    R2->>R2: reconfigure → 重启 PG 指向 node2

    Note over P: 旧 Primary 重启
    P->>DCS: get_cluster()
    P->>P: 检测: 无 standby.signal + 不是最健康
    P->>P: rejoin_as_replica(node2)
    P->>P: pg_ctl stop
    P->>P: pg_rewind (从 node2)
    P->>P: 写入 standby.signal + primary_conninfo
    P->>P: pg_ctl start (standby 模式)
    Note over P: 自动 streaming 追赶数据
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
        DISK1[hard_state.json log_entries.json state_machine.json]
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
│   ├── node1       → {conn_url, api_url, state, role}  (TTL)
│   ├── node2       → {conn_url, api_url, state, role}  (TTL)
│   └── node3       → {conn_url, api_url, state, role}  (TTL)
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

当前范围：同步复制配置与状态发布已实现；故障切换时强制优先同步备库 / quorum 约束尚未实现。

## 模块依赖关系

```
pg-ha (binary)
├── pg-ha-core      HA 引擎 + PG 生命周期 + 配置 + 类型
│   ├── ha.rs           决策循环 (run_cycle)
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
│   ├── routes.rs       健康检查 + 管理端点 + /metrics
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

pg-ha 收到 `SIGTERM` 或 `SIGINT` 信号后，执行分阶段优雅关停，确保数据安全和快速故障转移。

```mermaid
sequenceDiagram
    participant OS as 操作系统
    participant PGHA as pg-ha Agent
    participant PG as PostgreSQL
    participant DCS as Raft DCS
    participant OTHER as 其他节点

    OS->>PGHA: SIGTERM / SIGINT

    Note over PGHA: Phase 1: 信号接收
    PGHA->>PGHA: 停止 HA 循环 (tokio::select! 退出)

    Note over PGHA: Phase 2: Leader 特殊处理
    alt 当前节点是 Leader
        PGHA->>PG: CHECKPOINT (刷盘)
        PG-->>PGHA: OK
        PGHA->>DCS: delete_leader(lock) 释放 Leader Lock
        DCS-->>PGHA: OK
        Note over DCS: Leader Lock 立即释放
    end

    Note over PGHA: Phase 3: 停止 PostgreSQL
    PGHA->>PG: pg_ctl stop -m fast
    PG->>PG: 断开客户端连接
    PG->>PG: 完成 WAL 刷盘
    PG-->>PGHA: 退出

    Note over PGHA: Phase 4: 进程退出
    PGHA->>OS: exit(0)

    Note over OTHER: 故障转移触发
    OTHER->>DCS: get_cluster() → unlocked
    OTHER->>OTHER: is_healthiest_node()?
    OTHER->>DCS: attempt_to_acquire_leader()
    OTHER->>OTHER: pg_ctl promote
    Note over OTHER: 新 Primary 就绪
```

**关键设计决策：**

1. **Leader 主动释放锁**：不等待 TTL 过期，立即释放 Leader Lock，使其他节点能在秒级内完成故障转移
2. **CHECKPOINT 在释放锁之前**：确保所有已提交事务的 WAL 已刷盘，避免数据丢失
3. **fast 模式停止 PG**：立即断开客户端连接，但允许正在进行的 WAL 写入完成
4. **信号处理的 tokio::select!**：与 HA 循环、API 服务器、Proxy 并行监听，任一退出触发关停

**时间线：**

| 阶段 | 耗时 | 说明 |
|------|------|------|
| Phase 1 | < 1ms | 信号接收，select! 退出 |
| Phase 2 | 1-5s | CHECKPOINT + 释放锁 |
| Phase 3 | 1-3s | pg_ctl stop fast |
| Phase 4 | 即时 | exit(0) |
| 故障转移 | 1-2s | 其他节点检测到 unlocked 并 promote |
| **总计** | **3-10s** | 从信号到新 Primary 就绪 |

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
