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

    R1 <-->|HTTP :2380| R2
    R2 <-->|HTTP :2380| R3
    R1 <-->|HTTP :2380| R3

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
├── config          → {loop_wait, ttl, postgresql: {parameters: {...}}}
├── failover        → {leader, candidate}  (switchover 请求)
├── sync            → {leader, sync_standby}
├── failsafe        → {node1: api_url, ...}
└── history         → [{timestamp, event_type, old_leader, new_leader}]
```

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
