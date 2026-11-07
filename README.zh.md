# pg-ha

[English](README.md)

带内置 Raft 共识和负载均衡的 PostgreSQL 高可用。

## 概述

pg-ha 是用 Rust 写的 PostgreSQL 高可用方案，对标 [Patroni](https://github.com/patroni/patroni) 那类能力：自动故障切换和同步复制，同时去掉 etcd、ZooKeeper、Consul 和 HAProxy。不实现 Patroni 的每一个开关。还没做的缺口列在 TODO 里。

- **没有 etcd/ZooKeeper/Consul** — 用 [openraft](https://github.com/databendlabs/openraft) 做嵌入式 Raft
- **没有 HAProxy** — 进程内 TCP 代理，带主动健康检查
- **单二进制** — 每个节点一个进程，不依赖外部服务
- **TLS** — Raft RPC 和 REST 都可以上 TLS。`raft.tls` / `restapi.tls` 的 `cert` + `key` 是服务端认证的 HTTPS；再加上 `ca_cert` 和 `require_client_cert: true` 才要求客户端证书。省略 `tls` 段则该平面保持明文（默认）。REST 的 Basic Auth 只有在 REST TLS 打开后才在传输中受保护
- **优雅关停** — 排空期间续锁，CHECKPOINT 和停库受 leader 租约限制，PostgreSQL 停掉之后才释放锁

## 文档

| 文档 | 中文 | English |
|------|------|---------|
| 架构 | [架构](docs/pg-ha-architecture.md) | [Architecture](docs/en/pg-ha-architecture.md) |
| 部署 | [部署指南](docs/deployment-guide.md) | [Deployment Guide](docs/en/deployment-guide.md) |
| 运维 | [运维指南](docs/operations-guide.md) | [Operations Guide](docs/en/operations-guide.md) |
| 不变量 | [不变量](docs/invariants.md) | [Invariants](docs/en/invariants.md) |
| 待办 | [待办](docs/TODO.md) | [TODO](docs/en/TODO.md) |

## 架构

每个节点跑一个 `pg-ha` 进程，里面有：

1. **HA Engine** — 周期性决策，负责 failover 和 promote
2. **Raft DCS** — 嵌入式共识，保存选主和集群状态
3. **REST API** — 健康检查（HAProxy 能用的那种）和管理接口
4. **TCP Proxy** — 把读写流量分到对应的 PostgreSQL

## 快速开始

```bash
# 克隆并启动 3 节点集群
git clone https://github.com/seraphico/pg-ha.git && cd pg-ha
make up

# 看集群
curl -s http://localhost:8008/cluster | jq .

# 经代理连主库（读写端口）
psql -h localhost -p 6432 -U postgres

# 停掉集群
make down
```

## Workspace

| Crate | 作用 |
|-------|------|
| `pg-ha` | 主程序，把各子系统装在一起 |
| `pg-ha-core` | HA 逻辑、PG 生命周期、集群类型、配置 |
| `pg-ha-dcs` | Raft 状态机、网络、DCS trait |
| `pg-ha-proxy` | 带健康检查的 PostgreSQL TCP 代理 |
| `pg-ha-api` | axum REST：健康检查和管理 |
| `pg-ha-ctl` | 集群管理 CLI |

## 许可证

Apache-2.0
