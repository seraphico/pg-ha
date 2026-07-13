# pg-ha 部署指南

## 系统要求

| 组件 | 最低版本 | 说明 |
|------|---------|------|
| PostgreSQL | 14+ | 需要 `pg_ctl`、`pg_basebackup`、`pg_rewind`（Docker 支持 14-18） |
| Rust | 1.77+ (nightly) | 编译需要 `let-else` 等 nightly 特性 |
| Docker | 24.0+ | 容器化部署 |
| Docker Compose | v2.20+ | 多节点编排 |
| 操作系统 | Linux (amd64/arm64) / macOS | 生产环境推荐 Linux |

**硬件建议（每节点）：**

| 环境 | CPU | 内存 | 磁盘 |
|------|-----|------|------|
| 开发/测试 | 2 核 | 2 GB | 10 GB SSD |
| 生产 | 4+ 核 | 8+ GB | 根据数据量，SSD/NVMe |

**网络要求：**

- 集群节点间网络延迟 < 5ms（Raft 共识性能依赖低延迟）
- 节点间以下端口互通：2380 (Raft)、8008 (API)、5432 (PG)

---

## 快速开始

使用 `make up` 一键启动三节点开发集群：

```bash
# 克隆项目
git clone <repo-url> pg-ha && cd pg-ha

# 启动三节点集群（自动编译 + 构建镜像 + 启动，默认 PG 16）
make up

# 使用其他 PostgreSQL 版本（支持 14-18）
make up PG_VERSION=17
make up PG_VERSION=18

# 验证集群状态
curl -s http://localhost:8008/cluster | jq .

# 连接 PostgreSQL（通过 Proxy RW 端口）
psql -h localhost -p 6432 -U postgres -d postgres
# 密码: secret

# 查看集群日志
make logs

# 停止并清除数据
make down
```

**启动后的端口映射（docker-compose）：**

| 节点 | PostgreSQL | REST API | Proxy RW | Proxy RO | Raft RPC |
|------|-----------|----------|----------|----------|----------|
| node1 | 5432 | 8008 | 6432 | 6433 | 2380 (内部) |
| node2 | 5433 | 8009 | 6434 | 6435 | 2380 (内部) |
| node3 | 5434 | 8010 | 6436 | 6437 | 2380 (内部) |

---

## 生产部署（裸机 3 节点）

### 前置条件

每台服务器上安装：

```bash
# 安装 PostgreSQL 16
sudo apt-get install postgresql-16 postgresql-client-16

# 确认 bin 路径
ls /usr/lib/postgresql/16/bin/pg_ctl

# 创建 pg-ha 用户和目录
sudo useradd -r -m -s /bin/bash pgha
sudo mkdir -p /var/lib/pg-ha/raft /var/lib/postgresql/16/data /etc/pg-ha
sudo chown pgha:pgha /var/lib/pg-ha/raft /var/lib/postgresql/16/data
```

### 部署二进制

```bash
# 从 Release 页面下载或自行编译
# 编译方式：
cargo build --release --target x86_64-unknown-linux-gnu -p pg-ha -p pg-ha-ctl

# 复制二进制
sudo cp target/x86_64-unknown-linux-gnu/release/pg-ha /usr/local/bin/
sudo cp target/x86_64-unknown-linux-gnu/release/pg-ha-ctl /usr/local/bin/
sudo chmod +x /usr/local/bin/pg-ha /usr/local/bin/pg-ha-ctl
```

### 配置文件

在每台节点创建 `/etc/pg-ha/pg-ha.yml`（根据节点修改 `name` 和 `raft.self_addr`）：

```yaml
name: node1                    # 每台不同: node1, node2, node3
scope: prod-cluster
namespace: service
loop_wait: 10
ttl: 30
retry_timeout: 10

postgresql:
  data_dir: /var/lib/postgresql/16/data
  bin_dir: /usr/lib/postgresql/16/bin
  listen: 0.0.0.0
  port: 5432
  superuser:
    username: postgres
    password: "CHANGE_ME_superuser_password"
    dbname: postgres
  replication:
    username: replicator
    password: "CHANGE_ME_replication_password"
    dbname: postgres
  parameters:
    max_connections: "200"
    wal_level: replica
    max_wal_senders: "10"
    max_replication_slots: "10"
    hot_standby: "on"
    shared_buffers: "2GB"
    effective_cache_size: "6GB"
    wal_keep_size: "1GB"

restapi:
  listen: 0.0.0.0
  port: 8008
  username: admin
  password: "CHANGE_ME_api_password"

raft:
  self_addr: "10.0.1.1:2380"  # 每台不同: .1, .2, .3
  partner_addrs:
    - "10.0.1.2:2380"         # 其他两台
    - "10.0.1.3:2380"
  data_dir: /var/lib/pg-ha/raft

proxy:
  rw_listen: 0.0.0.0
  rw_port: 6432
  ro_listen: 0.0.0.0
  ro_port: 6433

watchdog:
  mode: "off"

bootstrap:
  initdb:
    - data-checksums
    - encoding: UTF8
  dcs:
    loop_wait: 10
    ttl: 30
    maximum_lag_on_failover: 1048576
  post_bootstrap_sql:
    - "CREATE USER replicator WITH REPLICATION PASSWORD 'CHANGE_ME_replication_password'"
```

### systemd 服务文件

创建 `/etc/systemd/system/pg-ha.service`：

```ini
[Unit]
Description=pg-ha PostgreSQL High Availability Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pgha
Group=pgha
ExecStart=/usr/local/bin/pg-ha /etc/pg-ha/pg-ha.yml
Restart=on-failure
RestartSec=5s
TimeoutStopSec=30s
KillMode=mixed
KillSignal=SIGTERM

# 环境变量
Environment=RUST_LOG=pg_ha=info
Environment=PG_HA_LOG_FORMAT=json

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096

# 安全加固
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
# 启用并启动服务
sudo systemctl daemon-reload
sudo systemctl enable pg-ha
sudo systemctl start pg-ha

# 检查状态
sudo systemctl status pg-ha
journalctl -u pg-ha -f
```

---

## 配置参考

### 完整 YAML Schema

```yaml
# ─── 必填字段 ───
name: <string>                     # 节点唯一名称 (必填)
scope: <string>                    # 集群名称/范围 (必填)

# ─── 可选顶层字段 ───
namespace: <string>                # DCS key 路径前缀 (默认: "service")
loop_wait: <u64>                   # HA 循环间隔秒数 (默认: 10)
ttl: <u64>                         # Leader Lock TTL 秒数 (默认: 30, 必须 > loop_wait)
retry_timeout: <u64>               # DCS/PG 操作重试超时秒数 (默认: 10)

# ─── PostgreSQL 配置 (必填) ───
postgresql:
  data_dir: <path>                 # PGDATA 路径 (必填)
  bin_dir: <path>                  # PG 工具目录 (必填, 含 pg_ctl 等)
  listen: <string>                 # PG 监听地址 (默认: "0.0.0.0")
  port: <u16>                      # PG 端口 (默认: 5432)
  superuser:                       # 超级用户连接参数 (必填)
    username: <string>             # 用户名 (必填)
    password: <string>             # 密码 (可选)
    dbname: <string>               # 数据库名 (默认: "postgres")
  replication:                     # 复制用户连接参数 (必填)
    username: <string>             # 用户名 (必填)
    password: <string>             # 密码 (可选)
    dbname: <string>               # 数据库名 (默认: "postgres")
  parameters:                      # PG 参数 (可选, map<string, string>)
    <param_name>: <value>

# ─── REST API 配置 (必填) ───
restapi:
  listen: <string>                 # API 监听地址 (默认: "0.0.0.0")
  port: <u16>                      # API 端口 (默认: 8008)
  username: <string>               # Basic Auth 用户名 (可选, 设置后启用认证)
  password: <string>               # Basic Auth 密码 (可选)

# ─── Raft DCS 配置 (必填) ───
raft:
  self_addr: <string>              # 本节点 Raft RPC 地址 host:port (必填)
  partner_addrs:                   # 其他节点 Raft 地址列表 (必填, 至少1个)
    - <string>
  data_dir: <path>                 # Raft 持久化目录 (可选)
  node_id: <u64>                   # 显式节点 ID (可选, 默认从排序位置推导)

# ─── TCP Proxy 配置 (必填) ───
proxy:
  rw_listen: <string>              # RW Proxy 监听地址 (默认: "0.0.0.0")
  rw_port: <u16>                   # RW Proxy 端口 (默认: 6432)
  ro_listen: <string>              # RO Proxy 监听地址 (默认: "0.0.0.0")
  ro_port: <u16>                   # RO Proxy 端口 (默认: 6433)

# ─── Watchdog 配置 (可选) ───
watchdog:
  mode: <string>                   # "off" | "automatic" | "required" (默认: "off")
  device: <string>                 # 看门狗设备路径 (默认: "/dev/watchdog")
  safety_margin: <u64>             # 安全边际秒数 (默认: 5)

# ─── 节点标签 (可选) ───
tags:
  nofailover: <bool>               # 禁止本节点成为 Primary (默认: false)
  noloadbalance: <bool>            # 从 RO Proxy 排除本节点 (默认: false)
  noclone: <bool>                  # 禁止作为 clone 源 (默认: false)
  nosync: <bool>                   # 不参与同步复制 (默认: false)
  nostream: <bool>                 # 使用 WAL 文件恢复代替 streaming (默认: false)
  clonefrom: <bool>                # 优先作为 clone 源 (默认: false)
  replicatefrom: <string>          # 从指定节点复制 (级联复制, 可选)
  failover_priority: <u32>         # 故障转移优先级 (默认: 1, 0=等同 nofailover)
  sync_priority: <u32>             # 同步复制优先级 (默认: 0)

# ─── Bootstrap 配置 (可选, 仅首次初始化时使用) ───
bootstrap:
  initdb:                          # initdb 选项列表
    - <string>                     # 标志选项 (如 "data-checksums")
    - <key>: <value>               # 键值选项 (如 encoding: UTF8)
  dcs:                             # 初始动态配置 (写入 DCS /config)
    loop_wait: <u64>
    ttl: <u64>
    maximum_lag_on_failover: <u64>
  post_init: [<string>]            # 初始化后执行的 SQL 脚本路径
  custom_command: <string>         # 自定义 bootstrap 命令 (替代 initdb)
  post_bootstrap_sql: [<string>]   # bootstrap 后执行的 SQL 语句
```

### 环境变量覆盖

所有配置项均可通过 `PG_HA_` 前缀的环境变量覆盖，嵌套字段用双下划线 `__` 分隔：

| 环境变量 | 覆盖字段 |
|---------|---------|
| `PG_HA_NAME` | `name` |
| `PG_HA_SCOPE` | `scope` |
| `PG_HA_LOOP_WAIT` | `loop_wait` |
| `PG_HA_TTL` | `ttl` |
| `PG_HA_RETRY_TIMEOUT` | `retry_timeout` |
| `PG_HA_POSTGRESQL__PORT` | `postgresql.port` |
| `PG_HA_POSTGRESQL__LISTEN` | `postgresql.listen` |
| `PG_HA_RESTAPI__PORT` | `restapi.port` |
| `PG_HA_RAFT__SELF_ADDR` | `raft.self_addr` |
| `PG_HA_LOG_FORMAT` | 日志格式 (`text` / `json`) |
| `RUST_LOG` | 日志级别过滤 |

---

## TLS 配置

### 证书生成（自签名 CA 示例）

```bash
# 1. 生成 CA 私钥和证书
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=pg-ha-ca/O=pg-ha"

# 2. 生成服务端证书（每个节点一份）
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=node1/O=pg-ha"

# 创建 SAN 扩展文件（包含所有节点地址）
cat > san.ext << EOF
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = node1
DNS.2 = node2
DNS.3 = node3
IP.1 = 10.0.1.1
IP.2 = 10.0.1.2
IP.3 = 10.0.1.3
EOF

openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -extfile san.ext -extensions v3_req

# 3. 生成客户端证书（用于 mTLS）
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/CN=pg-ha-client/O=pg-ha"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt
```

### 证书文件分发

```
/etc/pg-ha/tls/
├── ca.crt         # CA 证书（所有节点相同）
├── server.crt     # 服务端证书（可所有节点共用或每节点独立）
├── server.key     # 服务端私钥
├── client.crt     # 客户端证书（mTLS 模式）
└── client.key     # 客户端私钥（mTLS 模式）
```

设置权限：

```bash
sudo chown pgha:pgha /etc/pg-ha/tls/*
sudo chmod 600 /etc/pg-ha/tls/*.key
sudo chmod 644 /etc/pg-ha/tls/*.crt
```

---

## Docker 部署

### PostgreSQL 版本选择

Docker 镜像支持 PostgreSQL 14-18，通过 `PG_VERSION` 构建参数控制：

```bash
# 默认使用 PG 16
make up

# 使用 PG 18
make up PG_VERSION=18

# 也可直接用 docker compose
PG_VERSION=17 docker compose build
docker compose up -d
```

entrypoint 会自动检测容器内的 PostgreSQL 版本，正确设置 `bin_dir` 和 `data_dir`，无需手动修改配置。

### 生产 docker-compose 配置

```yaml
# docker-compose.prod.yml
services:
  node1:
    image: pg-ha:latest
    container_name: pg-ha-node1
    hostname: node1
    restart: unless-stopped
    environment:
      PG_HA_NAME: node1
      PG_HA_SCOPE: prod-cluster
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
      PG_HA_RAFT_SELF: "node1:2380"
      PG_HA_RAFT_PARTNERS: "node2:2380,node3:2380"
      PG_HA_LOG_FORMAT: json
      RUST_LOG: "pg_ha=info"
    ports:
      - "5432:5432"
      - "8008:8008"
      - "6432:6432"
      - "6433:6433"
    volumes:
      - node1-raft:/var/lib/pg-ha/raft
      - node1-pgdata:/var/lib/postgresql/data
    networks:
      - pg-ha-net
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: "2.0"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8008/liveness"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 30s

  node2:
    image: pg-ha:latest
    container_name: pg-ha-node2
    hostname: node2
    restart: unless-stopped
    environment:
      PG_HA_NAME: node2
      PG_HA_SCOPE: prod-cluster
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
      PG_HA_RAFT_SELF: "node2:2380"
      PG_HA_RAFT_PARTNERS: "node1:2380,node3:2380"
      PG_HA_LOG_FORMAT: json
      RUST_LOG: "pg_ha=info"
    ports:
      - "5433:5432"
      - "8009:8008"
      - "6434:6432"
      - "6435:6433"
    volumes:
      - node2-raft:/var/lib/pg-ha/raft
      - node2-pgdata:/var/lib/postgresql/data
    networks:
      - pg-ha-net
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: "2.0"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8008/liveness"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 30s

  node3:
    image: pg-ha:latest
    container_name: pg-ha-node3
    hostname: node3
    restart: unless-stopped
    environment:
      PG_HA_NAME: node3
      PG_HA_SCOPE: prod-cluster
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
      PG_HA_RAFT_SELF: "node3:2380"
      PG_HA_RAFT_PARTNERS: "node1:2380,node2:2380"
      PG_HA_LOG_FORMAT: json
      RUST_LOG: "pg_ha=info"
    ports:
      - "5434:5432"
      - "8010:8008"
      - "6436:6432"
      - "6437:6433"
    volumes:
      - node3-raft:/var/lib/pg-ha/raft
      - node3-pgdata:/var/lib/postgresql/data
    networks:
      - pg-ha-net
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: "2.0"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8008/liveness"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 30s

networks:
  pg-ha-net:
    driver: bridge

volumes:
  node1-raft:
  node2-raft:
  node3-raft:
  node1-pgdata:
  node2-pgdata:
  node3-pgdata:
```

启动：

```bash
# 设置密码环境变量
export PG_PASSWORD="your-secure-password"

# 构建镜像
make build
docker compose -f docker-compose.prod.yml build

# 启动集群
docker compose -f docker-compose.prod.yml up -d

# 检查状态
docker compose -f docker-compose.prod.yml ps
curl -s http://localhost:8008/cluster | jq .
```

---

## 端口说明

| 端口 | 协议 | 方向 | 用途 |
|------|------|------|------|
| 5432 | TCP | 入站 | PostgreSQL 客户端连接（直连） |
| 8008 | HTTP | 入站 | REST API（健康检查 + 管理） |
| 2380 | HTTP(S) | 节点间 | Raft RPC（共识通信） |
| 6432 | TCP | 入站 | Proxy RW（路由到 Primary） |
| 6433 | TCP | 入站 | Proxy RO（负载均衡到 Replica） |

**防火墙规则建议：**

```bash
# 客户端访问（从应用层到 pg-ha）
ufw allow 6432/tcp   # Proxy RW
ufw allow 6433/tcp   # Proxy RO
ufw allow 8008/tcp   # API（仅管理网络）

# 集群内通信（节点间）
ufw allow from 10.0.1.0/24 to any port 2380  # Raft RPC
ufw allow from 10.0.1.0/24 to any port 5432  # PG 复制
ufw allow from 10.0.1.0/24 to any port 8008  # Proxy 健康检查
```
