# pg-ha 部署指南

## 系统要求

| 组件 | 最低版本 | 说明 |
|------|---------|------|
| PostgreSQL | 14+ | 需要 `pg_ctl`、`pg_basebackup`、`pg_rewind`。Docker 使用官方 `postgres` 镜像标签；`make up` 默认 16 |
| Rust | 1.88+ 稳定版 | `edition = "2024"`，并使用已稳定的 if-let chains。不需要 nightly |
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
- 节点间以下端口互通：2380（Raft RPC，配置了 `raft.tls` 时为 HTTPS）、8008（REST API，当前为 HTTP）、5432（PostgreSQL 复制）

---

## 快速开始

使用 `make up` 一键启动三节点开发集群：

```bash
# 克隆项目
git clone <repo-url> pg-ha && cd pg-ha

# 启动三节点集群（编译 Linux 二进制 + 构建镜像 + 启动，默认 PG 16）
make up

# 使用其他官方 postgres 镜像标签
make up PG_VERSION=17
make up PG_VERSION=18

# 验证集群拓扑（成员名、角色、复制上游）
curl -s http://localhost:8008/cluster | jq .

# 查看本节点详情（角色、状态、timeline、lag）
curl -s http://localhost:8008/patroni | jq .

# 连接 PostgreSQL（通过 Proxy RW 端口）
psql -h localhost -p 6432 -U postgres -d postgres
# 密码: secret
# 管理 API 的 Basic Auth 同样是 admin / secret（仅 /switchover、/config 等）

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

从 GitHub Release（`v*` tag 触发 `.github/workflows/release.yml`）下载对应平台的 `pg-ha-*.tar.gz`，或在本机编译。

Linux 上可以直接：

```bash
cargo build --release -p pg-ha -p pg-ha-ctl
sudo cp target/release/pg-ha target/release/pg-ha-ctl /usr/local/bin/
sudo chmod +x /usr/local/bin/pg-ha /usr/local/bin/pg-ha-ctl
```

要打出放进 Docker 镜像的 Linux 二进制，用仓库里的 `make build`。它按本机架构选择 `x86_64-unknown-linux-gnu` 或 `aarch64-unknown-linux-gnu`：本机已有 `cargo-zigbuild` 或 `cross` 时用它们，Linux 本机用 `cargo`，否则用 `docker-compose.build.yml` 里的 builder。产物在 `target/<target>/release/`。

```bash
make build
# 二进制在 target/$RUST_TARGET/release/，RUST_TARGET 可用 `make print-env` 查看
sudo cp target/x86_64-unknown-linux-gnu/release/pg-ha /usr/local/bin/   # 按 print-env 改目录
sudo cp target/x86_64-unknown-linux-gnu/release/pg-ha-ctl /usr/local/bin/
```

校验配置或生成样例（不启动进程）：

```bash
pg-ha --validate-config /etc/pg-ha/pg-ha.yml
pg-ha --generate-sample-config
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
  # REST 目前只有 HTTP。管理端口不要暴露到公网；需要加密时在前面放 TLS 终结。

raft:
  self_addr: "10.0.1.1:2380"  # 每台不同: .1, .2, .3
  partner_addrs:
    - "10.0.1.2:2380"         # 其他两台
    - "10.0.1.3:2380"
  data_dir: /var/lib/pg-ha/raft
  tls:                              # 省略则 Raft RPC 为明文 HTTP
    cert: /etc/pg-ha/tls/server.crt
    key: /etc/pg-ha/tls/server.key
    ca_cert: /etc/pg-ha/tls/ca.crt
    client_cert: /etc/pg-ha/tls/client.crt
    client_key: /etc/pg-ha/tls/client.key

log_format: json                    # text（默认）或 json

shutdown:
  drain_timeout_secs: 30            # SIGTERM 后、CHECKPOINT 之前的等待秒数

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
# drain_timeout_secs（默认 30）+ CHECKPOINT + pg_ctl stop，30 秒不够
TimeoutStopSec=120s
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
  username: <string>               # Basic Auth 用户名 (可选, 设置后管理端点要求认证)
  password: <string>               # Basic Auth 密码 (可选)
  # 没有 tls 字段。健康检查始终明文；管理端点在配置了 username 时要求 Basic Auth

# ─── Raft DCS 配置 (必填) ───
raft:
  self_addr: <string>              # 本节点 Raft RPC 地址 host:port (必填)
  partner_addrs:                   # 其他节点 Raft 地址列表 (必填, 至少1个)
    - <string>
  data_dir: <path>                 # 持久化目录 (可选)。设置后写入 raft_log.wal、
                                   # hard_state.json、state_machine.json、membership.json
  node_id: <u64>                   # 显式节点 ID (可选, 默认是地址排序后的位置 + 1)
  tls:                             # 可选。存在则 Raft RPC 为 HTTPS
    cert: <path>                   # 服务端证书 PEM (必填, 若启用 tls)
    key: <path>                    # 服务端私钥 PEM
    ca_cert: <path>                # 校验对端证书的 CA (可选)
    client_cert: <path>            # 出站客户端证书 (可选)
    client_key: <path>             # 出站客户端私钥 (可选)
    require_client_cert: <bool>    # 配置里有该字段，启动代码尚未强制校验客户端证书

# ─── TCP Proxy 配置 (必填) ───
proxy:
  rw_listen: <string>              # RW Proxy 监听地址 (默认: "0.0.0.0")
  rw_port: <u16>                   # RW Proxy 端口 (默认: 6432)
  ro_listen: <string>              # RO Proxy 监听地址 (默认: "0.0.0.0")
  ro_port: <u16>                   # RO Proxy 端口 (默认: 6433)

# ─── 日志与停机 (可选) ───
log_format: <string>               # "text"（默认）或 "json"。也可被 PG_HA_LOG_FORMAT 覆盖
shutdown:
  drain_timeout_secs: <u64>        # SIGTERM 后等待秒数，然后才 CHECKPOINT (默认: 30)

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
    synchronous_mode: <bool>       # 同步复制开关 (默认: false)
    synchronous_mode_strict: <bool># 无同步备库时是否阻塞写 (默认: false)
    synchronous_node_count: <u32>  # 同步备库数量 (默认: 1)
  post_init: [<string>]            # 初始化后执行的 SQL 脚本路径
  custom_command: <string>         # 自定义 bootstrap 命令 (替代 initdb)
  post_bootstrap_sql: [<string>]   # bootstrap 后执行的 SQL 语句
```

同步复制相关标签：`tags.nosync`（排除同步备库）、`tags.sync_priority`（同步优先级）。运行时也可通过 `PATCH /config` 开关同步复制，详见 [运维指南](operations-guide.md#同步复制)。

### 环境变量

二进制只覆盖下面这些字段（`Config::apply_env_overrides`，再加 `main` 里单独读取的日志格式）。没有列出的 `PG_HA_*` **不会**改 YAML。嵌套字段用双下划线 `__`。

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
| `PG_HA_LOG_FORMAT` | 日志格式，优先于 YAML 的 `log_format`（`json` 为 JSON，其他值为文本） |
| `RUST_LOG` | `tracing` 过滤指令。未设置时默认追加 `pg_ha=info`，并关闭 `openraft::replication` |

Docker 入口 `docker/entrypoint.sh` 是另一层：它用下面这些变量**生成** `/etc/pg-ha.yml`，不是调用上表的覆盖逻辑。`PG_HA_RAFT_SELF` 和 `PG_HA_RAFT__SELF_ADDR` 不是同一个变量。

| 入口变量 | 作用 |
|---------|------|
| `PG_HA_NAME` / `PG_HA_SCOPE` | 节点名、集群名（默认 hostname / `pg-ha-cluster`） |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | 超级用户和复制用户（复制用户与超级用户相同） |
| `PGDATA` / `PG_PORT` / `PG_HA_BIN_DIR` | 数据目录、端口、`bin_dir`（默认按镜像里的 `PG_MAJOR` 探测） |
| `PG_HA_API_PORT` / `PG_HA_PROXY_RW_PORT` / `PG_HA_PROXY_RO_PORT` | API 与代理端口 |
| `PG_HA_RAFT_SELF` / `PG_HA_RAFT_PARTNERS` | 本节点地址，以及逗号分隔的对端列表 |
| `PG_HA_RAFT_TLS_CERT` 及 `_KEY` / `_CA_CERT` / `_CLIENT_CERT` / `_CLIENT_KEY` | 设置了 `PG_HA_RAFT_TLS_CERT` 就写入 `raft.tls` |

开发集群 `docker-compose.yml` 会设置 Raft TLS，并把 `./docker/tls` 挂到 `/etc/pg-ha/tls`。入口生成的 HA 参数是 `loop_wait: 5`、`ttl: 15`，REST 认证为 `admin` / `secret`。

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

把路径写进每台节点的 `raft.tls`（见上面的生产 YAML）。服务端用 `cert` + `key` 把 2380 换成 HTTPS；出站客户端用 `ca_cert` 校验对端，并在同时提供 `client_cert` / `client_key` 时带上客户端证书。

`require_client_cert` 可以写在 YAML 里，但 `src/main.rs` 创建 Rustls 配置时没有读取它，服务端不会拒绝没有客户端证书的连接。不要把它当成已经生效的 mTLS。

REST API（8008）不受 `raft.tls` 影响，始终是 HTTP。

---

## Docker 部署

### PostgreSQL 版本选择

镜像是官方 `postgres:${PG_VERSION}`。`make up` 把 `PG_VERSION` 传给 compose，默认 **16**。`docker/Dockerfile` 在没收到该参数时自己的 `ARG` 默认是 19，正常走 `make up` 不会用到这个默认值。

```bash
# 默认 PostgreSQL 16
make up

# 换官方镜像标签
make up PG_VERSION=18

PG_VERSION=17 docker compose build
docker compose up -d
```

entrypoint 按容器内的 `PG_MAJOR`（或 `PG_HA_BIN_DIR` / `PGDATA`）填写 `bin_dir` 和 `data_dir`。

仓库里的编排文件是 `docker-compose.yml`（开发集群，Raft TLS、scope `test-cluster`）。下面的生产示例不是仓库里的文件，需要另存后使用，并且应带上与开发集群相同的 TLS 变量，或改成挂载你自己的证书。

### 生产 docker-compose 示例

```yaml
# docker-compose.prod.yml（示例，仓库中没有这个文件）
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
      PGDATA: /var/lib/postgresql/data
      PG_HA_RAFT_SELF: "node1:2380"
      PG_HA_RAFT_PARTNERS: "node2:2380,node3:2380"
      PG_HA_RAFT_TLS_CERT: /etc/pg-ha/tls/server.crt
      PG_HA_RAFT_TLS_KEY: /etc/pg-ha/tls/server.key
      PG_HA_RAFT_TLS_CA_CERT: /etc/pg-ha/tls/ca.crt
      PG_HA_RAFT_TLS_CLIENT_CERT: /etc/pg-ha/tls/client.crt
      PG_HA_RAFT_TLS_CLIENT_KEY: /etc/pg-ha/tls/client.key
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
      - ./docker/tls:/etc/pg-ha/tls:ro
    networks:
      - pg-ha-net
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
      PGDATA: /var/lib/postgresql/data
      PG_HA_RAFT_SELF: "node2:2380"
      PG_HA_RAFT_PARTNERS: "node1:2380,node3:2380"
      PG_HA_RAFT_TLS_CERT: /etc/pg-ha/tls/server.crt
      PG_HA_RAFT_TLS_KEY: /etc/pg-ha/tls/server.key
      PG_HA_RAFT_TLS_CA_CERT: /etc/pg-ha/tls/ca.crt
      PG_HA_RAFT_TLS_CLIENT_CERT: /etc/pg-ha/tls/client.crt
      PG_HA_RAFT_TLS_CLIENT_KEY: /etc/pg-ha/tls/client.key
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
      - ./docker/tls:/etc/pg-ha/tls:ro
    networks:
      - pg-ha-net
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
      PGDATA: /var/lib/postgresql/data
      PG_HA_RAFT_SELF: "node3:2380"
      PG_HA_RAFT_PARTNERS: "node1:2380,node2:2380"
      PG_HA_RAFT_TLS_CERT: /etc/pg-ha/tls/server.crt
      PG_HA_RAFT_TLS_KEY: /etc/pg-ha/tls/server.key
      PG_HA_RAFT_TLS_CA_CERT: /etc/pg-ha/tls/ca.crt
      PG_HA_RAFT_TLS_CLIENT_CERT: /etc/pg-ha/tls/client.crt
      PG_HA_RAFT_TLS_CLIENT_KEY: /etc/pg-ha/tls/client.key
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
      - ./docker/tls:/etc/pg-ha/tls:ro
    networks:
      - pg-ha-net
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

启动前先 `make build` 再构建镜像（`docker/Dockerfile` 复制的是已经编好的 Linux 二进制，不会在镜像构建时编译 Rust）：

```bash
export PG_PASSWORD="your-secure-password"
make build
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d
curl -s http://localhost:8008/cluster | jq .
```

开发集群直接 `make up`，不要套这份示例。`make down` 会删掉 Raft 和 PGDATA 卷。

---

## 端口说明

| 端口 | 协议 | 方向 | 用途 |
|------|------|------|------|
| 5432 | TCP | 入站 | PostgreSQL 客户端连接（直连） |
| 8008 | HTTP | 入站 | REST API（健康检查无需认证；管理端点在配置了 `restapi.username` 时要 Basic Auth）。当前没有内建 TLS |
| 2380 | HTTP 或 HTTPS | 节点间 | Raft RPC。未配置 `raft.tls` 时为 HTTP，配置后为 HTTPS |
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
