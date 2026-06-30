# fssvrgo

高性能分布式文件存储服务，支持 HTTP 和 gRPC 双协议访问，提供大文件分段并发传输、多实例部署数据一致性保障。

## 特性

- **双协议支持** — 同时提供 RESTful HTTP API 和 gRPC 服务
- **大文件分段传输** — 支持并发分段上传/下载（Multipart Upload/Parallel Download），GB 级文件传输效率显著提升
- **增量哈希校验** — 顺序上传时边写边计算 SHA256，完成时无需重新读取文件
- **多数据库支持** — SQLite（单机）和 PostgreSQL（生产），通过配置切换，SQL 方言自动翻译
- **分布式一致性** — Redis 分布式锁（SET NX + Lua 原子解锁）+ Redis 会话存储，支持多实例部署
- **文件句柄缓存** — 上传会话保持临时文件打开，避免每次 chunk 的 open/close 开销
- **预分配空间** — 创建上传会话时预分配文件空间（Truncate），减少文件系统碎片
- **细粒度路径锁** — sync.Map 实现按路径加锁，不同文件并发无阻塞
- **无锁进度追踪** — atomic.AddInt64 实现上传/下载进度更新，零锁竞争
- **Prepared Statement 缓存** — 数据库查询自动缓存预处理语句，减少 SQL 解析开销
- **指数退避重试** — 分布式锁获取采用指数退避 + 随机抖动策略

## 架构

```
┌─────────────────────────────────────────────────────┐
│                    Client                            │
├────────────────────┬────────────────────────────────┤
│     HTTP API       │         gRPC Service           │
│  (Gin Framework)   │                                │
├────────────────────┴────────────────────────────────┤
│                  Service Layer                       │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────┐ │
│  │ FileManager  │ │  Transfer    │ │  Directory   │ │
│  │              │ │  Service     │ │  Manager     │ │
│  │              │ │ ┌──────────┐ │ │              │ │
│  │              │ │ │Multipart │ │ │              │ │
│  │              │ │ │Upload    │ │ │              │ │
│  └──────────────┘ │ └──────────┘ │ └─────────────┘ │
├───────────────────┴──────────────┴──────────────────┤
│              Distributed Layer                       │
│  ┌──────────────┐ ┌──────────────┐                  │
│  │ Distributed  │ │  Session     │                  │
│  │ Lock         │ │  Store       │                  │
│  │ (Redis/Local)│ │ (Redis/Mem)  │                  │
│  └──────────────┘ └──────────────┘                  │
├─────────────────────────────────────────────────────┤
│              Storage & Database                      │
│  ┌──────────────┐ ┌──────────────────────────────┐  │
│  │ LocalStorage │ │  Database (SQLite/PostgreSQL) │  │
│  │ (sync.Map    │ │  - Dialect Translation        │  │
│  │  path locks) │ │  - Prepared Stmt Cache        │  │
│  └──────────────┘ └──────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

## 项目结构

```
fssvrgo/
├── cmd/
│   └── fsserver/          # 程序入口
│       └── main.go
├── internal/
│   ├── api/
│   │   ├── http/          # HTTP 服务器 (Gin)
│   │   └── grpc/          # gRPC 服务器
│   ├── auth/              # 认证服务 (API Key)
│   ├── cache/             # 缓存服务
│   ├── config/            # 配置加载与校验
│   ├── crypto/            # 加密服务 (AES-256-GCM)
│   ├── database/          # 数据库层
│   │   ├── database.go    #   连接管理 (SQLite/PostgreSQL)
│   │   ├── db.go          #   DB 封装 (预处理语句缓存)
│   │   ├── dialect.go     #   SQL 方言翻译 (? → $1, $2, ...)
│   │   ├── metadata.go    #   文件元数据 CRUD
│   │   └── migration.go   #   数据库迁移
│   ├── distributed/       # 分布式组件
│   │   ├── lock.go        #   分布式锁接口 + 本地锁
│   │   ├── redis.go       #   Redis 分布式锁 + 会话存储
│   │   └── session.go     #   会话存储接口
│   ├── logger/            # 日志 (Zap)
│   ├── metrics/           # Prometheus 指标
│   ├── service/
│   │   ├── directory/     # 目录管理
│   │   ├── filelist/      # 文件列表查询
│   │   ├── filemanager/   # 文件管理 (上传/下载/删除/重命名)
│   │   └── transfer/      # 文件传输服务
│   │       ├── service.go     # 顺序上传/下载 + 增量哈希
│   │       └── multipart.go   # 分段并发上传/下载
│   ├── storage/           # 存储适配层 (本地文件系统)
│   └── utils/             # 工具函数
├── proto/                 # gRPC Proto 定义
├── tests/                 # 集成测试
│   ├── http_api_test.go                      # HTTP API 测试
│   ├── grpc_service_test.go                  # gRPC 服务测试
│   ├── multi_instance_consistency_test.go    # 多实例一致性 (HTTP)
│   ├── multi_instance_grpc_consistency_test.go # 多实例一致性 (gRPC)
│   ├── redis_lock_consistency_test.go        # Redis 锁一致性
│   ├── postgresql_consistency_test.go        # PostgreSQL 集成测试
│   ├── perf_postgresql_redis_test.go         # PostgreSQL+Redis 性能测试
│   ├── large_file_parallel_test.go           # 大文件并发分段读写测试
│   └── segmented_vs_nonsegmented_test.go     # 分段 vs 不分段对比测试
├── config.yaml            # 配置文件
└── go.mod
```

## 快速开始

### 编译

```bash
go build -o fsserver ./cmd/fsserver
```

### 配置

编辑 `config.yaml`：

```yaml
server:
  http_port: 8080
  grpc_port: 9090
  grpc_enabled: true
  max_upload_size_mb: 4096
  max_chunk_size_mb: 256

storage:
  type: local
  local:
    root_dir: /data/fsserver

database:
  type: sqlite          # 或 postgresql
  path: /data/fsserver/fsserver.db

redis:
  enabled: false        # 多实例部署时启用
  address: "localhost:6379"
```

### PostgreSQL 配置

```yaml
database:
  type: postgresql
  host: localhost
  port: 5432
  name: fsserver
  user: fsserver
  password: your_password
  sslmode: disable
  pool_size: 25
```

### 运行

```bash
./fsserver              # 使用默认 config.yaml
./fsserver /path/to/config.yaml  # 指定配置文件
```

## API

### 文件操作

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/files` | 整体上传文件 |
| GET | `/api/v1/files/*path` | 下载文件（支持 Range） |
| DELETE | `/api/v1/files/*path` | 删除文件 |
| PATCH | `/api/v1/files/*path` | 重命名文件 |
| GET | `/api/v1/files` | 列出文件 |
| GET | `/api/v1/metadata/*path` | 获取元数据 |

### 流式上传（顺序）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/uploads` | 创建上传会话 |
| PUT | `/api/v1/uploads/:id/chunk` | 上传数据块 |
| GET | `/api/v1/uploads/:id/progress` | 查询进度 |
| POST | `/api/v1/uploads/:id/complete` | 完成上传 |
| DELETE | `/api/v1/uploads/:id` | 取消上传 |

### 分段并发上传

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/multipart-uploads` | 创建分段上传会话 |
| PUT | `/api/v1/multipart-uploads/:id/parts/:partNumber` | 上传分段数据 |
| GET | `/api/v1/multipart-uploads/:id` | 查询分段进度 |
| POST | `/api/v1/multipart-uploads/:id/complete` | 完成分段上传 |
| DELETE | `/api/v1/multipart-uploads/:id` | 取消分段上传 |

### 请求示例

**整体上传：**

```bash
curl -X POST http://localhost:8080/api/v1/files \
  -H "X-API-Key: your-key" \
  -F "file=@/path/to/file.txt" \
  -F "path=/docs/file.txt"
```

**流式上传：**

```bash
# 1. 创建会话
curl -X POST http://localhost:8080/api/v1/uploads \
  -H "X-API-Key: your-key" \
  -H "Content-Type: application/json" \
  -d '{"file_path":"/large.bin","file_name":"large.bin","total_size":1073741824,"hash":"abc..."}'

# 2. 逐块上传
curl -X PUT http://localhost:8080/api/v1/uploads/{session_id}/chunk \
  -H "X-API-Key: your-key" \
  -F "data=@chunk.bin" \
  -F "offset=0"

# 3. 完成上传
curl -X POST http://localhost:8080/api/v1/uploads/{session_id}/complete \
  -H "X-API-Key: your-key"
```

**分段并发上传：**

```bash
# 1. 创建分段会话
curl -X POST http://localhost:8080/api/v1/multipart-uploads \
  -H "X-API-Key: your-key" \
  -H "Content-Type: application/json" \
  -d '{"file_path":"/huge.bin","file_name":"huge.bin","total_size":4294967296,"hash":"def..."}'
# 返回 {"session_id":"...","part_size":67108864}

# 2. 并发上传各分段（可并行）
curl -X PUT http://localhost:8080/api/v1/multipart-uploads/{session_id}/parts/1 \
  -H "X-API-Key: your-key" \
  -F "data=@part1.bin" \
  -F "offset=0"

curl -X PUT http://localhost:8080/api/v1/multipart-uploads/{session_id}/parts/2 \
  -H "X-API-Key: your-key" \
  -F "data=@part2.bin" \
  -F "offset=67108864"

# 3. 完成上传
curl -X POST http://localhost:8080/api/v1/multipart-uploads/{session_id}/complete \
  -H "X-API-Key: your-key"
```

**Range 下载：**

```bash
curl -H "Range: bytes=0-1048575" \
  http://localhost:8080/api/v1/files/large.bin \
  -H "X-API-Key: your-key" \
  -o chunk1.bin
```

## 分段大小建议

| 文件大小 | 建议分段大小 |
|---------|------------|
| ≤ 100MB | 8MB |
| 100MB ~ 1GB | 16MB |
| 1GB ~ 10GB | 64MB |
| > 10GB | 128MB |

## 性能

测试环境：Intel Xeon Platinum 8457C, 本地文件系统, SQLite

### 分段 vs 不分段

| 操作 | 协议 | 模式 | 并发 | 吞吐量 |
|------|------|------|------|--------|
| 上传 256MB | HTTP | 不分段 | 1 | 105.55 MB/s |
| 上传 256MB | HTTP | 分段 | 4 | **117.28 MB/s** (+11%) |
| 上传 256MB | gRPC | 不分段 | 1 | 128.53 MB/s |
| 上传 256MB | gRPC | 分段 | 8 | **134.65 MB/s** (+5%) |
| 下载 100MB | HTTP | 不分段 | 1 | 313.99 MB/s |
| 下载 100MB | HTTP | 分段 | 4 | **503.62 MB/s** (+60%) |
| 下载 100MB | gRPC | 不分段 | 1 | 820.93 MB/s |
| 下载 100MB | gRPC | 分段 | 4 | **1309.00 MB/s** (+59%) |
| 下载 256MB | gRPC | 分段 | 4 | **1375.40 MB/s** |

### HTTP vs gRPC

| 操作 | HTTP | gRPC | gRPC 加速 |
|------|------|------|-----------|
| 下载 100MB (分段 c=4) | 504 MB/s | 1309 MB/s | **2.60x** |
| 下载 256MB (分段 c=4) | 410 MB/s | 1375 MB/s | **3.35x** |

### 关键结论

- **下载场景**：分段传输全面优于不分段，gRPC 分段4并发比不分段快 59%~67%
- **上传场景**：小文件不分段略优（会话开销），大文件分段开始占优
- **最优并发数**：4 并发是最佳平衡点，超过后锁竞争增加
- **gRPC 下载远快于 HTTP**：直连内存操作 vs 网络栈序列化，差距 2.6x~3.4x

## 多实例部署

### 架构

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Instance1│     │ Instance2│     │ Instance3│
│ HTTP+gRPC│     │ HTTP+gRPC│     │ HTTP+gRPC│
└────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │
     └────────────────┼────────────────┘
                      │
          ┌───────────┴───────────┐
          │                       │
    ┌─────┴──────┐        ┌──────┴─────┐
    │ PostgreSQL │        │   Redis    │
    │  (共享DB)  │        │ (锁+会话)  │
    └────────────┘        └────────────┘
          │
    ┌─────┴──────┐
    │ 共享存储    │
    │ (NFS/SSD)  │
    └────────────┘
```

### 配置

```yaml
database:
  type: postgresql
  host: pg-host
  port: 5432
  name: fsserver
  user: fsserver
  password: password
  pool_size: 25

redis:
  enabled: true
  address: "redis-host:6379"
  pool_size: 10

storage:
  type: local
  local:
    root_dir: /shared/fsserver   # 共享存储挂载点
```

### 一致性保障

- **分布式锁**：文件操作前获取 Redis 锁，保证跨实例互斥
- **会话可见性**：上传/下载会话存储在 Redis，任意实例可恢复
- **锁获取策略**：指数退避 + 随机抖动，避免惊群效应
- **Redis 会话更新批量化**：每 8 个 chunk 更新一次，减少 87.5% 的 Redis 写入

## 测试

```bash
# 单元测试
go test ./internal/...

# HTTP API 集成测试
go test ./tests/ -run TestHTTPAPI -v

# gRPC 服务集成测试
go test ./tests/ -run TestGRPCService -v

# 多实例一致性测试
go test ./tests/ -run TestMultiInstance -v

# Redis 锁一致性测试（需要 Redis）
go test ./tests/ -run TestRedisLock -v

# PostgreSQL 集成测试（需要 PostgreSQL）
go test ./tests/ -run TestPostgreSQL -v

# 大文件并发分段读写测试
go test ./tests/ -run TestMultipartUpload -v
go test ./tests/ -run TestParallelDownload -v

# 分段 vs 不分段性能对比
go test ./tests/ -run TestSegmentedVsNonSegmented -v

# Benchmark
go test ./tests/ -bench=BenchmarkMultipartUpload -benchmem
go test ./tests/ -bench=BenchmarkGRPC_SegmentedDownload -benchmem
```

## 技术栈

| 组件 | 技术 |
|------|------|
| HTTP 框架 | Gin |
| gRPC | google.golang.org/grpc |
| 数据库 | SQLite (modernc.org/sqlite) / PostgreSQL (lib/pq) |
| 分布式锁/会话 | Redis (go-redis/v9) |
| 日志 | Zap |
| 指标 | Prometheus client_golang |
| 配置 | YAML (goccy/go-yaml) |
| UUID | google/uuid |

## License

MIT
