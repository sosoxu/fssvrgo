# fssvrgo 设计文档

## 1. 系统架构

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

## 2. 模块设计

### 2.1 API 层

#### HTTP 服务器 (`internal/api/http/`)

- **框架**：Gin（Release 模式）
- **中间件链**：CORS → Metrics → Auth → Handler
- **路由设计**：统一 `/api/v1` 前缀，RESTful 风格
- **错误处理**：统一 JSON 错误响应格式 `{"error": "message"}`
- **大文件优化**：> 32MB 文件使用流式读取（`io.ReadSeeker`），避免全量加载到内存

#### gRPC 服务器 (`internal/api/grpc/`)

- **Proto 定义**：`proto/file_service.proto`
- **流式传输**：UploadFile（Client Streaming）、DownloadFile（Server Streaming）
- **优雅关闭**：10 秒超时后强制停止

### 2.2 服务层

#### FileManager (`internal/service/filemanager/`)

文件管理的核心服务，负责文件的上传、下载、删除、重命名和元数据查询。

**设计要点**：
- 双层锁机制：本地 `sync.Mutex`（进程内互斥）+ 分布式锁（跨实例互斥）
- 写操作流程：获取本地锁 → 获取分布式锁 → 执行操作 → 释放分布式锁 → 释放本地锁
- 幂等上传：文件已存在时覆盖更新而非报错

#### FileTransferService (`internal/service/transfer/`)

文件传输服务，管理上传/下载会话。

**顺序上传流程**：
1. 创建会话 → 预分配临时文件（Truncate）
2. 逐块写入 → 增量 SHA256 计算（仅顺序写入时有效）
3. 完成上传 → 哈希校验 → 获取分布式锁 → 移动临时文件到存储 → 写入元数据

**分段并发上传流程**：
1. 创建会话 → 预分配临时文件 → 计算建议分段大小
2. 并发写入各分段（`WriteAt` 指定偏移量）
3. 完成上传 → 校验所有分段状态 → 哈希校验 → 获取分布式锁 → 移动文件

**并发下载**：
- `ParallelDownloadChunks` 方法支持最多 8 并发分段读取
- 使用信号量控制并发数

**会话管理**：
- 内存存储：`sync.Map`
- Redis 存储：会话序列化为 JSON，每 8 个 chunk 批量更新
- 过期清理：后台协程定期清理超时会话

#### DirectoryManager (`internal/service/directory/`)

目录管理服务，负责目录的创建、删除、重命名和元数据查询。

**设计要点**：
- 删除支持递归和非递归模式
- 递归删除按批次（500 条）处理，避免大量数据时的内存问题
- 重命名时级联更新所有子文件和子目录的路径

#### FileListService (`internal/service/filelist/`)

文件列表查询服务，支持分页、排序和递归/非递归列表。

**设计要点**：
- 使用 UNION ALL 合并文件和目录查询结果
- 支持按 name、path、size、created_at、type 排序
- 防止 SQL 注入：排序字段白名单校验

### 2.3 分布式层

#### DistributedLock (`internal/distributed/lock.go`)

分布式锁接口，提供 Lock、Unlock、Extend 三个方法。

**实现**：
- `RedisDistributedLock`：基于 Redis SET NX + Lua 原子解锁
- `LocalDistributedLock`：本地空实现，单机部署时使用

**锁获取策略**（`AcquireLock`）：
- 指数退避：基础延迟 × 2^(重试次数-1)
- 随机抖动：退避时间 + [0, 退避/2) 随机值
- 最大退避：2 秒
- 上下文取消支持

#### SessionStore (`internal/distributed/session.go`)

会话存储接口，提供 Set、Get、Delete、Exists 四个方法。

**实现**：
- `RedisSessionStore`：基于 Redis，支持 TTL
- `MemorySessionStore`：基于 `sync.RWMutex` + `map`，支持 TTL

#### 实现状态说明（2026-09-11 评审）

以下模块已有代码骨架，但**尚未接入请求路径**，启用配置不会改变运行时行为：

| 模块 | 现状 |
|---|---|
| `internal/etcd` | 仅提供客户端封装，启动时按配置连接，无业务消费 |
| `internal/discovery` | 支持 `Register`；无组件消费 `Discover`/`Watch`，实例间尚不互相发现 |
| `internal/consistency` | 启动时校验仲裁参数并构造管理器；读写路径从不调用，一致性级别设置不生效 |

启动日志会对"启用但未接线"的模块输出 `WARN`，避免运维误判。详见
`docs/design-review-20260911.md` 中的 R2。

### 2.4 存储层

#### StorageAdapter 接口

```go
type StorageAdapter interface {
    StorageType() string
    Write(string, []byte) error
    WriteFromTempFile(string, string) error
    WriteFromReader(string, io.Reader) error
    Read(string) ([]byte, error)
    ReadAt(string, int, int64) ([]byte, error)
    OpenReader(string) (io.ReadCloser, error)
    Remove(string) error
    Exists(string) (bool, error)
    List(string) ([]string, error)
    GetSize(string) (int64, error)
    Rename(string, string) error
    CreateDirectory(string) error
    RemoveDirectory(string) error
}
```

**接口边界说明**：

- `WriteAt`（原地随机写）与 `ValidatePath`（路径校验）**不在接口内**。前者是本地文件系统能力，
  对象存储无法提供（MinIO 实现直接返回 `ErrWriteAtUnsupported`），需要拼装大文件的调用方走
  "写本地临时文件 → `WriteFromTempFile`"；后者是接口的**不变量**而非调用方要主动调用的方法，
  每个操作都必须自行拒绝越出根目录的路径。两者仍保留在具体类型上，供后端测试使用。
- 需要枚举全部对象的场景（如元数据/存储对账）通过**可选能力接口** `ObjectLister`
  （`ListObjects(ctx) ([]string, error)`）获得，而不是把枚举塞进 `StorageAdapter`；
  不支持枚举的后端只是不实现该接口。
- 存储路径锁表（`CleanPathLocks`）同样不属于接口，它是本地实现细节，只对测试开放。
- `Exists` 返回 `(bool, error)`：只有确认不存在才返回 `(false, nil)`，后端故障
  （网络错误、权限问题、路径非法等）必须以 error 形式上抛，避免调用方把基础设施故障
  误判为"文件不存在"从而走覆盖/新建分支。

#### LocalStorage (`internal/storage/local.go`)

本地文件系统存储实现。

**设计要点**：
- `sync.Map` 实现细粒度路径锁，不同文件并发无阻塞
- `WriteAt` 支持文件预分配和偏移写入
- `WriteFromTempFile` 使用 `os.Rename` 原子移动
- 路径安全：禁止 `..` 路径遍历

#### MinIOStorage (`internal/storage/minio.go`)

MinIO/S3 兼容对象存储实现。

**设计要点**：
- `WriteAt` 限制 512MB 以内（需读取全量数据再回写）
- `WriteFromTempFile` 使用 `FPutObject` 从本地文件上传
- `WriteFromReader` 支持 `io.Seeker` 自动检测内容长度
- `Rename` 实现：复制源对象 → 删除源对象
- `List` 仅返回一级子项（非递归）

### 2.5 数据库层

#### DB 封装 (`internal/database/db.go`)

- Prepared Statement 缓存：`sync.Map` + `sql.Stmt`
- SQL 方言翻译：`?` → `$1, $2, ...`（PostgreSQL）

#### 数据模型

| 表名 | 用途 | 关键字段 |
|------|------|---------|
| `files` | 文件元数据 | id, path, name, size, hash, storage_type, is_deleted |
| `directories` | 目录元数据 | id, path, name, is_deleted |
| `transfer_tasks` | 传输任务 | id, type, file_id, client_id, offset, total_size, status |
| `audit_log` | 审计日志 | id, timestamp, operation, resource_path, user_identifier, client_ip |
| `api_keys` | API 密钥 | id, key_hash, name, permissions, expires_at, is_active |
| `schema_migrations` | 数据库迁移 | version, name, applied_at |

**软删除设计**：文件和目录使用 `is_deleted` 标记，删除操作不物理删除记录。

**元数据/存储对账（reconciliation）**：

- 启动时执行一次**只读**扫描：比较 `files` 表中 `is_deleted = FALSE` 的路径集合与存储后端枚举出的对象集合，
  分别报告 `missing_in_storage`（有元数据无对象）与 `orphan_in_storage`（有对象无元数据）的数量与样本
- `maintenance.reconcile_enabled` 打开周期性对账（与清理服务同周期）
- `maintenance.reconcile_repair` 打开自动修复：删除孤儿对象、把悬空元数据软删除（不物理删除记录）
- 枚举能力由存储后端以可选接口 `ListObjects(ctx)` 实现（local 与 MinIO 均已实现）；
  不支持枚举的后端会跳过对象侧比较并在报告中说明原因

该机制针对的是"对象已写入存储但元数据写入前进程崩溃"以及反向的残留场景——
存储与元数据之间没有分布式事务，对账是当前选择的补偿手段。

### 2.6 认证模块 (`internal/auth/`)

**AuthService** 设计：
- API Key 哈希存储（SHA256）
- 内存用户管理（`map[string]*User`）
- 基于角色的权限控制（admin/user）
- IP 级认证失败计数和速率限制
- 最大失败次数：10 次，封禁时长：300 秒

### 2.7 加密模块 (`internal/crypto/`)

**CryptoService** 设计：
- 算法：AES-256-GCM
- 密钥来源：hex 编码的 32 字节密钥 或 通行短语（自动填充/截断到 32 字节）
- 加密流程：生成随机 Nonce → GCM Seal → Base64 编码
- 解密流程：Base64 解码 → 提取 Nonce → GCM Open

### 2.8 缓存模块 (`internal/cache/`)

**Cache** 设计：
- 内存缓存：`sync.RWMutex` + `map[string]*entry`
- TTL 支持：过期条目惰性删除
- LRU 淘汰：基于最早过期时间淘汰
- 最大容量限制

### 2.9 指标模块 (`internal/metrics/`)

**Metrics** 设计（Prometheus）：
- `fsserver_http_requests_total`：HTTP 请求总数（method, path, status）
- `fsserver_http_request_duration_seconds`：HTTP 请求延迟（method, path）
- `fsserver_upload_size_bytes`：上传文件大小分布
- `fsserver_active_uploads`：活跃上传数

## 3. 关键流程

### 3.1 文件上传流程

```
Client → HTTP/gRPC → Auth Middleware → FileManager/TransferService
                                              ↓
                                    Acquire Local Lock
                                              ↓
                                    Acquire Distributed Lock
                                              ↓
                                    Write to Temp File
                                              ↓
                                    Hash Verification
                                              ↓
                                    Move Temp → Storage
                                              ↓
                                    Write Metadata to DB
                                              ↓
                                    Release Locks
```

### 3.2 分段并发上传流程

```
Client → CreateMultipartUpload → Pre-allocate Temp File
                                       ↓
         ┌─────────────────────────────┐
         │  Part 1 → WriteAt(offset=0) │
         │  Part 2 → WriteAt(offset=P) │  (并发)
         │  Part N → WriteAt(offset=…) │
         └─────────────────────────────┘
                                       ↓
         CompleteMultipartUpload
                                       ↓
         Verify All Parts Completed
                                       ↓
         Hash Verification (SHA256)
                                       ↓
         Acquire Lock → Move to Storage → Update DB
```

### 3.3 多实例部署架构

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

## 4. 技术选型

| 组件 | 技术 | 说明 |
|------|------|------|
| HTTP 框架 | Gin | 高性能 HTTP 框架 |
| gRPC | google.golang.org/grpc | 流式传输支持 |
| 数据库 | SQLite (modernc.org/sqlite) | 纯 Go 实现，无 CGO 依赖 |
| 数据库 | PostgreSQL (lib/pq) | 生产环境关系型数据库 |
| 分布式锁/会话 | Redis (go-redis/v9) | 分布式协调 |
| 对象存储 | MinIO (minio-go/v7) | S3 兼容对象存储 |
| 日志 | Zap | 高性能结构化日志 |
| 指标 | Prometheus client_golang | 监控指标采集 |
| 配置 | YAML (gopkg.in/yaml.v3) | 人类可读配置格式 |
| UUID | google/uuid | 唯一标识符生成 |
| 加密 | crypto/aes + crypto/cipher | AES-256-GCM 加密 |

## 5. 数据库迁移

使用 `MigrationManager` 管理数据库版本：

- `schema_migrations` 表记录已应用的迁移版本
- 迁移按版本号顺序执行
- 每个迁移包含版本号、名称和 Up 函数
- 支持幂等执行（已应用的迁移自动跳过）

## 6. 配置结构

```yaml
server:        # 服务器配置（端口、并发、限制）
tls:           # TLS/HTTPS 配置
storage:       # 存储配置（local/minio）
database:      # 数据库配置（sqlite/postgresql）
logging:       # 日志配置（级别、格式、输出）
cache:         # 缓存配置（类型、TTL、容量）
redis:         # Redis 配置（地址、连接池）
etcd:          # etcd 配置（端点、前缀）
consistency:   # 一致性配置（级别、仲裁数）
discovery:     # 服务发现配置（类型、地址、间隔）
auth:          # 认证配置（密钥、Token 有效期）
crypto:        # 加密配置（算法、密钥文件）
```

## 7. 测试策略

### 7.1 单元测试

- 各模块独立测试：`internal/...` 下的 `_test.go` 文件
- 覆盖核心逻辑：认证、配置、加密、工具函数

### 7.2 集成测试

CI（`.github/workflows/ci.yml`）会启动 PostgreSQL 15 与 Redis 7 服务容器，并设置
`FSS_TEST_REQUIRE_INFRA=1`：数据库不可达时测试**失败**而不是跳过，避免"因为没连上库而全绿"
掩盖只在 PostgreSQL 上出现的缺陷。本地开发不设置该变量时仍保持跳过语义。

| 测试文件 | 测试内容 |
|---------|---------|
| `http_api_test.go` | HTTP API 全流程测试 |
| `grpc_service_test.go` | gRPC 服务全流程测试 |
| `multi_instance_consistency_test.go` | 多实例 HTTP 一致性 |
| `multi_instance_grpc_consistency_test.go` | 多实例 gRPC 一致性 |
| `redis_lock_consistency_test.go` | Redis 分布式锁一致性 |
| `postgresql_consistency_test.go` | PostgreSQL 集成测试 |
| `perf_postgresql_redis_test.go` | PostgreSQL+Redis 性能测试 |
| `large_file_parallel_test.go` | 大文件并发分段读写 |
| `segmented_vs_nonsegmented_test.go` | 分段 vs 不分段对比 |
