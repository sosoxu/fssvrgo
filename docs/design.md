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
│  │ LocalStorage │ │  Database (PostgreSQL)        │  │
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
- **认证授权**：unary/stream 拦截器统一执行 API Key/JWT 认证和方法级 RBAC
- **指标**：使用独立 gRPC 指标，以完整方法名和规范 gRPC code 记录
- **优雅关闭**：10 秒超时后强制停止

### 2.2 服务层

#### FileManager (`internal/service/filemanager/`)

文件管理的核心服务，负责文件的上传、下载、删除、重命名和元数据查询。

**设计要点**：
- 双层锁机制：本地 `sync.Mutex`（进程内互斥）+ 分布式锁（跨实例互斥）
- 写操作流程：流数据 staging → 获取本地锁和分布式锁 → 获取 PostgreSQL fenced transaction → 原子替换存储 → 更新元数据 → 再次校验 token 并提交
- 重命名按路径排序同时锁定源和目标，避免不同源文件争抢同一目标；元数据查询错误立即中止
- 幂等上传：文件已存在时覆盖更新而非报错

#### FileTransferService (`internal/service/transfer/`)

文件传输服务，管理上传/下载会话。

**顺序上传流程**：
1. 创建会话 → 预分配临时文件（Truncate）
2. 逐块写入 → 要求 `offset == uploaded_size` → 增量 SHA256 计算
3. 每块写入后先 `fsync`，检查会话租约，再更新共享会话存储；任一步失败均不确认该进度
4. 完成上传 → 哈希校验 → 获取带续期状态的分布式锁和 namespace lease → 在 fenced transaction 中原子移动临时文件、提交文件元数据与终态账本

**分段并发上传流程**：
1. 创建会话 → 预分配临时文件 → 计算建议分段大小
2. 并发写入各分段（`WriteAt` 指定偏移量），拒绝分片重叠和不一致重试
3. 每次写入持久化分片清单、覆盖区间和进度
4. 完成上传 → 校验全区间无空洞 → 哈希校验 → 获取分布式锁 → 移动文件

**并发下载**：
- `ParallelDownloadChunks` 方法支持最多 8 并发分段读取
- 使用信号量控制并发数

**会话管理**：
- 内存存储：`sync.Map`
- Redis 存储：会话序列化为 JSON，每个已确认上传块均写入
- 跨实例恢复：Redis 保存状态，共享 `storage.temp_dir` 保存临时数据；恢复时重新打开文件句柄
- 进度查询：直接读取共享会话存储，支持跨实例观察
- 过期清理：与上传使用同一会话锁，重新读取共享 `updated_at` 后清理，避免旧实例误删活动会话

#### DirectoryManager (`internal/service/directory/`)

目录管理服务，负责目录的创建、删除、重命名和元数据查询。

**设计要点**：
- 删除支持递归和非递归模式
- 递归删除在单个 fenced transaction 内用集合更新软删除整棵元数据树；本地目录先原子移动到同盘备份，事务失败恢复、成功后清理
- 本地目录重命名同时锁定源和目标，在 exact-path fence 下原子移动整个目录并事务更新所有子路径；事务或租约失败时移动回源路径

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

### 2.4 存储层

#### StorageAdapter 接口

```go
type StorageAdapter interface {
    StorageType() string
    ValidatePath(string) error
    Write(string, []byte) error
    WriteAt(string, []byte, int64) error
    WriteFromTempFile(string, string) error
    WriteFromReader(string, io.Reader) error
    Read(string) ([]byte, error)
    ReadAt(string, int, int64) ([]byte, error)
    OpenReader(string) (io.ReadCloser, error)
    Remove(string) error
    Exists(string) bool
    List(string) ([]string, error)
    GetSize(string) (int64, error)
    Rename(string, string) error
    CreateDirectory(string) error
    RemoveDirectory(string) error
    CleanPathLocks()
}
```

#### LocalStorage (`internal/storage/local.go`)

本地文件系统存储实现。

**设计要点**：
- `sync.Map` 实现细粒度路径锁，不同文件并发无阻塞
- `WriteAt` 支持文件预分配和偏移写入
- 普通写入先写同目录临时文件、`fsync` 后 `rename`，避免目标文件半写
- HTTP 大文件流先写存储根下隐藏的 `.fssvr-staging` 保留目录并 `fsync`；该目录不属于用户命名空间，最终 fence 内以同文件系统 `rename` 原子提交
- 覆盖提交为旧目标创建可回滚备份；元数据写入或锁续期失败时恢复旧文件
- `WriteFromTempFile` 在同文件系统优先使用 `os.Rename` 原子移动
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
- 服务配置只接受 PostgreSQL，单机和多实例运行采用同一数据库语义
- SQLite 方言和驱动仅供隔离功能测试及兼容性测试使用；其结果不得作为多实例或并发性能验收数据

#### 层级命名空间租约 (`internal/database/namespace_lock.go`)

- PostgreSQL `namespace_lock_holders` 保存 `path/token/mode/fence_token/expires_at`；shared holder 使用事务级 shared advisory lock，exclusive holder 使用独占 advisory lock，保证冲突判定原子且共享获取不串行
- 文件读写为祖先目录和文件自身申请 shared holder；目录变更为祖先申请 shared、对子树根申请 exclusive holder
- `namespace_fence_seq` 分配单调 token，`namespace_fence_heads` 持久化每个最终写路径的最高 token；最终写事务按路径顺序持有 lease 的全部层级 shared/exclusive advisory locks，并在 exact-path lock 内只允许当前或更高 token 推进 head。较旧 token 即使 holder 仍存活也会被拒绝，holder 在事务中途过期也不会让新父目录操作越过旧子文件事务
- holder 获取和续期都是短 SQL/事务，不长期占用连接；获取时按路径顺序持有 advisory decision lock，再用单条 CTE 批量清理过期 holder、判定冲突并写入全部 holder，避免深层路径逐条往返；同一目录的兄弟文件 shared holder 兼容
- 过期与续期统一使用 PostgreSQL `CURRENT_TIMESTAMP`；过期 holder 不能被旧实例重新续活
- HTTP/gRPC 流式响应和下载会话持有 shared 租约直到完成，防止目录在传输中途移动
- 下载会话把 holder token 和加密标志持久化到 Redis；跨实例接管复用同一 token，并在接管节点重建明文临时文件
- 同一下载会话的分块读取持有 shared 操作租约，完成/中止持有 exclusive 操作租约；完成删除共享状态后，旧实例不能凭本地缓存继续读取

#### 数据模型

| 表名 | 用途 | 关键字段 |
|------|------|---------|
| `files` | 文件元数据 | id, path, name, size, hash, storage_type, is_deleted |
| `directories` | 目录元数据 | id, path, name, is_deleted |
| `transfer_tasks` | 传输任务 | id, type, file_id, client_id, offset, total_size, status |
| `audit_log` | 审计日志 | id, timestamp, operation, resource_path, user_identifier, client_ip |
| `api_keys` | API 密钥 | id, key_hash, name, permissions, expires_at, is_active |
| `namespace_lock_holders` | 层级命名空间与下载会话租约 holder | path, token, mode, fence_token, expires_at |
| `namespace_fence_heads` | 最终写路径已接受的最高 fencing token | path, fence_token, updated_at |
| `transfer_session_results` | 上传完成/取消终态与幂等结果（24h） | session_id, session_type, status, file_id, file_path, hash, expires_at |
| `schema_migrations` | 数据库迁移 | version, name, applied_at |

**软删除设计**：文件和目录使用 `is_deleted` 标记，删除操作不物理删除记录。

### 2.6 认证模块 (`internal/auth/`)

**AuthService** 设计：
- API Key 哈希存储（SHA256）
- 内存用户管理（`map[string]*User`）
- 基于角色的权限控制（admin/user）
- JWT access/refresh 有效期来自配置，refresh `jti` 一次性消费
- IP 级认证失败计数和速率限制；Redis 启用时与 refresh 撤销状态跨实例共享
- 最大失败次数：10 次，封禁时长：300 秒

### 2.7 加密模块 (`internal/crypto/`)

**CryptoService** 设计：
- 算法：AES-256-GCM
- 密钥来源：hex 编码的 32 字节密钥，或使用 scrypt 从通行短语派生 32 字节密钥
- 文件格式：魔数与版本标识、分块大小、明文总大小，以及逐块长度、随机 Nonce 和 GCM 密文
- 当前分块大小为 4 MiB，每块独立认证，上传与下载内存占用有界
- 解密兼容旧版 Base64 整文件格式；新写入统一使用分块格式

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
- `fsserver_grpc_requests_total`：gRPC 请求总数（method, code）
- `fsserver_grpc_request_duration_seconds`：gRPC 请求延迟（method）
- `fsserver_upload_size_bytes`：上传文件大小分布
- `fsserver_active_uploads`：当前正在处理数据的上传请求数

请求延迟 bucket 覆盖 5ms 到 120s，上传大小 bucket 覆盖 1KiB 到 4GiB；gRPC 指标拦截器位于认证外层，因此认证失败也纳入 code 统计。

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
| 服务数据库 | PostgreSQL (lib/pq) | 单机和多实例统一使用的关系型数据库 |
| 测试数据库 | SQLite (modernc.org/sqlite) | 仅用于隔离功能和方言兼容测试 |
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
database:      # PostgreSQL 数据库配置
logging:       # 日志配置（级别、格式、输出）
cache:         # 缓存配置（类型、TTL、容量）
redis:         # Redis 配置（地址、连接池）
etcd:          # etcd 配置（端点、前缀）
consistency:   # 当前只接受 none；eventual/strong 尚未实现复制链路
discovery:     # 服务发现配置（类型、地址、间隔）
auth:          # 认证配置（密钥、Token 有效期）
crypto:        # 加密配置（算法、密钥文件）
```

## 7. 当前限制

- `consistency.eventual` 与 `consistency.strong` 没有接入真实读写和复制流程，启动配置校验会拒绝这两个值。
- Redis 锁可报告续期失败；PostgreSQL 最终写使用单调 fencing token 和持久化 path head，低 token 无法再次提交。本地文件系统本身不解析 token，仍不能承诺跨数据库与存储的全局线性一致。
- 本地文件创建、覆盖、删除和目录重命名具有运行时失败补偿；没有持久化操作日志，进程崩溃窗口仍需启动恢复机制。
- 目录操作与子文件读写已有共享/独占层级租约和最终写 fence；递归删除元数据使用单个集合事务，本地目录通过同盘备份补偿。MinIO 清理仍在 DB 提交后执行，且没有对象存储与数据库的全局事务。
- 上传块逐次 `fsync` 优先保证确认持久性；固定环境性能门禁未完成，优化必须以 WAL/组提交等不削弱确认语义的方案为前提。
- 加密下载仍先生成完整明文临时文件，内存有界但首字节延迟和临时空间开销尚未闭环。
- HTTP 变更类成功操作与失败响应、gRPC 成功/失败请求已纳入审计；正常关闭会先停止接收、排空队列，并用调用方 context 完成最终刷新。HTTP 读取类成功请求尚未完整覆盖，进程崩溃可靠投递仍需要持久化 outbox。
- 本轮没有 MinIO 环境，MinIO 专属目录能力、覆盖补偿和真实集成行为未验收。

## 8. 测试策略

### 8.1 单元测试

- 各模块独立测试：`internal/...` 下的 `_test.go` 文件
- 覆盖核心逻辑：认证、配置、加密、工具函数

### 8.2 集成测试

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
