# fssvrgo 技术栈总览

> 本文档系统整理 fssvrgo 项目用到的全部技术点，涵盖存储、数据库、缓存、分布式协调、安全认证、API 协议、文件传输、一致性、可观测性、并发控制、工程化部署、性能优化、测试体系等十三个大类。
> 每个技术点标注实现位置与核心原理，便于快速了解项目技术全貌。

---

## 目录

- [一、存储与数据库](#一存储与数据库)
- [二、缓存与分布式协调](#二缓存与分布式协调)
- [三、安全与认证](#三安全与认证)
- [四、API 与协议层](#四api-与协议层)
- [五、文件传输核心](#五文件传输核心)
- [六、一致性与服务治理](#六一致性与服务治理)
- [七、可观测性](#七可观测性)
- [八、并发控制技术](#八并发控制技术)
- [九、工程化与部署](#九工程化与部署)
- [十、性能优化](#十性能优化)
- [十一、测试体系](#十一测试体系)
- [十二、工具函数与路径安全](#十二工具函数与路径安全)
- [十三、issue 修复技术方案](#十三issue-修复技术方案)
- [依赖总览](#依赖总览)

---

## 一、存储与数据库

### 1.1 可插拔存储适配器

- **技术**：`StorageAdapter` 接口抽象（15 个方法），支持本地文件系统与 MinIO/S3 对象存储
- **库**：`github.com/minio/minio-go/v7`
- **位置**：[internal/storage/local.go](../internal/storage/local.go)、[internal/storage/minio.go](../internal/storage/minio.go)
- **原理**：上层业务代码依赖接口而非实现，通过配置项 `storage.type`（local/minio）切换后端，无需改动业务代码
- **亮点**：
  - 路径穿越防护（`filepath.EvalSymlinks` 解析符号链接 + `resolveExistingAncestor` 处理新文件路径）
  - 对象存储语义适配（`WriteAt` 显式返回 `ErrWriteAtUnsupported`、目录零字节标记、`Exists` 回退子对象前缀列举）
  - 跨后端错误归一（[errors.go](../internal/storage/errors.go) `IsNotExist()` 同时识别 `os.ErrNotExist` 与 S3 `NoSuchKey`）

### 1.2 多方言 SQL 数据库抽象

- **技术**：SQLite（纯 Go 无 CGO）+ PostgreSQL 双支持，手写 SQL 无 ORM
- **库**：`modernc.org/sqlite`、`github.com/lib/pq`
- **位置**：[internal/database/database.go](../internal/database/database.go)、[internal/database/dialect.go](../internal/database/dialect.go)、[internal/database/db.go](../internal/database/db.go)
- **原理**：方言翻译层 `Dialect.Translate()` 自动将 `?` 占位符转为 PostgreSQL 的 `$N`
- **亮点**：
  - 预处理语句缓存（`sync.Map` + `*sql.Stmt`，`LoadOrStore` 防并发重复 prepare）
  - SQLite WAL 模式优化（`journal_mode=WAL`、`busy_timeout=5000`、`synchronous=NORMAL`）
  - 连接池管理（`SetMaxOpenConns`/`SetMaxIdleConns`/`SetConnMaxLifetime`/`SetConnMaxIdleTime`）
  - 事务封装 `Tx` 自动应用方言翻译
  - 迁移机制 `MigrationManager` 维护 `schema_migrations` 版本表，按 Version 升序执行

### 1.3 异步批写审计日志

- **技术**：缓冲 channel + 后台 goroutine 解耦审计持久化与请求路径
- **位置**：[internal/database/audit_writer.go](../internal/database/audit_writer.go)
- **原理**：`Submit` 非阻塞入队（满则丢弃告警，绝不阻塞请求），后台按 batchSize 或 flushInterval（默认 1s）批量 flush
- **亮点**：
  - `Close` 使用调用方 context 做最终 flush（避免自身 ctx 已取消无法 flush）
  - 逐行写入命中预处理语句缓存（而非多行 INSERT）
  - ctx.Done 时 drain channel 到 pending

### 1.4 软删除 + 定期物理清理

- **技术**：`is_deleted` 标记 + 后台清理服务
- **位置**：[internal/database/cleanup.go](../internal/database/cleanup.go)、[internal/database/metadata.go](../internal/database/metadata.go)
- **原理**：删除操作仅置 `is_deleted=TRUE`，`CleanupService` 后台按 retention 天数物理删除 DB 记录并清理存储对象
- **亮点**：跨后端使用 `storage.IsNotExist(err)`（而非 `os.IsNotExist`）兼容 MinIO `NoSuchKey` 错误

### 1.5 LIKE 通配符注入防护

- **技术**：`escapeLikePattern` 转义 `%`/`_`/`\` + `ESCAPE '\\'` 子句
- **位置**：[internal/service/directory/manager.go](../internal/service/directory/manager.go)、[internal/service/filelist/service.go](../internal/service/filelist/service.go)
- **原理**：目录名/文件名含通配符时，未转义会导致跨目录误匹配（如 `a%` 匹配所有 a 开头路径），转义后作为字面量匹配

---

## 二、缓存与分布式协调

### 2.1 双层缓存

- **技术**：内存 LRU + Redis，统一 `CacheAdapter` 接口
- **库**：`container/list`（标准库双向链表）、`github.com/redis/go-redis/v9`
- **位置**：[internal/cache/cache.go](../internal/cache/cache.go)、[internal/cache/redis.go](../internal/cache/redis.go)
- **原理**：`Cache`（内存）与 `RedisCache` 均实现 `CacheAdapter` 接口，HTTP Server 透明持有接口字段
- **亮点**：
  - O(1) LRU（`map[string]*entry` + `*list.List`，entry 内嵌 `*list.Element` 指针实现 O(1) 删除）
  - TTL 惰性过期（Get 检测过期删除）+ 后台 `cleanupLoop` 每分钟全量扫描
  - `sync.Once` 保证 `Stop()` 安全关闭 channel（可重复调用不 panic）

### 2.2 分布式锁（本地 + Redis）

- **技术**：`DistributedLock` 接口双实现
- **位置**：[internal/distributed/lock.go](../internal/distributed/lock.go)
- **原理**：Redis `SetNX` 抢锁 + Lua 脚本原子解锁/续约（check token + DEL/PEXPIRE）
- **亮点**：
  - 指数退避 + 随机 jitter 防惊群（`1<<(i-1) * baseDelay`，上限 2s，crypto/rand 生成 jitter）
  - `AcquireLockWithRenewal` 后台 goroutine 以 `ttl/3`（最小 2s）周期续约，大文件上传防锁过期
  - `LocalDistributedLock` 进程内 `map` + 过期时间，单进程测试用

### 2.3 会话存储

- **技术**：`SessionStore` 接口双实现（Redis + 内存），支持跨实例上传续传
- **位置**：[internal/distributed/session.go](../internal/distributed/session.go)
- **原理**：key 格式 `fsserver:session:<prefix>:<key>`，JSON 序列化，TTL 由 Redis 管理

### 2.4 etcd 服务注册与发现

- **技术**：Lease Keepalive 保活 + 前缀发现 + Watch 实时通知
- **库**：`go.etcd.io/etcd/client/v3`
- **位置**：[internal/discovery/discovery.go](../internal/discovery/discovery.go)、[internal/etcd/etcd.go](../internal/etcd/etcd.go)
- **原理**：注册时 `Grant` 租约（TTL = interval*3）+ `Put` 带 lease + `KeepAlive` goroutine 续约；实例宕机 lease 自动过期
- **亮点**：`Watch` 慢消费者丢弃避免阻塞（`select { case resultCh <- ...: default: }`）

---

## 三、安全与认证

### 3.1 AES-256-GCM 对称加密

- **技术**：对称加密 + scrypt 内存硬密钥派生
- **库**：`crypto/aes`、`crypto/cipher`、`crypto/rand`、`golang.org/x/crypto/scrypt`
- **位置**：[internal/crypto/crypto.go](../internal/crypto/crypto.go)
- **原理**：随机 nonce + `gcm.Seal` 认证加密 + base64 编码（nonce 前置），密钥经 scrypt（N=32768, r=8, p=1）派生 32 字节
- **亮点**：流式加密文件 `EncryptFile`/`DecryptFileStreaming`（注释坦承 AES-GCM 需全量密文认证，io.Copy 仅限制句柄持有时间）

### 3.2 JWT 双 Token 认证

- **技术**：access + refresh token，HS256 签名
- **库**：`github.com/golang-jwt/jwt/v5`
- **位置**：[internal/auth/jwt.go](../internal/auth/jwt.go)
- **原理**：`GenerateTokenPair` 同时签发 access（默认 24h）和 refresh（默认 168h），每 token 带唯一 `jti`
- **亮点**：
  - Refresh token 一次性使用（`RefreshToken` 将旧 jti 加入 `sync.Map` 黑名单，`ValidateToken` 拒绝黑名单）
  - 签名方法校验防 alg=none 攻击（强制 `*jwt.SigningMethodHMAC`）
  - 后台 `cleanupBlacklist` 机会性清理过期黑名单防内存泄漏

### 3.3 API Key 认证 + RBAC 授权

- **技术**：SHA-256 哈希存储 + 角色权限控制
- **位置**：[internal/auth/auth.go](../internal/auth/auth.go)
- **原理**：只存哈希不存明文，`ValidateApiKey` 三级查找（默认 admin key → 内存 users → 数据库 ApiKey 表）
- **亮点**：
  - admin 精确匹配 + 逗号分隔多值匹配，避免 `strings.Contains` 误判（"not-admin" 不被提权）
  - `GetUserByApiKey` 支持 JWT access token（gRPC JWT 用户授权）
  - 限流防爆破（10 次失败/300s 窗口触发 429）
  - gRPC `methodPermissions` map 按 FullMethod 映射 (resource, action)，拦截器先认证后授权

### 3.4 输入安全防护

- **技术**：路径穿越防护、CORS 安全默认、文件名校验
- **位置**：[internal/api/http/server.go](../internal/api/http/server.go)、[internal/utils/utils.go](../internal/utils/utils.go)
- **亮点**：
  - `IsValidFilePath` 多层防御（拒空/根路径，`path.Clean` 后拒 `.`/`..`/`/`，段级检查拒 `..` 和空段）
  - `IsValidFileName` 拒控制字符、`..` 子串、`/`/`\`
  - CORS 默认空字符串不开放跨域（仅显式配置时发送 CORS 头）
  - `Content-Disposition` 转义双引号 + `filename*=UTF-8''` URL 编码

### 3.5 临时文件路径校验

- **技术**：强制 tempFilePath 位于系统临时目录
- **位置**：[internal/storage/tempfile.go](../internal/storage/tempfile.go)
- **原理**：`WriteFromTempFile` 接口可上传任意本地文件，校验 `tempFilePath` 必须位于 `os.TempDir()` 或 `/tmp/` 下，防止上传 `/etc/shadow` 等敏感文件
- **亮点**：`filepath.Abs` 转绝对路径再前缀比较防相对路径绕过；双前缀检查跨平台兼容

### 3.6 gRPC 内部错误屏蔽

- **技术**：`internalError` helper 屏蔽内部错误细节
- **位置**：[internal/api/grpc/server.go](../internal/api/grpc/server.go)
- **原理**：内部错误含敏感信息（DB 路径、SQL 错误），日志记录完整错误，返回客户端统一 `codes.Internal "internal server error"`

---

## 四、API 与协议层

### 4.1 gRPC 流式服务

- **技术**：client/server streaming 上传下载 + unary RPC
- **库**：`google.golang.org/grpc`、`google.golang.org/protobuf`
- **位置**：[proto/file_service.proto](../proto/file_service.proto)、[internal/api/grpc/server.go](../internal/api/grpc/server.go)
- **原理**：`FileService` 含 9 个 RPC，`UploadRequest` 用 `oneof data` 区分 metadata 和 chunk
- **亮点**：
  - 拦截器链（`ChainUnaryInterceptor` 认证+授权+指标，`ChainStreamInterceptor` 同）
  - 小文件快速路径（单分块 ≤16MB 绕过 transferSvc 会话机制，Redis 操作 6→2，磁盘操作 6→1）
  - 输入校验防 DoS（TotalSize 正值校验、chunkSize 上限 32MB、pageSize 上限）
  - 优雅停机（`GracefulStop` 10s 超时强制 `Stop`）

### 4.2 HTTP REST API

- **技术**：Gin 框架 + 中间件链
- **库**：`github.com/gin-gonic/gin`
- **位置**：[internal/api/http/server.go](../internal/api/http/server.go)
- **原理**：路由分组（公开 `/health`/`/ready` + `/api/v1` 业务组 + `/metrics`），中间件链处理横切关注点
- **亮点**：
  - `requestIDMiddleware` 透传/生成 X-Request-Id（限 128 字符）贯穿日志/审计/响应
  - `corsMiddleware` 空配置不发 CORS 头（安全默认）
  - `concurrencyMiddleware` 信号量限并发（workers*4，满则 503）
  - `authMiddleware` 支持 X-API-Key/Bearer/Api-Key 三种 header，API key 失败回退 JWT
  - `requirePermission(resource, action)` / `requireAdmin()` 路由级 RBAC 中间件
  - 上传策略自适应（加密全量读/小文件缓冲/大文件流式三分支）
  - 加密下载用 `http.ServeContent` 正确处理 Content-Length 与 Range

### 4.3 上传策略自适应

- **技术**：三分支上传策略
- **位置**：[internal/api/http/server.go](../internal/api/http/server.go) `handleUpload`
- **原理**：
  - 加密路径：全量读入内存加密（AES-GCM 需全量）
  - 小文件（≤1MB）：缓冲入内存走 `UploadFile`（`bytes.NewReader` Seeker，MinIO 单次 PUT 而非 multipart 3 次往返）
  - 大文件：流式 `UploadFileFromReader` 限内存峰值
- **亮点**：防伪造 header.Size（读后判断 `len(data) > smallUploadThreshold` 报错）

### 4.4 加密下载正确处理

- **技术**：解密到临时文件 + `http.ServeContent`
- **位置**：[internal/api/http/server.go](../internal/api/http/server.go) `handleEncryptedDownload`
- **原理**：加密文件统一解密到明文临时文件，`http.ServeContent` 基于明文自动处理 Content-Length 和 Range 请求
- **解决的问题**：大文件流式下载返回密文、Range 返回密文片段、Content-Length 与实际内容大小不匹配

---

## 五、文件传输核心

### 5.1 分块上传 + 断点续传

- **技术**：预分配临时文件 + `WriteAt` 随机偏移写入 + 跨实例会话恢复
- **位置**：[internal/service/transfer/service.go](../internal/service/transfer/service.go)
- **原理**：创建会话时 `os.Create` + `file.Truncate(totalSize)` 预分配，`UploadChunk` 通过 `tempFile.WriteAt(data, offset)` 支持任意顺序到达
- **亮点**：
  - 跨实例会话恢复（内存未命中回退 Redis，重新打开临时文件句柄）
  - `LoadOrStore` 防恢复竞态（输掉竞态的 goroutine 关闭自己重开的 fd 防泄漏）
  - `SetTempDir` 支持共享卷（NFS）实现真正跨实例续传
  - 分布式锁 + 自动续约保证大文件上传期间锁不超时
  - DoS 配额（`atomic` 计数，`maxSessions=1000` 上限）

### 5.2 Multipart 分片上传

- **技术**：字节区间覆盖追踪 + 动态分片大小
- **位置**：[internal/service/transfer/multipart.go](../internal/service/transfer/multipart.go)
- **原理**：`coveredRanges []byteRange` 维护排序无重叠区间表，`addCoveredRange` 做区间合并，`isFullyCovered` 验证 `[0, TotalSize)` 无缺口
- **亮点**：`uploadedSize` 计数无法检测重复/重叠上传留下的零填充空洞，区间覆盖追踪补齐该缺陷；`suggestedPartSize` 按总大小分级（≤100MB→8MB，≤1GB→16MB，≤10GB→64MB，>10GB→128MB）

### 5.3 流式哈希增量校验

- **技术**：`sha256.New()` + `io.TeeReader` 边写边算
- **位置**：[internal/service/filemanager/manager.go](../internal/service/filemanager/manager.go) `UploadFileFromReader`、[internal/service/transfer/service.go](../internal/service/transfer/service.go) `UploadChunk`
- **原理**：`io.TeeReader` 在每次 Read 时同时写入 `hashWriter`，内存峰值仅 buffer 大小
- **亮点**：顺序到达增量更新（`lastOffset` atomic 检查），乱序到达 `hashValid=0` 失效，完成时回退全文件重算

### 5.4 并行下载

- **技术**：信号量限并发（cap=8）+ `sync.WaitGroup`
- **位置**：[internal/service/transfer/multipart.go](../internal/service/transfer/multipart.go) `ParallelDownloadChunks`

### 5.5 会话更新批量化

- **技术**：每 8 个 chunk 批量更新 Redis
- **位置**：[internal/service/transfer/service.go](../internal/service/transfer/service.go) `UploadChunk`
- **原理**：`chunkNum%8 == 0` 时才 `sessionStore.Set`，减少 87.5% 的 Redis 写入

---

## 六、一致性与服务治理

### 6.1 可插拔一致性级别

- **技术**：None / Eventual / Strong 三档 + Quorum 仲裁
- **位置**：[internal/consistency/consistency.go](../internal/consistency/consistency.go)
- **原理**：`BeforeWrite`/`BeforeRead` 按 `availableReplicas+1 >= writeQuorum/readQuorum` 判定；Strong 同步复制，Eventual 异步复制
- **亮点**：
  - 版本单调递增（`sync.RWMutex` 读写锁分离）
  - `ValidateQuorum` 强制 `readQuorum + writeQuorum > replicaCount+1`（quorum 重叠定理）
  - 副本健康检测后台循环（超 `syncInterval*3` 未同步标记不可用）

### 6.2 文件/目录管理服务

- **技术**：双锁机制（本地 `sync.Map` + 分布式 Redis）+ 批量事务删除
- **位置**：[internal/service/filemanager/manager.go](../internal/service/filemanager/manager.go)、[internal/service/directory/manager.go](../internal/service/directory/manager.go)
- **亮点**：
  - `TryLock` 安全回收锁条目防锁逃逸（删除正在使用的锁会导致后续 `LoadOrStore` 创建新 mutex，两 goroutine 持不同锁却操作同路径）
  - 目录递归删除分批事务（batchSize=500，每批 `BeginTx` 内批量 UPDATE + Commit）
  - 回滚补偿（`RenameFile` 元数据更新失败回滚存储层 rename，回滚失败记录日志）
  - 元数据复用减少 DB 查询（`DownloadFileData`/`DownloadFileDataAt` 接受已查询的 meta）

### 6.3 游标式分页避免 COUNT

- **技术**：pageSize+1 + HasMore + include_total 按需 COUNT
- **位置**：[internal/service/filelist/service.go](../internal/service/filelist/service.go)
- **原理**：默认 `fetchLimit = pageSize+1`，多查一行判断 `HasMore`，`Total=-1` 表示未计算；仅 `ListFilesWithTotal` 执行 COUNT
- **亮点**：排序字段白名单防 SQL 注入；`UNION ALL` 合并 files 和 directories 表（不去重更快）；LIKE 转义防注入

---

## 七、可观测性

### 7.1 Prometheus 指标监控

- **技术**：Counter / Histogram / Gauge 四类指标 + `/metrics` 端点
- **库**：`github.com/prometheus/client_golang`
- **位置**：[internal/metrics/metrics.go](../internal/metrics/metrics.go)
- **指标**：
  - `fsserver_http_requests_total`（CounterVec，method/path/status 标签）
  - `fsserver_http_request_duration_seconds`（HistogramVec，`DefBuckets`）
  - `fsserver_upload_size_bytes`（Histogram，`ExponentialBuckets(1024, 2, 10)` 1KB~1MB）
  - `fsserver_active_uploads`（Gauge，实时并发）
- **亮点**：门面模式封装（`RecordHTTPRequest`/`RecordUpload`/`IncActiveUploads`），gRPC 拦截器自动记录

### 7.2 zap 结构化日志

- **技术**：双核心 Tee（控制台 + JSON 文件）+ SugaredLogger
- **库**：`go.uber.org/zap`
- **位置**：[internal/logger/logger.go](../internal/logger/logger.go)
- **原理**：`zapcore.NewTee(cores...)` 聚合控制台 core（`ConsoleEncoder` stdout）+ 文件 core（`JSONEncoder` ISO8601 时间）
- **亮点**：级别动态配置（`zapLevel.UnmarshalText`）；Caller + Error 堆栈（`AddStacktrace(ErrorLevel)`）；包级函数 nil 安全

---

## 八、并发控制技术

### 8.1 Go 并发原语综合应用

| 原语 | 用途 | 典型位置 |
|------|------|---------|
| `sync.Map` | 会话表、文件锁表、预编译语句缓存、JWT 黑名单 | transfer/service.go、filemanager/manager.go、database/db.go、auth/jwt.go |
| `sync.Mutex` | 临时文件写互斥、哈希写互斥、本地锁 map | transfer/service.go、distributed/lock.go |
| `sync.RWMutex` | 版本号读写分离、缓存 LRU、DB 句柄、会话存储 | consistency.go、cache.go、database.go |
| `sync.Once` | 缓存 stopCh 单次关闭幂等 | cache.go |
| `sync.WaitGroup` | 并行下载、并发测试 | multipart.go、各 *_test.go |
| `atomic.AddInt64` | 会话计数、已传字节、chunk 计数 | transfer/service.go |
| `atomic.LoadInt32`/`StoreInt32` | closed 标志、hashValid 标志（无锁读） | transfer/service.go |
| `atomic.LoadInt64`/`StoreInt64` | lastOffset、UploadedSize 进度 | transfer/service.go |
| `channel`（信号量） | 并行下载限流（cap=8）、Watch 推送、stopCh、审计缓冲 | multipart.go、discovery.go、cache.go、audit_writer.go |
| `context.WithCancel`/`WithTimeout` | 清理线程、锁续约、etcd 操作、超时控制 | transfer/service.go、lock.go、etcd.go |
| `TryLock` | 锁条目安全回收防锁逃逸 | local.go、minio.go、filemanager/manager.go |

### 8.2 关键并发设计

- **LoadOrStore 防恢复竞态**：跨实例恢复时多 goroutine 同时重建会话副本，只有第一个胜出，其余关闭自己重开的 fd
- **TryLock 防锁逃逸**：删除 `sync.Map` 中的锁条目前先 `TryLock` 探测，避免删除正在被持有的锁
- **atomic 标志位无锁化**：`closed`/`hashValid` 用 `int32` + atomic，热路径无锁读
- **RWMutex 读写分离**：版本号、DB 句柄读多写少场景
- **sync.Once 关闭幂等**：`Cache.Stop()` 可重复调用不 panic

### 8.3 context 超时控制应用场景

- 分布式锁获取 30s 超时、释放 5s 超时、续约 5s 超时
- etcd 操作 5s 超时
- 认证操作 2s 超时
- 清理线程 `context.WithCancel` 贯穿生命周期

---

## 九、工程化与部署

### 9.1 容器化构建

- **技术**：多阶段 Docker 构建 + 静态二进制 + 非 root 用户
- **位置**：[Dockerfile](../Dockerfile)
- **亮点**：
  - Builder 阶段 `golang:1.25-alpine`，`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w"`（去路径/去 debug 信息缩体积）
  - 运行时 `alpine:3.20` + ca-certificates + tzdata，非 root `app` 用户
  - 8080（业务）+ 9090（metrics）双端口暴露
  - `/data` 数据卷挂载点

### 9.2 CI/CD

- **技术**：GitHub Actions + protoc 代码生成
- **位置**：[.github/workflows/ci.yml](../.github/workflows/ci.yml)
- **流程**：checkout → setup-go 1.25 → 安装 protoc + protoc-gen-go + protoc-gen-go-grpc → `go build` → `go vet` → `go test -short ./internal/...`
- **亮点**：重型集成/性能测试由 `FSS_BENCH_ENABLED=1` 环境变量门控，不污染 CI

### 9.3 Makefile 工程化命令

- **位置**：[Makefile](../Makefile)

| 命令 | 用途 |
|------|------|
| `make build` | 编译二进制 |
| `make test` | 全量测试（含集成测试） |
| `make test-short` | CI 短测试（跳过集成测试） |
| `make vet` | 静态检查 |
| `make lint` | Lint（依赖 vet） |
| `make clean` | 清理二进制 |

### 9.4 配置管理

- **技术**：YAML 配置文件 + 结构化默认值 + 分模块校验
- **库**：`gopkg.in/yaml.v3`
- **位置**：[internal/config/config.go](../internal/config/config.go)、[config.yaml](../config.yaml)
- **配置模块**：server、tls、storage、database、logging、cache、redis、etcd、consistency、discovery、auth、crypto（共 12 个）
- **亮点**：安全默认值（CORS 默认空、auth.enabled 默认 true）；分模块校验（storage type 枚举、minio 必填项、auth secret 必填等）

---

## 十、性能优化

### 10.1 Seeker 修复（吞吐提升 32.4 倍）

- **问题**：HTTP 写 MinIO 时 `io.TeeReader` 不实现 `io.Seeker`，MinIO SDK 强制走 multipart upload（3 次 HTTP 往返）
- **方案**：小文件（≤1MB）先全量读入内存，调用 `UploadFile` 传入 `bytes.NewReader` Seeker，让 MinIO 单次 PUT
- **效果**：HTTP 写 MinIO 吞吐 10.4 → 336.7 files/s

### 10.2 gRPC 快速路径（吞吐提升 3.73 倍）

- **方案**：单分块完整上传直接调 `FileManager.UploadFile`，绕过 transferSvc 会话机制
- **效果**：gRPC 写吞吐 631 → 2353 files/s

### 10.3 内存调优防 OOM

- **配置**：`GOMEMLIMIT=1500MiB GOGC=20`
- **原理**：minio-go 客户端库高并发下 RSS 膨胀，强制 Go 运行时在 1500MiB 上限时积极 GC（GOGC=20 表示堆增长 20% 即触发）

### 10.4 连接池调优

- DB 连接池：`PoolSize = 并发 + 32`，PostgreSQL `max_connections=300`
- HTTP 客户端：自定义 Transport，`MaxIdleConnsPerHost = 并发`
- Redis 锁：超时 30s、TTL 10s、重试间隔 50ms

### 10.5 路径分片防单目录退化

- **策略**：`/bench/<proto>/<NNN>/<NNNNNN>`，每子目录 ≤1000 条目
- **效果**：50 万文件无 `readdir` 退化

### 10.6 性能数据

| 场景 | 吞吐 |
|------|------|
| gRPC 流式上传 1GB | 409 MB/s |
| gRPC 流式下载 1GB | 1635 MB/s |
| HTTP 流式上传 1GB | 189 MB/s |
| 集中存储 vs 对象存储 | 快 6.6~7.6 倍 |

---

## 十一、测试体系

### 11.1 跨后端测试框架

- **位置**：[internal/storage/cross_backend_test.go](../internal/storage/cross_backend_test.go)
- **原理**：`runOnBothBackends` 抽象测试函数接收 `StorageAdapter` 接口，同一断言验证 Local 和 MinIO 行为一致性

### 11.2 分段 vs 非分段对比测试

- **位置**：[tests/segmented_vs_nonsegmented_test.go](../tests/segmented_vs_nonsegmented_test.go)
- **测试矩阵**：3 文件大小 × 3 并发 × 2 协议 × 2 模式 × 2 操作
- **亮点**：`CompareResult` 结构化记录；自动计算加速比；配套 24 个 Benchmark 函数

### 11.3 边界测试

- **位置**：[tests/boundary_test.go](../tests/boundary_test.go)
- **四层测试**：存储层（10 项）、FileManager 层（3 项）、Transfer 层（4 项）、路径校验（4 项）
- **关键场景**：空文件、特殊字符、深嵌套、路径穿越、hash 不匹配、offset 越界

### 11.4 多实例一致性测试

- **位置**：[tests/multi_instance_consistency_test.go](../tests/multi_instance_consistency_test.go)
- **原理**：`MultiInstanceCluster` 框架，N 实例共享 SQLite DB + LocalStorage + Redis 锁，验证跨实例并发安全

### 11.5 真实后端集成测试

- **位置**：[tests/](../tests/) 多个测试文件
- **原理**：连接真实 PostgreSQL 16 + Redis 7 + MinIO S3 兼容服务器（非 mock），每测试创建唯一 bucket

### 11.6 基准测试门控

- **环境变量**：`FSS_BENCH_ENABLED=1` 默认不运行，CI 仅跑 `go test -short`
- **存储后端选择**：`FSS_BENCH_STORAGE=local|minio`
- **规模梯度**：1K → 10K → 50 万 → 1GB

### 11.7 共享测试工具

- **位置**：[tests/testutil/testutil.go](../tests/testutil/testutil.go)
- **原理**：`NewTestServer()` 一键拉起完整服务栈，`t.Cleanup` 自动清理

---

## 十二、工具函数与路径安全

### 12.1 路径规范化与防穿越

- **位置**：[internal/utils/utils.go](../internal/utils/utils.go)
- **`NormalizePath`**：`path.Clean` 后剥离前导 `/`（MinIO 对象 key 不以 `/` 开头）
- **`IsValidFilePath`**：多层防御
  1. 拒绝空路径和根路径 `/`
  2. `path.Clean` 后拒绝 `.`/`..`/`/`
  3. 段级检查：分割原始路径，拒绝任何 `..` 段或空段（双斜杠）

### 12.2 流式哈希

- **位置**：[internal/utils/utils.go](../internal/utils/utils.go) `SHA256File`
- **原理**：256KB 缓冲区逐块读取，避免大文件全量加载内存

### 12.3 时间戳 UTC 标准化

- **位置**：[internal/utils/utils.go](../internal/utils/utils.go)
- **格式**：ISO 8601 UTC（`2006-01-02T15:04:05Z`）

---

## 十三、issue 修复技术方案

> 以下技术点来源于 [ISSUES.md](../ISSUES.md) 记录的 10 个 GitHub Issue + 4 个细节项的修复方案，以及后续验收发现的 #89-#103 系列优化。

### 13.1 分布式锁续期（#56）

- **问题**：10s TTL 锁在大文件上传期间过期，其他实例可获取锁导致并发写冲突
- **方案**：`AcquireLockWithRenewal` 后台 goroutine 每 TTL/3 续期
- **应用**：`UploadFile`/`UploadFileFromReader`/`CompleteUpload`/目录操作均使用续期锁

### 13.2 Multipart 字节区间完整性校验（#57）

- **问题**：仅靠 `uploadedSize` 计数无法检测重复/重叠上传留下的零填充间隙
- **方案**：`byteRange`/`coveredRanges` 跟踪已写字节区间，`addCoveredRange` 合并排序，`isFullyCovered` 校验 `[0, TotalSize)` 完整覆盖

### 13.3 gRPC RBAC 角色校验（#58）

- **问题**：gRPC 拦截器仅认证不授权，任何已认证用户可调用所有 RPC
- **方案**：`methodPermissions` 映射表（按 FullMethod 映射 resource/action），`authorize` 方法按角色校验

### 13.4 AES 密钥 scrypt 派生（#59）

- **问题**：直接取 passphrase 前 32 字节作为 AES-256 密钥，短密码导致弱密钥
- **方案**：scrypt（N=32768, r=8, p=1）派生 32 字节密钥

### 13.5 JWT 刷新令牌一次性轮换（#60）

- **问题**：Refresh token 未一次性轮换，被盗令牌可无限刷新
- **方案**：`jti` 黑名单 + `ValidateToken` 检查 + 后台清理

### 13.6 API Key 与 JWT 认证解耦（#61）

- **问题**：`ValidateApiKey` 回退 JWT 验证导致逻辑混乱；非 admin 可指定任意用户身份
- **方案**：移除 `ValidateApiKey` 的 JWT 回退；`authMiddleware` 增加 JWT 独立验证路径；`handleGenerateToken` 非 admin 强制真实身份

### 13.7 上传会话 DoS 配额（#63）

- **问题**：无限并发上传会话可耗尽内存/磁盘/文件描述符
- **方案**：`maxSessions`（默认 1000）+ `sessionCount`（atomic），所有终止路径（成功/错误/中止）释放槽位

### 13.8 审计日志记录真实调用者（#64）

- **问题**：`auditLog` 未从 context 获取已认证用户，审计记录缺少操作主体
- **方案**：从 gin context 读取 `*auth.User`，记录 `user:<ID>` 而非脱敏的 token

### 13.9 符号链接绕过防护（#65）

- **问题**：`ValidatePath` 未解析符号链接，可通过软链接访问 root_dir 外文件
- **方案**：`filepath.EvalSymlinks` + `resolveExistingAncestor`（解析到已存在祖先目录，处理新文件路径）

### 13.10 pathLocks 锁逃逸修复（#101）

- **问题**：`Remove`/`Rename` 在解锁后立即 `pathLocks.Delete`，若另一 goroutine 同时 `LoadOrStore` 存入新 mutex，导致两 goroutine 持不同锁却操作同路径
- **方案**：移除热路径 `pathLocks.Delete`；`CleanPathLocks` 改用 `TryLock` 探测无占用再删除

### 13.11 transfer 资源泄漏与数据竞争修复（#100）

- **问题**：`LoadOrStore` 竞态失败方未关闭多余 temp file 句柄；错误路径未释放 session slot；`tempFile` 访问未加锁
- **方案**：竞态失败方关闭 fd；补全所有错误路径的 `releaseSessionSlot` + `Delete` + 临时文件清理；`tempFile` 访问加 `tempFileMu` 锁

### 13.12 加密下载路径修复（#99）

- **问题**：大文件流式下载返回密文、Range 返回密文片段、Content-Length 与实际内容不匹配
- **方案**：加密文件统一走 `handleEncryptedDownload`，解密到临时文件后用 `http.ServeContent` 响应

### 13.13 列表接口性能优化（#89-#93）

- **#89 审计日志异步批写**：消除请求路径同步 DB 写
- **#90 LRU O(1) 双向链表**：淘汰从 O(N) map 扫描改为 O(1) 双向链表
- **#91 列表跳过 COUNT**：默认 `include_total=false`，新增 `HasMore` 支持无 total 分页
- **#92 MinIO WriteAt 拒绝**：明确拒绝整文件读改写的 OOM/非原子风险
- **#93 下载路径复用 meta**：消除冗余 `GetFileMetadata` 查询

### 13.14 鉴权越权与 SQL 注入修复（#95-#98）

- **#95**：admin 权限精确匹配、gRPC 错误屏蔽、JWT token 类型校验、API key 权限校验
- **#96**：SQL LIKE 通配符转义 + `rows.Err()` 错误传播
- **#97**：gRPC 输入校验防 DoS（TotalSize/chunkSize/pageSize 上限）
- **#98**：gRPC JWT 用户授权（`GetUserByApiKey` 支持 JWT）

### 13.15 资源管理小问题（#103）

- `cache.Stop()` 加 `sync.Once` 防重复 close channel panic
- `io.Copy` 返回值检查并记录日志
- `RenameFile` 回滚失败记录日志避免静默丢失文件

---

## 依赖总览

| 类别 | 依赖 | 版本 |
|------|------|------|
| Web 框架 | gin | v1.12.0 |
| RPC | grpc + protobuf | v1.81.1 / v1.36.11 |
| 数据库 | lib/pq + modernc.org/sqlite | v1.12.3 / v1.50.1 |
| 对象存储 | minio-go/v7 | v7.2.0 |
| 缓存/锁 | redis/go-redis/v9 | v9.20.0 |
| 协调 | etcd/client/v3 | v3.6.12 |
| 安全 | golang-jwt/v5 + golang.org/x/crypto | v5.3.1 / v0.51.0 |
| 监控 | prometheus/client_golang | v1.23.2 |
| 日志 | zap | v1.27.0 |
| 配置 | yaml.v3 | v3.0.1 |
| 测试 | gofakes3（S3 模拟） | v0.0.0-20260208201424 |
| UUID | google/uuid | v1.6.0 |

---

## 技术亮点总结

1. **统一 Adapter 接口**贯穿存储/缓存/锁/会话四层，后端切换是配置项而非代码改动
2. **方言翻译层**让同一套 SQL 跑在 SQLite 和 PostgreSQL 上，保留预处理语句缓存
3. **分块/断点续传 + Multipart 字节覆盖追踪**的完整传输实现
4. **锁逃逸/恢复竞态**等并发边界场景的针对性设计（TryLock 回收、LoadOrStore 竞态处理）
5. **异步批写审计日志**通过 channel + 后台 goroutine 解耦持久化与请求路径
6. **分布式锁 Lua 脚本原子性** + 退避抖动重试 + 自动续约
7. **小文件快速路径**等性能优化有详尽注释说明权衡依据
8. **路径穿越防护、LIKE 注入转义、JWT alg 校验、refresh token 黑名单、临时文件路径校验**等多处安全细节
9. **多维基准测试**覆盖 1K~50 万文件 + 1GB 大文件，p50/p95/p99 延迟分布
10. **多实例一致性测试框架**验证跨实例并发安全
