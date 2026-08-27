# fssvrgo 需求文档

## 1. 项目概述

fssvrgo 是一个文件存储服务，支持 HTTP 和 gRPC 双协议访问、大文件分段传输以及基于共享数据库、共享存储和 Redis 协调的多实例部署。

## 2. 核心需求

### 2.1 双协议支持

- 提供 RESTful HTTP API（基于 Gin 框架）
- 提供 gRPC 服务（基于 Protocol Buffers 定义）
- 两种协议共享同一服务层，确保行为一致性

### 2.2 文件操作

| 操作 | HTTP 方法 | HTTP 路径 | gRPC 方法 |
|------|-----------|-----------|-----------|
| 整体上传 | POST | `/api/v1/files` | `UploadFile` (stream) |
| 下载文件 | GET | `/api/v1/files/*path` | `DownloadFile` (stream) |
| 删除文件 | DELETE | `/api/v1/files/*path` | `DeleteFile` |
| 重命名文件 | PATCH | `/api/v1/files/*path` | `RenameFile` |
| 列出文件 | GET | `/api/v1/files` | `ListFiles` |
| 获取元数据 | GET | `/api/v1/metadata/*path` | `GetMetadata` |
| 创建目录 | POST | `/api/v1/directories` | `CreateDirectory` |

### 2.3 流式传输

#### 2.3.1 顺序上传

| 操作 | HTTP 方法 | HTTP 路径 |
|------|-----------|-----------|
| 创建上传会话 | POST | `/api/v1/uploads` |
| 上传数据块 | PUT | `/api/v1/uploads/:id/chunk` |
| 查询进度 | GET | `/api/v1/uploads/:id/progress` |
| 完成上传 | POST | `/api/v1/uploads/:id/complete` |
| 取消上传 | DELETE | `/api/v1/uploads/:id` |

- 数据块 `offset` 必须等于服务端已确认进度，不允许跳跃或重叠写入
- 每个成功数据块的数据必须先 `fsync`，随后在租约仍有效时写入会话存储；任一步失败均不得返回成功
- 使用共享 `storage.temp_dir` 和 Redis 时，新实例可恢复会话并继续上传
- 进度查询必须从共享会话状态读取；过期清理按共享 `updated_at` 判断，并与上传使用同一会话锁

#### 2.3.2 分段并发上传

| 操作 | HTTP 方法 | HTTP 路径 |
|------|-----------|-----------|
| 创建分段上传会话 | POST | `/api/v1/multipart-uploads` |
| 上传分段数据 | PUT | `/api/v1/multipart-uploads/:id/parts/:partNumber` |
| 查询分段进度 | GET | `/api/v1/multipart-uploads/:id` |
| 完成分段上传 | POST | `/api/v1/multipart-uploads/:id/complete` |
| 取消分段上传 | DELETE | `/api/v1/multipart-uploads/:id` |

- 分片编号、偏移、大小、哈希和覆盖区间必须持久化
- 不同分片不得重叠；相同分片仅允许以相同偏移和大小重试
- 完成前必须确认 `[0, total_size)` 无空洞，并支持在另一实例恢复、完成或取消

#### 2.3.3 Range 下载

- 支持 HTTP Range 请求头，实现分段下载
- 支持指定起始和结束字节位置
- 单次 Range 请求最大 32MB

### 2.4 数据存储

- **本地文件系统**：默认存储方式，使用 `sync.Map` 实现细粒度路径锁
- **MinIO 对象存储**：支持 S3 兼容的对象存储后端

### 2.5 数据库支持

- **PostgreSQL**：所有服务部署（包括单机）必须使用 PostgreSQL，使用 `lib/pq` 驱动；启动配置必须拒绝 SQLite
- **SQLite**：仅允许用于隔离功能测试和 SQL 方言兼容测试，不作为运行、多实例一致性或性能验收数据库
- SQL 方言自动翻译（`?` → `$1, $2, ...`）
- Prepared Statement 缓存减少 SQL 解析开销

### 2.6 分布式支持

- **Redis 分布式锁**：SET NX + Lua 原子解锁，保证跨实例互斥
- **Redis 会话存储**：上传会话和 Multipart 状态存储在 Redis；恢复上传还要求所有实例使用同一个共享 `storage.temp_dir`
- **指数退避重试**：锁获取采用指数退避 + 随机抖动策略
- **进度确认语义**：每个成功上传块均已持久化，Redis 写入失败时客户端必须重试
- **重命名互斥**：文件和本地目录重命名必须按固定顺序同时锁定源、目标路径；数据库查询失败不得按“不存在”继续
- **层级命名空间互斥**：文件读写对所有祖先目录持共享租约，目录创建、删除和重命名对目标子树根持独占租约；活跃流式下载期间父目录变更必须等待，同一目录下不同文件仍允许并行
- **下载会话接管**：下载会话在另一实例恢复时必须复用原命名空间租约所有权；完成或中止必须等待正在读取的分块，并使所有实例立即拒绝后续读取
- **加密会话恢复**：加密状态必须随下载会话持久化；接管实例必须使用相同密钥重建明文读取源，缺少密钥时明确失败，禁止返回存储密文
- **上传终态幂等**：顺序和分段上传的完成/取消终态必须持久化到 PostgreSQL；完成结果与文件元数据在同一事务提交，Redis 缓存丢失或响应丢失后跨实例重试必须返回相同结果
- **递归删除原子性**：同一次递归删除涉及的根目录、子目录和文件元数据必须在一个数据库事务内变更；任一语句失败不得保留已删除的前置批次
- **租约时钟**：层级租约过期和续期必须使用 PostgreSQL 时钟，不能依赖各实例本地时钟；提交元数据前必须确认租约仍有效
- **最终写 fencing**：PostgreSQL 必须为 namespace lease 分配单调 token，并持久化每个最终写路径的最高 token；文件/目录最终事务必须持有 lease 的全部层级 advisory locks，存储变更和元数据提交必须位于 exact-path transaction lock 内，低于已提交 head 的 token 即使 holder 尚未过期也必须拒绝
- **一致性级别限制**：`eventual` 和 `strong` 尚无真实复制链路，配置校验必须拒绝；当前仅支持 `none`

### 2.7 安全

- **API Key 认证**：支持 `X-API-Key` 头和 `Authorization: Bearer` 头
- **JWT 认证**：支持配置化 access/refresh 有效期和 refresh token 一次性消费
- **速率限制**：基于 IP 的认证失败计数，Redis 启用时跨实例共享，超过阈值后临时封禁
- **文件加密**：采用版本化、分块 AES-256-GCM 文件格式，上传和下载内存占用有界；兼容读取旧 Base64 格式
- **路径安全**：防止路径遍历攻击（`..` 检测）

### 2.8 可观测性

- **结构化日志**：基于 Zap 的 JSON 格式日志
- **Prometheus 指标**：HTTP 和 gRPC 分别记录请求总数与延迟；成功提交后记录上传大小；活跃上传表示当前正在处理数据的上传请求数，不表示跨请求会话总数
- **审计日志**：HTTP 变更类成功操作和全部失败响应、gRPC 成功与失败请求均异步记录；正常关闭必须停止接收新记录、排空已接受队列并在截止时间内刷新。HTTP 读取类成功请求及进程崩溃下的可靠投递尚未完成

### 2.9 配置管理

- YAML 格式配置文件
- 配置项验证和默认值填充
- 支持的配置模块：server、tls、storage、database、logging、cache、redis、etcd、consistency、discovery、auth、crypto

## 3. 性能需求

### 3.1 目标性能指标

下表是验收目标，不是对任意机器的无条件保证。验收报告必须同时记录 CPU、内存、磁盘/网络、Go 版本、文件大小、并发数、HTTP/gRPC、存储与数据库后端、Redis 状态、加密状态、预热次数、样本数，以及 p50/p95/p99 和错误率。缺少这些信息的历史数据仅作参考，不得用于发布门禁。

| 场景 | 目标 |
|------|------|
| HTTP 单次上传 1GB | ≤ 25s |
| HTTP 流式上传 1GB | ≤ 8s |
| gRPC 流式上传 1GB | ≤ 4s |
| HTTP 单次下载 1GB | ≤ 1s |
| gRPC 流式下载 1GB | ≤ 1s |
| 并发上传 (10 并发) | 稳定无错误 |

发布门禁使用固定环境连续运行至少 3 轮，错误率必须为 0；时间目标以 3 轮 p95 判定。当前仓库 benchmark 可用于回归比较，但尚未提供满足上述条件的自动化 1GB 性能门禁，因此本项状态为“待验收”。

### 3.2 分段大小建议

| 文件大小 | 建议分段大小 |
|---------|------------|
| ≤ 100MB | 8MB |
| 100MB ~ 1GB | 16MB |
| 1GB ~ 10GB | 64MB |
| > 10GB | 128MB |

### 3.3 关键优化

- 增量哈希校验：顺序上传时边写边计算 SHA256
- 文件句柄缓存：上传会话保持临时文件打开
- 预分配空间：创建会话时 Truncate，减少文件系统碎片
- 细粒度路径锁：sync.Map 按路径加锁
- 无锁进度追踪：atomic.AddInt64 实现进度更新

## 4. 部署需求

### 4.1 单机部署

- PostgreSQL + 本地文件系统
- 单机与多实例使用相同数据库语义，避免因 SQLite 单写者模型得到失真的并发结论

### 4.2 多实例部署

- PostgreSQL + Redis + 共享存储（NFS/SSD）
- 分布式锁保证跨实例互斥
- 会话状态存储在 Redis，共享 `storage.temp_dir` 保存上传临时数据
- Redis 文件锁用于快速互斥，PostgreSQL 层级租约和持久化 fence head 阻止旧 token 再次提交。本地文件系统不是原生 token-aware 存储且没有跨资源事务，仍不能宣称任意网络分区/进程故障下的全局线性一致性

## 5. 待实现需求

以下需求已有配置定义或部分代码实现，但尚未完整落地：

| 需求 | 状态 | Issue |
|------|------|-------|
| 数据一致性级别控制 | `eventual`/`strong` 未接入复制链路，配置已显式拒绝 | R-03 |
| 审计完整性与可靠投递 | HTTP 读取类成功请求未完整覆盖；异步缓冲满或数据库故障时仍可能丢失 | R-09 |
| 存储原生 fencing / 跨资源事务 | PostgreSQL 元数据提交 fence 已实现；本地文件系统和 MinIO 不原生校验 token，仍无跨数据库与存储事务 | R-06 |
| 1GB 性能发布门禁 | 有 benchmark 和历史数据，尚无固定环境自动门禁报告 | R-11 |
| MinIO 能力对齐 | 本轮无 MinIO 环境，目录能力和补偿机制待真实后端验证 | R-13 |

其余历史条目（TLS 启动、etcd 注册/发现、JWT、API Key 管理、HTTP 审计查询、目录 HTTP API、gRPC 认证与指标、分块文件加密、缓存清理、软删除清理和程序入口）已有实现。它们是否满足生产要求仍以对应测试和上述限制为准。
