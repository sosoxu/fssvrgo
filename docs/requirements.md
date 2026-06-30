# fssvrgo 需求文档

## 1. 项目概述

fssvrgo 是一个高性能分布式文件存储服务，支持 HTTP 和 gRPC 双协议访问，提供大文件分段并发传输、多实例部署数据一致性保障。

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

#### 2.3.2 分段并发上传

| 操作 | HTTP 方法 | HTTP 路径 |
|------|-----------|-----------|
| 创建分段上传会话 | POST | `/api/v1/multipart-uploads` |
| 上传分段数据 | PUT | `/api/v1/multipart-uploads/:id/parts/:partNumber` |
| 查询分段进度 | GET | `/api/v1/multipart-uploads/:id` |
| 完成分段上传 | POST | `/api/v1/multipart-uploads/:id/complete` |
| 取消分段上传 | DELETE | `/api/v1/multipart-uploads/:id` |

#### 2.3.3 Range 下载

- 支持 HTTP Range 请求头，实现分段下载
- 支持指定起始和结束字节位置
- 单次 Range 请求最大 32MB

### 2.4 数据存储

- **本地文件系统**：默认存储方式，使用 `sync.Map` 实现细粒度路径锁
- **MinIO 对象存储**：支持 S3 兼容的对象存储后端

### 2.5 数据库支持

- **SQLite**：单机部署，使用 `modernc.org/sqlite`（纯 Go 实现）
- **PostgreSQL**：生产环境，使用 `lib/pq` 驱动
- SQL 方言自动翻译（`?` → `$1, $2, ...`）
- Prepared Statement 缓存减少 SQL 解析开销

### 2.6 分布式支持

- **Redis 分布式锁**：SET NX + Lua 原子解锁，保证跨实例互斥
- **Redis 会话存储**：上传/下载会话存储在 Redis，任意实例可恢复
- **指数退避重试**：锁获取采用指数退避 + 随机抖动策略
- **会话更新批量化**：每 8 个 chunk 更新一次，减少 87.5% 的 Redis 写入

### 2.7 安全

- **API Key 认证**：支持 `X-API-Key` 头和 `Authorization: Bearer` 头
- **速率限制**：基于 IP 的认证失败计数，超过阈值后临时封禁
- **文件加密**：AES-256-GCM 加密存储
- **路径安全**：防止路径遍历攻击（`..` 检测）

### 2.8 可观测性

- **结构化日志**：基于 Zap 的 JSON 格式日志
- **Prometheus 指标**：HTTP 请求总数、请求延迟、上传大小、活跃上传数
- **审计日志**：记录文件操作的操作类型、资源路径、用户标识、客户端 IP

### 2.9 配置管理

- YAML 格式配置文件
- 配置项验证和默认值填充
- 支持的配置模块：server、tls、storage、database、logging、cache、redis、etcd、consistency、discovery、auth、crypto

## 3. 性能需求

### 3.1 目标性能指标

| 场景 | 目标 |
|------|------|
| HTTP 单次上传 1GB | ≤ 25s |
| HTTP 流式上传 1GB | ≤ 8s |
| gRPC 流式上传 1GB | ≤ 4s |
| HTTP 单次下载 1GB | ≤ 1s |
| gRPC 流式下载 1GB | ≤ 1s |
| 并发上传 (10 并发) | 稳定无错误 |

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

- SQLite + 本地文件系统
- 最小依赖，开箱即用

### 4.2 多实例部署

- PostgreSQL + Redis + 共享存储（NFS/SSD）
- 分布式锁保证跨实例互斥
- 会话存储在 Redis 保证任意实例可恢复

## 5. 待实现需求

以下需求已有配置定义或部分代码实现，但尚未完整落地：

| 需求 | 状态 | Issue |
|------|------|-------|
| HTTPS/TLS 双端口监听 | 配置已定义，未实现 | #2 |
| etcd 集成 | 配置已定义，未实现 | #3 |
| 服务发现机制 | 配置已定义，未实现 | #4 |
| 数据一致性级别控制 | 配置已定义，未实现 | #5 |
| JWT Token 认证 | 配置已定义，未实现 | #6 |
| API Key 管理 API | 数据库已建表，无端点 | #7 |
| 审计日志持久化和查询 | 数据库已建表，未持久化 | #8 |
| 目录删除和重命名 HTTP API | 服务层已实现，无端点 | #9 |
| gRPC 认证拦截器 | 未实现 | #10 |
| gRPC 指标采集 | 未实现 | #11 |
| 流式上传/下载加密 | 未实现 | #12 |
| 缓存自动清理和 Redis 后端 | 部分实现 | #13 |
| 软删除数据清理机制 | 未实现 | #14 |
| 高并发稳定性修复 | 部分失败 | #15 |
| 程序入口 main.go | 未创建 | #16 |
