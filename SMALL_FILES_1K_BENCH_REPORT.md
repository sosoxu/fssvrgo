# 1000 小文件性能测试报告

**生成时间：** 2026-07-01T07:15:03Z
**测试对象：** fssvrgo 分布式文件存储服务
**测试范围：** 集中存储（LocalStorage）· 写入 1000 个 1KB 文件后回读 · HTTP 与 gRPC 双协议覆盖
**代码版本：** main `a29210d`（含 gRPC 小文件上传快速路径优化）

> 结论速览：4 个阶段共 **4,000 次操作全部成功，0 错误**，总耗时 1.81s。
> - **写入：gRPC 比 HTTP 快 1.96×**（2455 vs 1254 files/s）；
> - **读取：gRPC 比 HTTP 快 1.08×**（5293 vs 4899 files/s），两者基本持平；
> - gRPC 在读写两端均略胜或持平，写入场景优势最明显。

---

## 1. 测试目标

按要求对当前项目进行小规模性能压测：

- 写入 **1000 个 1KB 大小**的文件，随后读回这些文件；
- **只测试集中存储**（本地文件系统 LocalStorage，不测 MinIO 分布式对象存储）；
- **使用 Redis 作为一致性锁**（分布式 `SET NX` 加锁 + Lua 原子解锁）；
- **使用 PostgreSQL 作为元数据数据库**；
- **HTTP 与 RPC（gRPC over TCP）两种方式都要覆盖**——必须是真实的 gRPC TCP 调用；
- 出具本测试报告。

## 2. 测试环境

| 项目 | 配置 |
|------|------|
| 操作系统 | Ubuntu 24.04.3 LTS (Noble Numbat) |
| 内核架构 | linux/amd64 |
| CPU | INTEL(R) XEON(R) PLATINUM 8582C |
| Go 运行可用核数 (GOMAXPROCS) | 2 |
| 内存 | 5.8 GiB |
| 磁盘 | 1.5 TB |
| Go 版本 | go1.25.1 |
| 数据库 | PostgreSQL 16.14（`max_connections=300`，连接池 `PoolSize=并发+32`） |
| 一致性锁 | Redis 7.0.15（`SET NX` + Lua 原子解锁） |
| 存储后端 | LocalStorage（集中式本地文件系统） |
| 认证 | 已禁用（`auth.Init(false, "")`），排除认证噪声 |

## 3. 测试配置

| 参数 | 值 | 说明 |
|------|----|------|
| 文件数量 | 1,000 | 每阶段 1000 |
| 单文件大小 | 1024 B (1 KB) | 固定填充内容 |
| HTTP 并发 | 12 | 与 gRPC 持平，确保公平对比 |
| gRPC 并发 | 12 | 与 HTTP 持平，确保公平对比 |
| 路径分片 | `/bench/<proto>/<NNN>/<NNNNNN>` | 每子目录 ≤1000 条目 |
| HTTP 客户端 | 自定义 Transport，连接池复用 | `MaxIdleConnsPerHost=并发` |
| gRPC 客户端 | `grpc.NewClient` + 真实 TCP 监听 | Client Streaming 上传 / Server Streaming 下载 |
| 数据库连接池 | `PoolSize = 并发 + 32` | 避免 PG 连接打满 |
| Redis | 每次写/删均走分布式锁 | `AcquireLock` 超时 30s、TTL 10s、重试 50ms |
| 总超时 | 10 min | `go test -timeout 10m` |

## 4. 测试结果总览

四个阶段：HTTP 写 → HTTP 读 → gRPC 写 → gRPC 读，串行执行，每阶段独立清理并重建 1000 文件。

### 4.1 汇总表

| 阶段 | 协议 | 操作 | 文件数 | 耗时 | 错误 | files/s | MB/s | 延迟 p50/p95/p99 (μs) |
|------|------|------|-------|------|------|---------|------|----------------------|
| HTTP Write | HTTP | 写 | 1,000 | 0.80 s | 0 | **1253.8** | 1.22 | 7461 / 11767 / 95792 |
| HTTP Read | HTTP | 读 | 1,000 | 0.20 s | 0 | **4898.7** | 4.78 | 2062 / 4618 / 10731 |
| gRPC Write | gRPC | 写 | 1,000 | 0.41 s | 0 | **2455.3** | 2.40 | 4307 / 6285 / 39537 |
| gRPC Read | gRPC | 读 | 1,000 | 0.19 s | 0 | **5293.1** | 5.17 | 1963 / 4207 / 10489 |

- **总操作数：** 4,000（写 2,000 + 读 2,000）
- **总错误数：** 0
- **总耗时：** 1.81 s
- **测试结论：** `PASS`

### 4.2 完整延迟分布（来自 JSON 结果）

| 阶段 | min (μs) | avg (μs) | max (μs) | p50 (μs) | p95 (μs) | p99 (μs) | 样本 |
|------|----------|----------|----------|----------|----------|----------|------|
| HTTP Write | 2785 | 9531 | 169595 | 7461 | 11767 | 95792 | 1000 |
| HTTP Read | 299 | 2418 | 18719 | 2062 | 4618 | 10731 | 1000 |
| gRPC Write | 1465 | 4859 | 52641 | 4307 | 6285 | 39537 | 1000 |
| gRPC Read | 355 | 2257 | 14277 | 1963 | 4207 | 10489 | 1000 |

## 5. 对比分析

### 5.1 HTTP vs gRPC

| 指标 | HTTP | gRPC | 倍率 |
|------|------|------|------|
| 写吞吐 (files/s) | 1253.8 | 2455.3 | **gRPC 快 1.96×** |
| 读吞吐 (files/s) | 4898.7 | 5293.1 | gRPC 快 1.08×（基本持平） |
| 写 p50 (μs) | 7461 | 4307 | **gRPC 低 42%** |
| 写 p95 (μs) | 11767 | 6285 | **gRPC 低 47%** |
| 写 p99 (μs) | 95792 | 39537 | **gRPC 低 59%** |
| 读 p50 (μs) | 2062 | 1963 | gRPC 低 5% |
| 读 p95 (μs) | 4618 | 4207 | gRPC 低 9% |
| 读 p99 (μs) | 10731 | 10489 | gRPC 低 2% |

**分析：**

- **写入：gRPC 全面胜出（吞吐 1.96×，尾部延迟低 47~59%）。** gRPC 快速路径对单分块完整上传直接调用 `FileManager.UploadFile`，绕过会话机制。gRPC 凭 HTTP/2 长连接 + 多路复用，单次操作基准成本（p50=4307μs）远低于 HTTP multipart（p50=7461μs），尾部延迟更稳（写 p99 39537μs vs HTTP 95792μs）。
- **读取：两者基本持平，gRPC 略快 1.08×。** 两者读路径相同（均直接调 `fm.DownloadFileAt`），差异来自传输层。小规模下连接池预热差异不显著，gRPC 长连接略占优。gRPC 读延迟分布也更优（p50/p95/p99 均略低）。

### 5.2 读 vs 写

| 协议 | 写 (files/s) | 读 (files/s) | 读/写倍率 |
|------|-------------|-------------|----------|
| HTTP | 1253.8 | 4898.7 | **3.91×** |
| gRPC | 2455.3 | 5293.1 | **2.16×** |

**分析：读取仍快于写入，写入是瓶颈。**

- **写路径**（`FileManager.UploadFile`）：分布式锁获取/释放（Redis 往返）→ `Exists` 检查 → `storage.Write`（路径锁 + `os.WriteFile` + 目录创建）→ SHA256 全量计算 → PG 元数据 `INSERT`。每个文件至少 1 次 Redis 往返 + 1 次 PG 写 + 1 次磁盘写。
- **读路径**（`FileManager.DownloadFile`）：`GetFileMetadata`（PG 单行查询，命中索引）→ `storage.Read`（`os.ReadFile`）。无锁、无哈希、无写盘，OS 页缓存命中率高。
- gRPC 写优化后吞吐翻倍（2455 vs HTTP 1254），使读/写倍率收窄至 2.16×；HTTP 读/写倍率 3.91×，写瓶颈更明显。

## 6. 关键发现

1. **零错误：** 4,000 次操作全部成功，PostgreSQL + Redis 锁 + 本地存储组合在小规模下稳定可靠。
2. **gRPC 写入优势明显：** 吞吐 1.96×、p99 低 59%，得益于快速路径绕过会话机制 + HTTP/2 长连接复用。
3. **gRPC 读取略胜：** 小规模下两者基本持平（gRPC 快 1.08×），gRPC 延迟分布略优。
4. **写入是瓶颈：** 受 Redis 锁往返 + SHA256 + PG 插入 + 磁盘写制约，写吞吐 1254~2455 files/s；读吞吐达 4899~5293 files/s。
5. **小规模尾部延迟波动大：** 1000 样本下 p99/max 受偶发毛刺影响明显（HTTP 写 p99=95792μs 但 max=169595μs），统计意义弱于大样本（50 万规模）。建议关注 p50/p95 而非 p99。

## 7. 复现方式

测试代码：[tests/massive_small_files_test.go](file:///workspace/tests/massive_small_files_test.go)
原始结果：[small_files_1k_results.json](file:///workspace/small_files_1k_results.json)

前置：启动 PostgreSQL 16 与 Redis 7，创建 `fsserver` 库/用户（密码 `fsserver123`），`max_connections≥300`。

```bash
FSS_BENCH_ENABLED=1 \
FSS_BENCH_COUNT=1000 \
FSS_BENCH_HTTP_CONCURRENCY=12 \
FSS_BENCH_GRPC_CONCURRENCY=12 \
FSS_BENCH_OUT=/workspace/small_files_1k_results.json \
go test -run 'TestMassiveSmallFiles_Performance' -count=1 -v -timeout 10m ./tests/
```

> 测试由 `FSS_BENCH_ENABLED=1` 环境变量门控，默认不运行，不会污染 CI。

## 8. 测试方法学说明

- **真实双协议：** HTTP 走 Gin REST API（`POST /api/v1/files` 上传、`GET /api/v1/files/*path` 下载）；gRPC 走真实 TCP 监听的 `FileService`（`UploadFile` 客户端流式、`DownloadFile` 服务端流式），客户端用 `grpc.NewClient` 建立真实连接，非进程内调用。
- **公平并发：** HTTP 与 gRPC 均使用并发 12，消除并发差异对协议对比的干扰。
- **共享同一后端：** 四阶段共用同一套 PostgreSQL + Redis + LocalStorage，确保对比变量唯一。
- **逐操作计时：** 记录每一次操作的端到端延迟（含网络 + 锁 + DB + 磁盘），统计 min/avg/max/p50/p95/p99。
- **隔离清理：** 每阶段开始前清空 DB 表、Flush Redis、删除存储目录并清理 `pathLocks`，保证阶段间互不干扰。
