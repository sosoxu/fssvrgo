# 1000 小文件 MinIO 对象存储性能测试报告

**生成时间：** 2026-07-01T08:03:29Z
**测试对象：** fssvrgo 分布式文件存储服务
**测试范围：** MinIO 对象存储 · 写入 1000 个 1KB 文件后回读 · HTTP 与 gRPC 双协议覆盖
**代码版本：** main（含 gRPC 小文件上传快速路径优化）

> 结论速览：4 个阶段共 **4,000 次操作全部成功，0 错误**。
> - **写入：gRPC 比 HTTP 快 47.07×**（488 vs 10.4 files/s）——HTTP 走 `WriteFromReader`（非 Seeker → MinIO 触发 multipart），gRPC 走快速路径 `Write`（Seeker → 直接 PUT）；
> - **读取：HTTP 比 gRPC 快 1.28×**（1566 vs 1224 files/s）——两者读路径相同；
> - **HTTP 写入是严重瓶颈**：p50=360ms，单文件延迟极高，根因为 MinIO SDK 对非 Seeker 的 `io.Reader` 走 multipart upload 路径。

---

## 1. 测试目标

按要求对当前项目进行 MinIO 对象存储性能压测：

- 写入 **1000 个 1KB 大小**的文件，随后读回这些文件；
- **测试对象存储 MinIO**（非集中式本地存储）；
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
| 内存 | 5.8 GiB（cgroup 限制） |
| 磁盘 | 1.5 TB |
| Go 版本 | go1.25.1 |
| 数据库 | PostgreSQL 16.14（`max_connections=300`，连接池 `PoolSize=并发+32`） |
| 一致性锁 | Redis 7.0.15（`SET NX` + Lua 原子解锁） |
| **存储后端** | **MinIO 对象存储**（S3 兼容，单节点，本地 9000 端口） |
| MinIO 版本 | DEVELOPMENT.GOGET (go1.25.1, source build) |
| 认证 | 已禁用（`auth.Init(false, "")`），排除认证噪声 |

> **内存限制说明：** MinIO 客户端库（minio-go v7）在并发上传时内存开销显著高于本地存储。c=12 时测试进程 OOM（RSS 3.8GB），故降至 c=4 并设置 `GOMEMLIMIT=1500MiB GOGC=20` 强制 GC。这一限制本身也是 MinIO 后端相对于本地存储的一个客观差异。

## 3. 测试配置

| 参数 | 值 | 说明 |
|------|----|------|
| 文件数量 | 1,000 | 每阶段 1000 |
| 单文件大小 | 1024 B (1 KB) | 固定填充内容 |
| HTTP 并发 | 4 | 受 MinIO 内存开销限制，从 12 降至 4 |
| gRPC 并发 | 4 | 同上，与 HTTP 持平确保公平对比 |
| 路径分片 | `/bench/<proto>/<NNN>/<NNNNNN>` | 每子目录 ≤1000 条目 |
| HTTP 客户端 | 自定义 Transport，连接池复用 | `MaxIdleConnsPerHost=并发*2` |
| gRPC 客户端 | `grpc.NewClient` + 真实 TCP 监听 | Client Streaming 上传 / Server Streaming 下载 |
| MinIO 配置 | `localhost:9000`，`minioadmin/minioadmin` | 每次测试创建唯一 bucket `fssbench-<timestamp>` |
| 数据库连接池 | `PoolSize = 并发 + 32` | 避免 PG 连接打满 |
| Redis | 每次写/删均走分布式锁 | `AcquireLock` 超时 30s、TTL 10s、重试 50ms |
| GC 调优 | `GOMEMLIMIT=1500MiB`，`GOGC=20` | 防止 MinIO 客户端内存膨胀触发 OOM |
| 总超时 | 10 min | `go test -timeout 10m` |

## 4. 测试结果总览

四个阶段：HTTP 写 → HTTP 读 → gRPC 写 → gRPC 读，串行执行，每阶段独立清理并重建 1000 文件。

### 4.1 汇总表

| 阶段 | 协议 | 操作 | 文件数 | 耗时 | 错误 | files/s | MB/s | 延迟 p50/p95/p99 (μs) |
|------|------|------|-------|------|------|---------|------|----------------------|
| HTTP Write | HTTP | 写 | 1,000 | 96.36 s | 0 | **10.4** | 0.01 | 360120 / 686124 / 769512 |
| HTTP Read | HTTP | 读 | 1,000 | 0.64 s | 0 | **1565.5** | 1.53 | 2313 / 3781 / 9498 |
| gRPC Write | gRPC | 写 | 1,000 | 2.05 s | 0 | **488.4** | 0.48 | 7772 / 10899 / 16343 |
| gRPC Read | gRPC | 读 | 1,000 | 0.82 s | 0 | **1224.1** | 1.20 | 2226 / 5921 / 25271 |

- **总操作数：** 4,000（写 2,000 + 读 2,000）
- **总错误数：** 0
- **总耗时：** 100.52 s
- **测试结论：** `PASS`

### 4.2 完整延迟分布（来自 JSON 结果）

| 阶段 | min (μs) | avg (μs) | max (μs) | p50 (μs) | p95 (μs) | p99 (μs) | 样本 |
|------|----------|----------|----------|----------|----------|----------|------|
| HTTP Write | 41013 | 384904 | 868907 | 360120 | 686124 | 769512 | 1000 |
| HTTP Read | 1140 | 2550 | 18089 | 2313 | 3781 | 9498 | 1000 |
| gRPC Write | 4537 | 8182 | 40891 | 7772 | 10899 | 16343 | 1000 |
| gRPC Read | 854 | 3264 | 79734 | 2226 | 5921 | 25271 | 1000 |

## 5. 对比分析

### 5.1 HTTP vs gRPC

| 指标 | HTTP | gRPC | 倍率 |
|------|------|------|------|
| 写吞吐 (files/s) | 10.4 | 488.4 | **gRPC 快 47.07×** |
| 读吞吐 (files/s) | 1565.5 | 1224.1 | HTTP 快 1.28× |
| 写 p50 (μs) | 360120 | 7772 | **gRPC 低 97.8%** |
| 写 p95 (μs) | 686124 | 10899 | **gRPC 低 98.4%** |
| 写 p99 (μs) | 769512 | 16343 | **gRPC 低 97.9%** |
| 读 p50 (μs) | 2313 | 2226 | gRPC 低 4% |
| 读 p95 (μs) | 3781 | 5921 | HTTP 低 36% |
| 读 p99 (μs) | 9498 | 25271 | HTTP 低 62% |

**分析：**

- **写入：gRPC 以 47× 的优势碾压 HTTP。** 根因在于两条写路径对 MinIO SDK 的调用方式不同：
  - **gRPC 快速路径**（`fm.UploadFile` → `storage.Write`）：使用 `bytes.NewReader(data)` 构造 `io.Reader`，这是一个 `io.Seeker`。MinIO SDK 检测到 Seeker 后可直接用 `Content-Length` 发起单次 HTTP PUT，1KB 文件一次请求完成。
  - **HTTP 路径**（`fm.UploadFileFromReader` → `storage.WriteFromReader`）：使用 `io.TeeReader(reader, hashWriter)` 构造的 reader，**不是** `io.Seeker`。MinIO SDK 对非 Seeker 的 reader 走 **multipart upload** 路径（initiate → upload part → complete），即使文件只有 1KB 也要 3 次 HTTP 往返，导致 p50=360ms 的极高延迟。

- **读取：HTTP 略快 1.28×。** 两者读路径相同（均调 `fm.DownloadFileAt` → `storage.ReadAt` → MinIO `GetObject` + Range），差异来自传输层。HTTP/1.1 多连接池在高并发读时能更充分地利用连接并行度；gRPC 单 TCP 连接的 HTTP/2 流受 goroutine 调度影响。但 gRPC 读 p50（2226μs）略低于 HTTP（2313μs），长尾更不稳定（p99=25271μs）。

### 5.2 读 vs 写

| 协议 | 写 (files/s) | 读 (files/s) | 读/写倍率 |
|------|-------------|-------------|----------|
| HTTP | 10.4 | 1565.5 | **150.8×** |
| gRPC | 488.4 | 1224.1 | **2.51×** |

**分析：HTTP 的读/写倍率高达 150.8×，写入是极端瓶颈。**

- **HTTP 写瓶颈根因**：MinIO SDK 对非 Seeker reader 走 multipart upload，每个 1KB 文件需 3 次 HTTP 往返（initiate multipart / upload part / complete multipart），加上分布式锁往返 + PG 元数据写入，单文件 p50=360ms。
- **gRPC 写经快速路径优化后**，走 Seeker → 单次 PUT，p50=7.8ms，读/写倍率收窄至 2.51×。
- **读取不受 multipart 影响**：`GetObject` 是单次 HTTP GET，两种协议均高效（1224~1566 files/s）。

### 5.3 MinIO vs 集中式本地存储（参考对比）

与同期集中式本地存储（LocalStorage）1000 文件测试对比（同为 c=4 量级，本地存储用 c=12）：

| 指标 | LocalStorage (c=12) | MinIO (c=4) | 差异 |
|------|---------------------|-------------|------|
| HTTP Write (files/s) | 1253.8 | 10.4 | MinIO 慢 120× |
| HTTP Read (files/s) | 4898.7 | 1565.5 | MinIO 慢 3.1× |
| gRPC Write (files/s) | 2455.3 | 488.4 | MinIO 慢 5.0× |
| gRPC Read (files/s) | 5293.1 | 1224.1 | MinIO 慢 4.3× |

> 注：本地存储用 c=12，MinIO 受内存限制用 c=4，非完全等并发对比。但即使折算并发差异，MinIO 的 HTTP 写仍然慢两个数量级，主因是 multipart upload 路径而非并发数。

## 6. 关键发现

1. **零错误、零数据丢失：** 4,000 次操作全部成功，MinIO + PostgreSQL + Redis 锁组合在持续负载下稳定可靠。

2. **HTTP 写入是严重瓶颈（10.4 files/s, p50=360ms）：** 根因为 MinIO SDK 对非 Seeker 的 `io.Reader`（`io.TeeReader`）走 multipart upload 路径，即使 1KB 文件也要 3 次 HTTP 往返。这是 HTTP `UploadFileFromReader` + MinIO `WriteFromReader` 组合的结构性缺陷。

3. **gRPC 快速路径有效缓解写入瓶颈（488 files/s）：** gRPC 单分块快速路径走 `fm.UploadFile` → `storage.Write`，使用 `bytes.NewReader`（Seeker），MinIO SDK 直接单次 PUT，p50=7.8ms，比 HTTP 快 47×。

4. **读取性能不受写入路径影响：** HTTP 读 1566 files/s、gRPC 读 1224 files/s，均高效。MinIO `GetObject` 是单次 HTTP GET，无 multipart 开销。

5. **MinIO 内存开销显著：** minio-go 客户端库在并发上传时内存占用远高于本地存储，c=12 即 OOM（RSS 3.8GB）。需设置 `GOMEMLIMIT` + `GOGC` 调优，并将并发降至 4。这是 MinIO 后端相对于本地存储的客观运维成本。

6. **对象存储不适合海量小文件直写：** 1KB 文件经对象存储 API（PUT/GET）每次都有 HTTP 往返开销，吞吐远低于本地文件系统的 `os.WriteFile`。生产环境对海量小文件应考虑合并写入或使用对象存储的批量 API。

## 7. 优化建议

1. **【关键】HTTP 写路径对齐 gRPC 快速路径：** 对小文件（如 < 1MB），HTTP `handleUpload` 应走 `fm.UploadFile`（内存缓冲 + `bytes.NewReader`）而非 `fm.UploadFileFromReader`（TeeReader 流式），使 MinIO SDK 走单次 PUT 而非 multipart。预计可提升 HTTP 写吞吐 40× 以上。

2. **MinIO `WriteFromReader` 优化：** 在 `storage.WriteFromReader` 中，若 reader 不是 Seeker，可先读入 `bytes.Buffer` 再以 `bytes.NewReader` 传入 `PutObject`，对小文件避免 multipart 路径。需权衡内存占用（已在 `UploadFileFromReader` 注释中说明此 fallback 设计）。

3. **连接池调优：** MinIO 客户端默认连接池可能不足以支撑高并发，可调大 `maxConns` 参数。

4. **内存管理：** 在 MinIO 后端下，建议生产环境设置 `GOMEMLIMIT` 为容器内存的 80%，避免 minio-go 客户端内存膨胀导致 OOM。

5. **并发参数：** MinIO 后端建议 HTTP/gRPC 并发 ≤ 8（2 核环境），高于此值内存风险增大且吞吐不再提升。

## 8. 复现方式

测试代码：[tests/massive_small_files_test.go](file:///workspace/tests/massive_small_files_test.go)
原始结果：[minio_1k_results.json](file:///workspace/minio_1k_results.json)

前置：
1. 启动 PostgreSQL 16 与 Redis 7，创建 `fsserver` 库/用户（密码 `fsserver123`），`max_connections≥300`；
2. 启动 MinIO 服务器：
   ```bash
   MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
   minio server /data/minio --address ":9000" --console-address ":9001"
   ```

运行测试（`FSS_BENCH_STORAGE=minio` 切换至 MinIO 后端）：

```bash
GOMEMLIMIT=1500MiB GOGC=20 \
FSS_BENCH_ENABLED=1 \
FSS_BENCH_COUNT=1000 \
FSS_BENCH_HTTP_CONCURRENCY=4 \
FSS_BENCH_GRPC_CONCURRENCY=4 \
FSS_BENCH_STORAGE=minio \
FSS_BENCH_MINIO_ENDPOINT=localhost:9000 \
FSS_BENCH_MINIO_ACCESS_KEY=minioadmin \
FSS_BENCH_MINIO_SECRET_KEY=minioadmin \
FSS_BENCH_OUT=/workspace/minio_1k_results.json \
go test -run 'TestMassiveSmallFiles_Performance' -count=1 -v -timeout 10m ./tests/
```

> 测试由 `FSS_BENCH_ENABLED=1` 环境变量门控，默认不运行，不会污染 CI。
> `FSS_BENCH_STORAGE` 默认为 `local`（集中式存储），设为 `minio` 切换至 MinIO 对象存储。

## 9. 测试方法学说明

- **真实双协议：** HTTP 走 Gin REST API（`POST /api/v1/files` 上传、`GET /api/v1/files/*path` 下载）；gRPC 走真实 TCP 监听的 `FileService`（`UploadFile` 客户端流式、`DownloadFile` 服务端流式），客户端用 `grpc.NewClient` 建立真实连接，非进程内调用。
- **真实 MinIO：** 使用真实 MinIO 服务器（非 gofakes3 mock），每次测试创建唯一 bucket `fssbench-<timestamp>`，确保数据隔离。
- **公平并发：** HTTP 与 gRPC 均使用并发 4（受 MinIO 内存限制），消除并发差异对协议对比的干扰。
- **共享同一后端：** 四阶段共用同一套 PostgreSQL + Redis + MinIO，确保对比变量唯一。
- **逐操作计时：** 记录每一次操作的端到端延迟（含网络 + 锁 + DB + MinIO API），统计 min/avg/max/p50/p95/p99。
- **隔离清理：** 每阶段开始前清空 DB 表、Flush Redis、清理 MinIO path locks，保证阶段间互不干扰。
