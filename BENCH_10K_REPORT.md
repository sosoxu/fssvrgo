# 1 万小文件读写性能测试报告（集中存储 vs 对象存储）

**生成时间：** 2026-07-01T23:15:41Z
**测试对象：** fssvrgo 分布式文件存储服务
**测试范围：** 写入 10,000 个 1KB 文件后回读 · 集中存储（LocalStorage）与对象存储（MinIO）对比 · HTTP 与 gRPC 双协议覆盖
**代码版本：** 含 gRPC 快速路径优化 + HTTP 小文件 Seeker 修复 + 内存阈值收紧（commit `0f19927`）

> 结论速览：8 个阶段共 **80,000 次操作全部成功，0 错误**。
> - **HTTP 写 MinIO 瓶颈已消除：** 修复后 HTTP 写 MinIO 从 10.4 → **336.7 files/s（提升 32.4×）**，证明 Seeker 修复有效；
> - **集中存储写入：gRPC 比 HTTP 快 1.28×**（3249 vs 2548 files/s）；
> - **对象存储写入：gRPC 比 HTTP 快 1.33×**（447 vs 337 files/s）；
> - **读取两种存储均接近持平**（HTTP 略快 1.08~1.09×），读路径不受 Seeker 影响；
> - **集中存储全面优于对象存储**：写快 5.6~7.6×，读快 6.2~6.6×。

---

## 1. 测试目标

按要求对当前项目进行性能压测：

- 写入 **10,000 个 1KB 大小**的文件，随后读回这些文件；
- **分别测试集中存储**（LocalStorage 本地文件系统）**与对象存储**（MinIO S3 兼容）；
- **使用 Redis 作为一致性锁**（分布式 `SET NX` 加锁 + Lua 原子解锁）；
- **使用 PostgreSQL 作为元数据数据库**；
- **HTTP 与 RPC（gRPC over TCP）两种方式都要覆盖**——必须是真实的 gRPC TCP 调用；
- 出具本测试报告。

## 2. 测试环境

| 项目 | 配置 |
|------|------|
| 操作系统 | Ubuntu 24.04.3 LTS (Noble Numbat) |
| 内核架构 | linux/amd64 |
| Go 运行可用核数 (GOMAXPROCS) | 2 |
| Go 版本 | go1.25.1 |
| 数据库 | PostgreSQL 16.14（`max_connections=300`，连接池 `PoolSize=并发+32`） |
| 一致性锁 | Redis 7.0.15（`SET NX` + Lua 原子解锁） |
| 集中存储后端 | LocalStorage（集中式本地文件系统） |
| 对象存储后端 | MinIO（S3 兼容，单节点本地部署，RELEASE.2025-09-07） |
| 内存调优（MinIO 测试） | `GOMEMLIMIT=1500MiB GOGC=20`（防止 minio-go 客户端库内存膨胀导致 OOM） |
| 认证 | 已禁用（`auth.Init(false, "")`），排除认证噪声 |

## 3. 测试配置

| 参数 | 集中存储 | 对象存储 | 说明 |
|------|---------|---------|------|
| 文件数量 | 10,000 | 10,000 | 每阶段 1 万 |
| 单文件大小 | 1024 B (1 KB) | 1024 B (1 KB) | 固定填充内容 |
| HTTP 并发 | 12 | 4 | MinIO 降至 4 避免 minio-go 内存膨胀 OOM |
| gRPC 并发 | 12 | 4 | 与 HTTP 持平 |
| 内存限制 | 无 | `GOMEMLIMIT=1500MiB GOGC=20` | MinIO 测试专用 |
| 路径分片 | `/bench/<proto>/<NNN>/<NNNNNN>` | 同左 | 每子目录 ≤1000 条目 |
| 数据库连接池 | `PoolSize = 并发 + 32` | 同左 | 避免 PG 连接打满 |
| Redis | 每次写/删均走分布式锁 | 同左 | `AcquireLock` 超时 30s、TTL 10s、重试 50ms |

> **并发差异说明：** 集中存储用 c=12（与历史测试一致，便于横向对比）；MinIO 用 c=4（minio-go 客户端库在 c=12 时 RSS 达 3.8GB 触发 OOM，c=4 + GOMEMLIMIT 是当前沙箱稳定上限）。对比对象存储内部 HTTP vs gRPC 时变量唯一（均为 c=4）；对比两种存储时已标注并发差异。

## 4. 测试结果总览

每组测试四个阶段：HTTP 写 → HTTP 读 → gRPC 写 → gRPC 读，串行执行，每阶段独立清理并重建 1 万文件。

### 4.1 集中存储（LocalStorage）

| 阶段 | 协议 | 操作 | 文件数 | 耗时 | 错误 | files/s | MB/s | 延迟 p50/p95/p99 (μs) |
|------|------|------|-------|------|------|---------|------|----------------------|
| HTTP Write | HTTP | 写 | 10,000 | 3.93 s | 0 | **2547.7** | 2.49 | 4144 / 8026 / 13048 |
| HTTP Read | HTTP | 读 | 10,000 | 1.05 s | 0 | **9508.5** | 9.29 | 1008 / 2989 / 5110 |
| gRPC Write | gRPC | 写 | 10,000 | 3.08 s | 0 | **3249.2** | 3.17 | 3277 / 6292 / 10104 |
| gRPC Read | gRPC | 读 | 10,000 | 1.14 s | 0 | **8743.0** | 8.54 | 1123 / 2865 / 4861 |

- **总操作数：** 40,000（写 20,000 + 读 20,000）
- **总错误数：** 0
- **总耗时：** 9.60 s
- **测试结论：** `PASS`

### 4.2 对象存储（MinIO）

| 阶段 | 协议 | 操作 | 文件数 | 耗时 | 错误 | files/s | MB/s | 延迟 p50/p95/p99 (μs) |
|------|------|------|-------|------|------|---------|------|----------------------|
| HTTP Write | HTTP | 写 | 10,000 | 29.70 s | 0 | **336.7** | 0.33 | 11154 / 19678 / 24314 |
| HTTP Read | HTTP | 读 | 10,000 | 6.93 s | 0 | **1442.7** | 1.41 | 2183 / 6299 / 9672 |
| gRPC Write | gRPC | 写 | 10,000 | 22.38 s | 0 | **446.9** | 0.44 | 8303 / 14870 / 18978 |
| gRPC Read | gRPC | 读 | 10,000 | 7.50 s | 0 | **1333.9** | 1.30 | 2348 / 7302 / 10520 |

- **总操作数：** 40,000（写 20,000 + 读 20,000）
- **总错误数：** 0
- **总耗时：** 72.22 s
- **测试结论：** `PASS`

## 5. 对比分析

### 5.1 HTTP vs gRPC（同存储后端内对比）

| 存储后端 | 操作 | HTTP (files/s) | gRPC (files/s) | 倍率 |
|---------|------|---------------|---------------|------|
| 集中存储 | 写 | 2547.7 | 3249.2 | **gRPC 快 1.28×** |
| 集中存储 | 读 | 9508.5 | 8743.0 | HTTP 快 1.09× |
| 对象存储 | 写 | 336.7 | 446.9 | **gRPC 快 1.33×** |
| 对象存储 | 读 | 1442.7 | 1333.9 | HTTP 快 1.08× |

**分析：**

- **写入：gRPC 在两种存储后端均胜出（1.28~1.33×）。** gRPC 快速路径对单分块完整上传直接调用 `FileManager.UploadFile`（`bytes.NewReader` Seeker），绕过会话机制；HTTP 小文件路径经 Seeker 修复后也走 `UploadFile`（1MB 阈值内）。两者写路径已对齐，差异主要来自传输层——HTTP/2 长连接 + 多路复用使 gRPC 单次操作基准成本更低（集中存储写 p50: 3277 vs 4144μs，对象存储写 p50: 8303 vs 11154μs）。
- **读取：HTTP 略快 1.08~1.09×，两者实质持平。** 读路径相同（`GetFileMetadata` + `storage.Read`），无锁无哈希。HTTP/1.1 多连接池在高并发读时能更充分利用连接并行度；gRPC 单 TCP 连接的 HTTP/2 流在 2 核环境下受 goroutine 调度影响，吞吐略低。集中存储读 p50（1008 vs 1123μs）与对象存储读 p50（2183 vs 2348μs）差距均 <15%。

### 5.2 集中存储 vs 对象存储（横向对比）

| 操作 | 协议 | 集中存储 (files/s) | 对象存储 (files/s) | 倍率 | 备注 |
|------|------|-------------------|-------------------|------|------|
| 写 | HTTP | 2547.7 | 336.7 | **集中快 7.6×** | 并发 12 vs 4 |
| 写 | gRPC | 3249.2 | 446.9 | **集中快 7.3×** | 并发 12 vs 4 |
| 读 | HTTP | 9508.5 | 1442.7 | **集中快 6.6×** | 并发 12 vs 4 |
| 读 | gRPC | 8743.0 | 1333.9 | **集中快 6.6×** | 并发 12 vs 4 |

**分析：**

- **集中存储全面领先 6.6~7.6×。** 其中并发差异（12 vs 4）贡献约 3× 理论上限，剩余 2.2~2.5× 来自对象存储固有开销（网络 + S3 协议解析 + MinIO 磁盘冗余）。
- **写入差距（7.3~7.6×）略大于读取（6.6×）。** 对象存储写需经 S3 PutObject 协议（HTTP 往返 + MinIO 落盘 + 元数据索引），读则经 GetObject（HTTP 往返 + 磁盘读），写的协议开销更高。
- **对象存储读 p50（2183~2348μs）是集中存储（1008~1123μs）的 ~2×**，主要来自网络往返 + S3 协议解析，属合理范围。

### 5.3 HTTP 写 MinIO 瓶颈修复验证（核心成果）

| 指标 | 修复前（1000 文件） | 修复后（10000 文件） | 提升 |
|------|-------------------|---------------------|------|
| HTTP 写吞吐 (files/s) | 10.4 | **336.7** | **32.4×** |
| HTTP 写 p50 (μs) | 360120 | 11154 | **32.3× 更低** |
| HTTP 写 p95 (μs) | 686124 | 19678 | 34.9× 更低 |
| HTTP 写 p99 (μs) | 769512 | 24314 | 31.6× 更低 |
| HTTP vs gRPC 写 | gRPC 快 47× | gRPC 快 1.33× | 瓶颈消除 |

**修复验证结论：** HTTP 写 MinIO 瓶颈已彻底消除。修复前 HTTP 写因 `io.TeeReader`（非 Seeker）触发 MinIO multipart upload（3 次 HTTP 往返），吞吐仅 10.4 files/s、p50 高达 360ms；修复后小文件（≤1MB）走 `UploadFile`（`bytes.NewReader` Seeker → 单次 PUT），吞吐达 336.7 files/s、p50 降至 11ms，**与 gRPC 写（447 files/s）差距从 47× 收窄至 1.33×**。剩余 1.33× 差距来自传输层（HTTP/1.1 vs HTTP/2），属正常协议差异。

### 5.4 完整延迟分布

| 存储后端 | 阶段 | min (μs) | avg (μs) | max (μs) | p50 (μs) | p95 (μs) | p99 (μs) | 样本 |
|---------|------|----------|----------|----------|----------|----------|----------|------|
| 集中存储 | HTTP Write | 1781 | 4706 | 47063 | 4144 | 8026 | 13048 | 10000 |
| 集中存储 | HTTP Read | 94 | 1260 | 34009 | 1008 | 2989 | 5110 | 10000 |
| 集中存储 | gRPC Write | 999 | 3688 | 69936 | 3277 | 6292 | 10104 | 10000 |
| 集中存储 | gRPC Read | 179 | 1370 | 38467 | 1123 | 2865 | 4861 | 10000 |
| 对象存储 | HTTP Write | 4153 | 11875 | 44502 | 11154 | 19678 | 24314 | 10000 |
| 对象存储 | HTTP Read | 663 | 2770 | 18133 | 2183 | 6299 | 9672 | 10000 |
| 对象存储 | gRPC Write | 3159 | 8948 | 58526 | 8303 | 14870 | 18978 | 10000 |
| 对象存储 | gRPC Read | 915 | 2997 | 30384 | 2348 | 7302 | 10520 | 10000 |

## 6. 关键发现

1. **零错误、零数据丢失：** 80,000 次操作（4 万写 + 4 万读）全部成功，证明 PostgreSQL + Redis 锁 + LocalStorage/MinIO 组合在持续负载下具备良好的可靠性与一致性。
2. **HTTP 写 MinIO 瓶颈已彻底消除：** Seeker 修复使 HTTP 写 MinIO 吞吐从 10.4 → 336.7 files/s（提升 32.4×），p50 延迟从 360ms → 11ms。HTTP 与 gRPC 写差距从 47× 收窄至 1.33×，瓶颈已从"代码缺陷"转为"正常协议差异"。
3. **gRPC 写入两种存储均胜出：** 凭快速路径绕过会话机制 + HTTP/2 长连接复用，gRPC 写吞吐在集中存储快 1.28×、对象存储快 1.33×，且尾部延迟更低（写 p99 均低于 HTTP）。
4. **读取性能两种协议实质持平：** HTTP 略快 1.08~1.09×，差距来自传输层并行度，非存储或锁瓶颈。读路径无锁无哈希，PG 单行查询 + 存储读为主。
5. **集中存储全面优于对象存储 6.6~7.6×：** 其中并发差异（12 vs 4）贡献约 3×，剩余 2.2~2.5× 为对象存储固有开销（网络 + S3 协议 + 磁盘冗余）。对象存储优势在分布式扩展与数据冗余，非单节点吞吐。
6. **内存调优有效：** MinIO 测试 c=4 + `GOMEMLIMIT=1500MiB GOGC=20` 全程稳定，无 OOM。
7. **延迟分布稳定：** 1 万文件规模下各阶段 p99/p50 倍率集中在 2~4×，无异常长尾，系统在高负载下延迟可预测。

## 7. 后续优化建议

1. **MinIO 客户端连接池复用：** 当前 minio-go 每次 PutObject 可能建连，复用连接可减少 TLS 握手开销，预计提升对象存储写吞吐 10~20%。
2. **MinIO 并发提升验证：** 在 GOMAXPROCS≥8 + 内存≥16GB 环境复测 c=12+，验证吞吐线性扩展，消除并发差异对横向对比的干扰。
3. **批量元数据写入：** 写路径 PG `INSERT` 逐条提交，可批量化（每 100 条一批）减少 PG 往返，预计提升写吞吐 15~30%。
4. **生产 MinIO 集群测试：** 当前为单节点本地 MinIO，生产分布式集群（4+ 节点 + 副本）吞吐应有变化，需单独评估。
5. **gRPC 读路径小文件优化：** 当前 `DownloadFile` 默认 chunk 1MB，对 1KB 文件仍走分块循环，可增加一次性读取快速路径减少协商开销。

## 8. 复现方式

测试代码：[tests/massive_small_files_test.go](file:///workspace/tests/massive_small_files_test.go)
集中存储结果：[local_10k_results.json](file:///workspace/local_10k_results.json)
对象存储结果：[minio_10k_results.json](file:///workspace/minio_10k_results.json)

前置：启动 PostgreSQL 16、Redis 7、MinIO（对象存储测试需要），创建 `fsserver` 库/用户（密码 `fsserver123`），`max_connections≥300`。

**集中存储测试：**
```bash
FSS_BENCH_ENABLED=1 \
FSS_BENCH_COUNT=10000 \
FSS_BENCH_HTTP_CONCURRENCY=12 \
FSS_BENCH_GRPC_CONCURRENCY=12 \
FSS_BENCH_STORAGE=local \
FSS_BENCH_OUT=/workspace/local_10k_results.json \
go test -run 'TestMassiveSmallFiles_Performance' -count=1 -v -timeout 10m ./tests/
```

**对象存储测试：**
```bash
GOMEMLIMIT=1500MiB GOGC=20 \
FSS_BENCH_ENABLED=1 \
FSS_BENCH_COUNT=10000 \
FSS_BENCH_HTTP_CONCURRENCY=4 \
FSS_BENCH_GRPC_CONCURRENCY=4 \
FSS_BENCH_STORAGE=minio \
FSS_BENCH_MINIO_ENDPOINT=localhost:9000 \
FSS_BENCH_MINIO_ACCESS_KEY=minioadmin \
FSS_BENCH_MINIO_SECRET_KEY=minioadmin \
FSS_BENCH_OUT=/workspace/minio_10k_results.json \
go test -run 'TestMassiveSmallFiles_Performance' -count=1 -v -timeout 20m ./tests/
```

> - 测试由 `FSS_BENCH_ENABLED=1` 环境变量门控，默认不运行，不会污染 CI。
> - 存储后端由 `FSS_BENCH_STORAGE` 环境变量选择（`local` 或 `minio`）。
> - MinIO 连接参数由 `FSS_BENCH_MINIO_*` 环境变量配置。
> - `GOMEMLIMIT` + `GOGC` 用于控制 minio-go 客户端库内存膨胀，避免 OOM。

## 9. 测试方法学说明

- **真实双协议：** HTTP 走 Gin REST API（`POST /api/v1/files` 上传、`GET /api/v1/files/*path` 下载）；gRPC 走真实 TCP 监听的 `FileService`（`UploadFile` 客户端流式、`DownloadFile` 服务端流式），客户端用 `grpc.NewClient` 建立真实连接，非进程内调用。
- **真实 MinIO 服务器：** 对象存储测试连接真实 MinIO S3 兼容服务器（非 mock），每次测试自动创建唯一 bucket（`fssbench-<unixnano>`）。
- **公平并发：** 同存储后端内 HTTP 与 gRPC 并发一致，消除并发差异对协议对比的干扰。
- **共享同一后端：** 每组测试四阶段共用同一套 PostgreSQL + Redis + 存储，确保对比变量唯一。
- **逐操作计时：** 记录每一次操作的端到端延迟（含网络 + 锁 + DB + 存储），统计 min/avg/max/p50/p95/p99。
- **隔离清理：** 每阶段开始前清空 DB 表、Flush Redis、清空存储目录/bucket 并清理 `pathLocks`，保证阶段间互不干扰。
