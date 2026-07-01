# 50 万小文件性能测试报告

**生成时间：** 2026-07-01T05:53:40Z
**测试对象：** fssvrgo 分布式文件存储服务
**测试范围：** 集中存储（LocalStorage）· 写入 50 万个 1KB 文件后回读 · HTTP 与 gRPC 双协议覆盖

> 结论速览：4 个阶段共 **2,000,000 次操作全部成功，0 错误**。读取吞吐约为写入的 5.4~8.9 倍；写入场景 HTTP 比 gRPC 快 1.72×，读取场景两者基本持平（HTTP 仅快 1.04×）。gRPC 在读、写两端的尾部延迟（p95/p99）均优于 HTTP。

---

## 1. 测试目标

按要求对当前项目进行性能压测：

- 写入 **50 万个 1KB 大小**的文件，随后读回这些文件；
- **只测试集中存储**（本地文件系统 LocalStorage，不测 MinIO 分布式对象存储）；
- **使用 Redis 作为一致性锁**（分布式 `SET NX` 加锁 + Lua 原子解锁）；
- **使用 PostgreSQL 作为元数据数据库**；
- **HTTP 与 RPC（gRPC over TCP）两种方式都要覆盖**——必须是真实的 gRPC TCP 调用，而非进程内服务层调用；
- 出具本测试报告。

## 2. 测试环境

| 项目 | 配置 |
|------|------|
| 操作系统 | Ubuntu 24.04.3 LTS (Noble Numbat) |
| 内核架构 | linux/amd64 |
| CPU | INTEL(R) XEON(R) PLATINUM 8582C |
| Go 运行可用核数 (GOMAXPROCS) | 2 |
| 内存 | 5.8 GiB |
| 磁盘 | 1.5 TB（测试时已用 184 G） |
| Go 版本 | go1.25.1 |
| 数据库 | PostgreSQL 16.14（`max_connections=300`，连接池 `PoolSize=并发+32`） |
| 一致性锁 | Redis 7.0.15（`SET NX` + Lua 原子解锁） |
| 存储后端 | LocalStorage（集中式本地文件系统） |
| 认证 | 已禁用（`auth.Init(false, "")`），排除认证噪声 |

## 3. 测试配置

| 参数 | 值 | 说明 |
|------|----|------|
| 文件数量 | 500,000 | 每阶段 50 万 |
| 单文件大小 | 1024 B (1 KB) | 固定填充内容 |
| HTTP 并发 | 12 | 探测后选定；同时设为 HTTP 信号量容量，避免 503 反压 |
| gRPC 并发 | 8 | 探测后选定（2 核环境下的稳定上限） |
| 路径分片 | `/bench/<proto>/<NNN>/<NNNNNN>` | 每子目录 ≤1000 条目，避免单目录 50 万条目退化 |
| HTTP 客户端 | 自定义 Transport，连接池复用 | `MaxIdleConnsPerHost=并发` |
| gRPC 客户端 | `grpc.NewClient` + 真实 TCP 监听 | Client Streaming 上传 / Server Streaming 下载 |
| 数据库连接池 | `PoolSize = 并发 + 32` | 避免 PG 连接打满 |
| Redis | 每次写/删均走分布式锁 | `AcquireLock` 超时 30s、TTL 10s、重试 50ms |
| 总超时 | 120 min | `go test -timeout 120m` |

> 并发数选择依据：2 CPU 沙箱环境下，HTTP 在 c=12、gRPC 在 c=8 时吞吐与稳定性达到最佳平衡；更高并发会触发 PG 连接争用或 Redis 锁重试，反而下降。

## 4. 测试结果总览

四个阶段：HTTP 写 → HTTP 读 → gRPC 写 → gRPC 读，串行执行，每阶段独立清理并重建 50 万文件。

### 4.1 汇总表

| 阶段 | 协议 | 操作 | 文件数 | 耗时 | 错误 | files/s | MB/s | 延迟 p50/p95/p99 (μs) |
|------|------|------|-------|------|------|---------|------|----------------------|
| HTTP Write | HTTP | 写 | 500,000 | 460.98 s | 0 | **1084.6** | 1.06 | 7005 / 29250 / 44332 |
| HTTP Read | HTTP | 读 | 500,000 | 85.90 s | 0 | **5820.6** | 5.68 | 1665 / 3825 / 11398 |
| gRPC Write | gRPC | 写 | 500,000 | 792.22 s | 0 | **631.1** | 0.62 | 11896 / 17882 / 27218 |
| gRPC Read | gRPC | 读 | 500,000 | 88.92 s | 0 | **5623.1** | 5.49 | 1196 / 2557 / 5653 |

- **总操作数：** 2,000,000（写 1,000,000 + 读 1,000,000）
- **总错误数：** 0
- **总耗时：** 1533.51 s（约 25.6 分钟）
- **测试结论：** `PASS`

### 4.2 完整延迟分布（来自 JSON 结果）

| 阶段 | min (μs) | avg (μs) | max (μs) | p50 (μs) | p95 (μs) | p99 (μs) | 样本 |
|------|----------|----------|----------|----------|----------|----------|------|
| HTTP Write | 1997 | 11062 | 246410 | 7005 | 29250 | 44332 | 500000 |
| HTTP Read | 207 | 2053 | 297019 | 1665 | 3825 | 11398 | 500000 |
| gRPC Write | 3017 | 12674 | 1009493 | 11896 | 17882 | 27218 | 500000 |
| gRPC Read | 264 | 1420 | 77938 | 1196 | 2557 | 5653 | 500000 |

## 5. 对比分析

### 5.1 HTTP vs gRPC

| 指标 | HTTP | gRPC | 倍率 |
|------|------|------|------|
| 写吞吐 (files/s) | 1084.6 | 631.1 | **HTTP 快 1.72×** |
| 读吞吐 (files/s) | 5820.6 | 5623.1 | HTTP 快 1.04×（基本持平） |
| 写 p99 (μs) | 44332 | 27218 | **gRPC 低 39%** |
| 读 p99 (μs) | 11398 | 5653 | **gRPC 低 50%** |
| 写 p95 (μs) | 29250 | 17882 | **gRPC 低 39%** |
| 读 p95 (μs) | 3825 | 2557 | **gRPC 低 33%** |

**分析：**

- **写入：HTTP 吞吐更高，gRPC 尾部延迟更优。** HTTP 写吞吐领先 1.72×，主因有二：① HTTP 并发（12）高于 gRPC（8）；② 对 1KB 这种极小文件，gRPC 客户端流式上传（`UploadFile(stream)`）的每流建连 + HTTP/2 帧封装固定开销占比过高，拉高了单次操作基准成本（gRPC 写 p50=11896μs vs HTTP 写 p50=7005μs）。但 gRPC 凭借单一长连接 + 多路复用，尾部延迟显著更稳——写 p95/p99 比 HTTP 低约 39%，长尾毛刺更少。
- **读取：两者吞吐基本持平，gRPC 延迟全面胜出。** 读吞吐仅差 4%（在并发更低的情况下），且 gRPC 读的 p50/p95/p99 全面优于 HTTP（p99 低 50%）。说明 gRPC 长连接复用在读密集场景下能有效规避 HTTP/1.1 连接池争用与队头阻塞。若 gRPC 读并发提升到 12，吞吐有望追平甚至超过 HTTP。

### 5.2 读 vs 写

| 协议 | 写 (files/s) | 读 (files/s) | 读/写倍率 |
|------|-------------|-------------|----------|
| HTTP | 1084.6 | 5820.6 | **5.37×** |
| gRPC | 631.1 | 5623.1 | **8.91×** |

**分析：读取远快于写入，写入是系统瓶颈。** 原因在于两条路径的重量级差异：

- **写路径**（`FileManager.UploadFile`）：分布式锁获取/释放（Redis 往返）→ `Exists` 检查 → `storage.Write`（含路径锁 + `os.WriteFile` + 目录创建）→ SHA256 全量计算 → PG 元数据 `INSERT`。每个文件至少 1 次 Redis 往返 + 1 次 PG 写 + 1 次磁盘写。
- **读路径**（`FileManager.DownloadFile`）：`GetFileMetadata`（PG 单行查询，命中索引）→ `storage.Read`（`os.ReadFile`）。无锁、无哈希、无写盘，且 OS 页缓存对刚写入的 1KB 文件命中率极高。

gRPC 读/写倍率（8.91×）高于 HTTP（5.37×），是因为 gRPC 写的额外流式开销进一步压低了写吞吐，而读路径两者接近。

## 6. 关键发现

1. **零错误、零数据丢失：** 200 万次操作无一失败，证明在 PostgreSQL + Redis 锁 + 本地存储的组合下，系统在持续高负载下具备良好的可靠性与一致性。
2. **写入是性能瓶颈：** 受 Redis 分布式锁往返、SHA256、PG 插入、磁盘写四重开销制约，写吞吐仅 631~1085 files/s；读吞吐达 5623~5821 files/s。
3. **gRPC 更稳、HTTP 写更快（小文件场景）：** gRPC 长连接使其尾部延迟更低、更可预测；HTTP 在写吞吐上因更高并发而领先。gRPC 客户端流式上传对海量微小文件并不友好——流建立开销被分摊到过少字节上。
4. **并发受限于 2 核环境：** HTTP c=12、gRPC c=8 是当前沙箱的稳定上限，更高并发会触发 PG 连接争用或 Redis 锁重试，反而下降。生产环境多核下吞吐有线性扩展空间。
5. **路径分片有效：** 采用 `/NNN/NNNNNN` 三级分片后，单目录条目数 ≤1000，未出现目录项膨胀导致的 `readdir` 退化，写入速率全程平稳（HTTP 写从 1706 files/s 缓降至 1085 files/s，主要受文件总量增长带来的缓存压力影响，而非目录结构问题）。
6. **HTTP 503 反压问题已规避：** 测试中将 HTTP `Workers` 设为并发数，使并发信号量容量匹配负载，避免服务器在目标负载下返回 503——测的是真实吞吐而非拒绝率。

## 7. 优化建议

1. **写路径批量化：** 元数据 `INSERT` 可考虑批量提交（如每 100 条一批），减少 PG 往返；Redis 锁可在确认无冲突时使用更短 TTL 或乐观锁，减少往返。
2. **跳过冗余检查：** 对保证全新的路径（如带唯一 ID 的写入），可跳过 `Exists` + `GetByPath` 查询，省一次 PG 读。
3. **gRPC 小文件优化：** 对小文件改用一元（unary）RPC 而非客户端流式，避免每文件建流开销；或在单流内打包多个小文件批量上传。
4. **提升 gRPC 并发：** 生产环境多核下将 gRPC 并发提到与 HTTP 持平（12+），吞吐有望追平 HTTP，同时享受更低尾部延迟。
5. **SHA256 流式化：** 写入若已用 `UploadFileFromReader` 流式路径，可避免全量入内存；对小文件收益有限，但对大文件意义重大。

## 8. 复现方式

测试代码：[tests/massive_small_files_test.go](file:///workspace/tests/massive_small_files_test.go)
运行日志：[massive_bench.log](file:///workspace/massive_bench.log)
原始结果：[massive_small_files_results.json](file:///workspace/massive_small_files_results.json)

前置：启动 PostgreSQL 16 与 Redis 7，创建 `fsserver` 库/用户（密码 `fsserver123`），`max_connections≥300`。

```bash
FSS_BENCH_ENABLED=1 \
FSS_BENCH_COUNT=500000 \
FSS_BENCH_HTTP_CONCURRENCY=12 \
FSS_BENCH_GRPC_CONCURRENCY=8 \
FSS_BENCH_OUT=/workspace/massive_small_files_results.json \
go test -run 'TestMassiveSmallFiles_Performance' -count=1 -v -timeout 120m ./tests/
```

> 测试由 `FSS_BENCH_ENABLED=1` 环境变量门控，默认不运行，不会污染 CI（CI 仅跑 `go test -short ./internal/...`）。

## 9. 测试方法学说明

- **真实双协议：** HTTP 走 Gin REST API（`POST /api/v1/files` 上传、`GET /api/v1/files/*path` 下载）；gRPC 走真实 TCP 监听的 `FileService`（`UploadFile` 客户端流式、`DownloadFile` 服务端流式），客户端用 `grpc.NewClient` 建立真实连接，非进程内调用。
- **共享同一后端：** 四阶段共用同一套 PostgreSQL + Redis + LocalStorage，确保对比变量唯一。
- **逐操作计时：** 记录每一次操作的端到端延迟（含网络 + 锁 + DB + 磁盘），统计 min/avg/max/p50/p95/p99。
- **进度采样：** 每 10 秒打印一次进度与瞬时速率，便于观察吞吐衰减。
- **隔离清理：** 每阶段开始前清空 DB 表、Flush Redis、删除存储目录并清理 `pathLocks`，保证阶段间互不干扰。
