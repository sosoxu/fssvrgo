# fssvrgo 设计评审报告

## 0. 元信息

| 项 | 内容 |
|---|---|
| 评审对象 | fssvrgo @ `a4abea6` |
| 评审分支 | `review/acceptance-20260911` |
| 评审日期 | 2026-09-11 |
| 评审范围 | `docs/requirements.md`、`docs/design.md`、`README.md`、`internal/` 与 `cmd/` 全部非测试代码（11,068 行） |
| 评审方法 | 文档-实现对照走查 + 全仓 grep 取证 + 真库实测（`go test ./internal/...` 连 PostgreSQL 12.6） |
| 结论 | 架构方向正确，但存在 3 个结构性缺陷和 14 项具体问题；需求文档目前**不能作为验收基线** |

本报告的每条结论都附有代码位置或实测输出，问题编号与 `ISSUES.md` 的"第二轮评审"登记表一一对应。

## 1. 项目目标与设计主张

**目标**：高性能分布式文件存储服务，HTTP + gRPC 双协议，支持大文件分段并发传输，多实例部署下保证数据一致性。

**设计主张**（`docs/design.md`）：

- 四层结构：API 层（Gin / gRPC）→ 服务层（FileManager / Transfer / Directory / FileList）→ 分布式层（分布式锁 / 会话存储）→ 存储与数据库层
- 双后端可切换：local / MinIO、SQLite / PostgreSQL，单机开箱即用、生产多实例共用同一套代码
- 大文件优化：增量哈希、预分配、分段并发 `WriteAt`、路径级细粒度锁、无锁进度追踪
- 多实例一致性：Redis 分布式锁（SET NX + Lua 解锁，带续期）+ Redis 会话存储 + 共享存储

## 2. 设计合理性评估

### 2.1 合理的部分

**双协议确实共享服务层**。HTTP 与 gRPC 都调用同一批 service，没有出现两套并行的业务逻辑，行为一致性有结构保证。

**后端可切换的抽象方向正确**。`StorageAdapter`、`SessionStore`、`DistributedLock` 三个接口让单机形态（local + memory + SQLite）与生产形态（MinIO + Redis + PostgreSQL）共用代码路径，符合需求文档 4.1 / 4.2 两种部署要求。

**性能设计有据可依**。增量 SHA256、预分配 `Truncate`、`WriteAt` 分段并发写、`atomic` 进度、路径级锁，与需求文档 3.3 节逐条对应，且有 `BENCH_10K_REPORT.md` 等实测报告支撑，不是纸面设计。

**关键取舍被显式记录**。`internal/database/audit_writer.go:15-22` 写明崩溃会丢失缓冲区日志；`internal/storage/minio.go:115-129` 写明对象存储不支持原地随机写；`internal/service/transfer/service.go:132-142` 写明默认临时目录是进程私有的、跨实例恢复需要共享卷；`internal/api/http/server.go:487-492` 写明加密路径必须全量入内存。作者清楚边界在哪。

**测试投入充分**。`tests/` 与 `internal/` 下测试代码合计 20,405 行，覆盖多实例一致性、Redis 锁、PostgreSQL 一致性、大文件并发、分段 vs 不分段对比等场景。

### 2.2 总体评价

架构分层方向正确，性能路径的设计是经过实测验证的，不是空谈。问题集中在三处：

1. **部分模块"装配了但没接线"**——代码、配置、文档三方都在，运行时却不生效；
2. **存储抽象这个核心 seam 画得偏低**——把对象存储无法满足的字节级随机写语义放进了接口；
3. **文档与实现相互漂移**——需求文档声称未实现的已实现，声称已支持的前提条件没写。

## 3. 缺陷清单

| 编号 | 级别 | 标题 |
|---|---|---|
| R1 | P0 | filelist 计数查询缺子查询别名，PostgreSQL 上必然失败 |
| R2 | P0 | 一致性与服务发现模块未接线（死代码） |
| R3 | P0 | 存储与元数据之间缺少原子性保障与对账补偿 |
| R4 | P1 | `StorageAdapter` 接口层次过低且含死方法 |
| R5 | P1 | `StorageAdapter.Exists` 吞掉错误 |
| R6 | P1 | CI 无真实 PostgreSQL/Redis，跳过而非失败，掩盖缺陷 |
| R7 | P1 | 需求、设计、README 与实现三方漂移 |
| R8 | P2 | HTTP 层与持久化耦合，单文件 1717 行 |
| R9 | P2 | 服务层依赖具体 `*database.DB`，单元测试必须起真库 |
| R10 | P2 | 请求上下文未贯穿（53 处 `context.Background()`） |
| R11 | P2 | SQL 方言翻译采用文本替换，语义不安全 |
| R12 | P2 | 进程内双层锁与锁序不统一，存储层锁表无回收 |
| R13 | P2 | 数据库 schema 定义重复两份 |
| R14 | P3 | 加密路径全量入内存，并发下有内存放大 |
| R15 | P3 | 同机多实例启动清理会误删其它实例临时目录 |
| R16 | P3 | 预编译语句缓存错误路径泄漏 |
| R17 | P3 | 审计日志异步批写的可见性窗口 |

### R1 [P0] filelist 计数查询缺子查询别名

**位置**：`internal/service/filelist/service.go:150`

```go
SELECT COUNT(*) FROM (SELECT id FROM files WHERE %s UNION ALL SELECT id FROM directories WHERE %s)
```

PostgreSQL 要求 `FROM` 子查询必须有别名，执行报 `42601 subquery in FROM must have an alias`。SQLite 与 MySQL 允许省略，因此该缺陷在 SQLite 时代不会暴露，测试套件迁移到 PostgreSQL（提交 `a4abea6`）后才发现。

**影响**：`internal/service/filelist` 全部用例失败（`TestListFiles_Empty`、`_WithFiles`、`_Pagination`、`_SortBy` 全部 8 个子用例、`_Recursive`、`TestListFilesWithTotal_ComputesExactCount`），对应 HTTP 列表接口中带 total 计数的路径。这在生产 PostgreSQL 部署下是必现故障。

**建议**：补别名 `) AS t`；同时补一条 PostgreSQL 后端下的列表接口回归用例。

### R2 [P0] 一致性与服务发现模块未接线

**位置**：`cmd/fsserver/main.go:271`（构造 `consistencyMgr`）、`cmd/fsserver/main.go:235`（构造 `discoverySvc`）

`ConsistencyManager` 在启动时被构造，但全仓库没有任何业务路径调用 `BeforeWrite`、`AfterWrite`、`HandleSyncRequest`、`RegisterReplica`（在 `internal/consistency` 包之外的引用数为 0）。`ServiceDiscovery` 只在启动时 `Register`，从不调用 `Discover` / `Watch`；`EtcdManager` 只构造不使用。

**影响**：需求文档 2.6 与 4.2 声称的"数据一致性保障""服务发现机制"在运行时不存在。这比"未实现"更危险——配置项、代码结构、文档三处都暗示能力已具备，验收时极易被误判为已交付。`docs/design.md` 3.3 的多实例架构图也与之矛盾（架构图只依赖 PostgreSQL + Redis + 共享存储，没有一致性/发现模块）。

**建议**：二选一并明确写进文档——要么接入读写路径并定义语义（仲裁阈值、失败降级行为），要么从启动流程和文档中移除。

### R3 [P0] 存储与元数据之间缺少原子性保障与对账补偿

**位置**：`internal/service/transfer/service.go:336` 起的 `CompleteUpload`（流程为：同步关闭临时文件 → 移动进存储 → 写数据库元数据）

两个写入目标（存储与数据库）之间没有事务或补偿。任一步失败或进程崩溃，都会留下孤儿文件（存储有、元数据无）或孤儿元数据（元数据有、存储无）。

启动清理只删除 `os.TempDir()` 下 `fsserver-uploads-` 前缀的目录（`cmd/fsserver/main.go:183-189`），从不校验存储与数据库的一致性。软删除只改 `is_deleted`，物理文件的实际回收与崩溃后的补偿也没有设计。

**影响**：多实例部署下会累积不可见的存储泄漏与"元数据指向不存在对象"的坏记录，且缺乏发现手段。

**建议**：至少提供一条对账路径（启动期或定期扫描，按 `storage_type` + `updated_at` 做增量核对），并明确崩溃语义写入设计文档。

### R4 [P1] StorageAdapter 接口层次过低且含死方法

**位置**：`internal/storage/local.go:12-29`

接口建模的是"字节级可随机写的文件"，但对象存储不具备该语义：

- `MinIOStorage.WriteAt` 直接返回 `ErrWriteAtUnsupported`（`internal/storage/minio.go:120-130`）
- `storage.WriteAt` 在生产代码中 **0 个调用点**（分段上传写的是本地临时文件，不是 storage）
- `ValidatePath` 在接口中声明，但全仓库 **0 个调用点**
- 设计文档声称接口含 `CleanPathLocks()`，实际接口中没有；该方法在 `LocalStorage`/`MinIOStorage` 上存在，但生产路径从不调用，只有测试调用

**影响**：接口同时携带"后端无法实现的语义"和"无人使用的方法"，调用方必须了解各后端的物理约束才能正确使用，抽象泄漏；测试面与认知负担被放大。

**建议**：把 seam 提升到对象/分片语义（`PutObject` / `GetRange` / `ComposeObject` / `DeletePrefix`），把"本地随机写"降为实现细节；删除死方法，或为每个方法列出真实调用点后再决定保留。

### R5 [P1] StorageAdapter.Exists 吞掉错误

**位置**：`internal/storage/local.go:21`

签名 `Exists(path string) bool` 无法区分"对象不存在"和"后端故障 / 权限错误 / 网络不可达"。调用方会把基础设施故障当成"文件不存在"，进而走覆盖或新建分支。

**影响**：故障以静默数据异常的形式出现，而不是以错误的形式暴露；在 MinIO 后端尤其危险（网络抖动会被解释成对象缺失）。

**建议**：改为 `Exists(path string) (bool, error)`，并逐个调用点明确"不存在"与"故障"的处理分支。

### R6 [P1] CI 缺少真实 PostgreSQL/Redis

**位置**：`.github/workflows/ci.yml`（只执行 `go test -short ./internal/...`）

`internal/pgtest/pgtest.go:64-70` 在 PostgreSQL 不可达时调用 `t.Skipf` 而不是失败。CI 环境中不存在 PostgreSQL，因此所有依赖真实数据库的用例被静默跳过，测试结果显示为全绿。

**影响**：R1 这类只在 PostgreSQL 上出现的缺陷可以长期潜伏。本轮实测（连接真实 PostgreSQL 12.6）一次就暴露了 filelist 整个包的失败。

**建议**：CI 增加 PostgreSQL 与 Redis 服务容器；引入显式开关（如 `FSS_TEST_REQUIRE_INFRA=1`），在 CI 中把"基础设施不可达"从 skip 升级为 fail。

### R7 [P1] 需求、设计、README 与实现三方漂移

详见第 4 节。核心影响是：验收无法以文档为准，也无法以代码为准，评审缺少等价于"规约"的锚点。

### R8 [P2] HTTP 层与持久化耦合

**位置**：`internal/api/http/server.go`（1717 行、45 个 handler）

构造函数 `NewServer`（`internal/api/http/server.go:56`）接收 11 个依赖；部分 handler 通过 service 层调用业务逻辑（27 处），另一部分直接构造持久化服务，例如 `database.NewAuditLogService(s.db)`（`:1022`）与 `database.NewApiKeyService(s.db)`（`:1176`）。

**影响**：API 层同时承担路由、校验、业务编排与持久化细节；接口与测试面被放大，单个文件难以导航与维护。

**建议**：把审计查询、API Key 管理下沉到独立 service，HTTP 层只保留路由、入参校验、响应映射与错误转换。

### R9 [P2] 服务层依赖具体类型

**位置**：`internal/service/filemanager/manager.go:19`（`db *database.DB`）、`internal/service/transfer/service.go:63-78`、`internal/api/http/server.go:56`

服务层对存储用的是接口（正确），对数据库用的是具体结构体。结果是几乎所有测试都必须启动真实数据库，单元层无法独立运行。

**影响**：这是 R1、R6 的根因之一——缺陷只能在集成层暴露，反馈周期长。

**建议**：为服务层需要的数据访问定义窄接口（如 `FileMetadataStore`、`AuditStore`），由 `*database.DB` 实现，测试用内存 fake。

### R10 [P2] 请求上下文未贯穿

**位置**：非测试代码中 `context.Background()` 出现 53 次，典型如 `internal/service/filemanager/manager.go:63`（分布式锁固定 30s 超时）、`internal/service/directory/manager.go:219`（事务用 `context.Background()`）

请求被取消、客户端断开或 deadline 到期后，后台仍会继续持锁、继续写盘、继续提交事务。

**建议**：把 `context.Context` 作为服务的首选参数，从 HTTP/gRPC 入口一路传到锁与事务。

### R11 [P2] SQL 方言翻译采用文本替换

**位置**：`internal/database/dialect.go:40-56`

`Translate` 逐字符把每个 `?` 替换为 `$n`，不理解字符串字面量、转义、`LIKE '%?%'`、以及 PostgreSQL 的 jsonb `?` / `?|` / `?&` 操作符。

**影响**：当前靠"不在 SQL 里写 `?` 字面量"的约定维持，任何引入含 `?` 的 SQL 都会静默产生错误语句。

**建议**：改用占位符感知的改写（跳过引号与注释），或直接按方言生成 SQL，不再做运行时替换。

### R12 [P2] 进程内双层锁与锁序不统一

**位置**：`internal/service/filemanager/manager.go:25`（`fileLocks`）、`internal/storage/local.go:33`（`pathLocks`）、`internal/service/directory/manager.go:64`（`dir:` 前缀锁）

同一个进程内存在两套按路径互斥的 `sync.Map`；文件操作用 `file:` 前缀的分布式锁，目录操作用 `dir:` 前缀，跨层级操作（把文件移入目录、递归删除目录）没有统一锁序。

`FileManager.CleanFileLocks` 有后台定期回收（`cmd/fsserver/main.go:380-390`），但存储层的 `CleanPathLocks` 不在接口中，生产路径从不调用，锁条目只增不减。

**建议**：统一到单一按路径锁表并定义锁序（如按路径字典序加锁）；为存储层锁表提供与接口一致的回收入口。

### R13 [P2] 数据库 schema 定义重复两份

**位置**：`cmd/fsserver/main.go:74-148`（内联 100+ 行建表 SQL，硬编码 PostgreSQL 风格类型）、`internal/database/metadata.go:104` 起（dialect-aware 版本，已标注 Deprecated，但测试仍在使用）

两份 schema 必然漂移，且 `main.go` 中那份绕过了 `dialect` 抽象，与 `internal/database/dialect.go` 的设计意图冲突。

**建议**：schema 单一来源（迁移文件或 `dialect` 生成），删除重复定义。

### R14 [P3] 加密路径全量入内存

**位置**：`internal/api/http/server.go:487-500`

AES-GCM 对整个消息做认证，导致加解密必须把完整文件读入内存，`max_upload_size_mb` 默认 1024。N 个并发加密上传即 N GB 级内存占用。需求文档第 12 项"流式上传/下载加密"尚未实现。

**建议**：改为分块加密（每块独立 nonce + 块内认证），或对加密路径的并发数与文件大小设独立上限。

### R15 [P3] 同机多实例启动清理会误删

**位置**：`cmd/fsserver/main.go:183-189`（启动时删除 `os.TempDir()` 下所有 `fsserver-uploads-*`）

该临时目录是每进程私有（`internal/service/transfer/service.go:81`）。同一台主机启动第二个实例时，会把第一个实例正在进行的上传临时目录删除，导致在传会话失败。

**建议**：按实例 ID 限定清理范围，或只清理超过安全时间阈值（如超过会话 TTL）的目录。

### R16 [P3] 预编译语句缓存错误路径泄漏

**位置**：`internal/database/db.go:66-72`

`Query` 出错时删除缓存条目，但没有调用 `stmt.Close()`。反复失败会泄漏服务端 prepared statement。

**建议**：删除前先 `Close()`，并考虑缓存容量上限。

### R17 [P3] 审计日志异步批写的可见性窗口

**位置**：`internal/database/audit_writer.go`

审计写入是异步批处理（已有取舍说明，属可接受设计），但查询端点直接从数据库读取，缓冲区未刷盘时查不到刚发生的操作。

**建议**：在文档中写明该一致性窗口，或在查询前提供显式 flush 选项。

## 4. 文档与实现漂移清单

### 4.1 文档声称"未实现"但实际已实现

`docs/requirements.md` 第 5 节表格至少 6 项已过时：

| 需求文档条目 | 实际情况 | 证据 |
|---|---|---|
| #16 程序入口 `main.go` 未创建 | 已存在，444 行 | `cmd/fsserver/main.go` |
| #10 gRPC 认证拦截器未实现 | 已实现，含 RBAC 方法级授权 | `internal/api/grpc/server.go:84-105` |
| #11 gRPC 指标采集未实现 | 已实现 | `internal/api/grpc/server.go:106-125` |
| #8 审计日志未持久化 | 已实现（异步批写） | `internal/database/audit_writer.go` |
| #9 目录删除/重命名无 HTTP 端点 | 已实现 | `internal/api/http/server.go:143-144` |
| #13 缓存 Redis 后端部分实现 | 已实现 | `internal/cache/redis.go:17` |

### 4.2 文档声称"已支持"但缺少前提条件

| 文档表述 | 实际前提 |
|---|---|
| README / `docs/design.md`：Redis 会话存储"保证任意实例可恢复" | 默认临时目录是进程私有的，跨实例恢复必须在 `storage.temp_dir` 配置共享卷；该前提只写在代码注释里（`internal/service/transfer/service.go:132-142`） |
| `docs/design.md:172`：MinIO `WriteAt` "限制 512MB 以内（需读取全量数据再回写）" | 实现已改为直接拒绝（`internal/storage/minio.go:120-130`） |
| `docs/design.md:2.4` 声称接口含 `CleanPathLocks()` | 实际接口中没有该方法 |

### 4.3 结论

当前状态下"是否符合设计"无法判定。**建议把"重建文档 = 验收基线"作为整改的第一步**，并约定：需求状态变更必须与代码同一次提交完成。

## 5. 实测证据

环境：WSL2 + Go 1.25.14 + PostgreSQL 12.6（Windows 宿主机，端口 5555）。

命令：

```bash
export FSS_TEST_PG_HOST=$(ip route show default | awk '{print $3}')
export FSS_TEST_PG_PORT=5555 FSS_TEST_PG_NAME=fsserver FSS_TEST_PG_USER=fsserver
export FSS_TEST_PG_PASS=fsserver123 FSS_TEST_PG_SSLMODE=disable
go test ./internal/... -count=1
```

结果：12 个包 `ok`，`internal/service/filelist` 失败，14 个用例报错，错误信息全部为同一条：

```
pq: FROM 中的子查询必须有一个别名 at column 22 (42601)
```

同一套代码在 `-short` 模式（CI 使用的模式）下显示全绿，因为 PostgreSQL 不可达时用例走 `t.Skipf`。

## 6. 建议的整改顺序

1. **定性未接线模块**（R2）：接入读写路径或移除，并同步文档。当前三方不一致的状态维护成本最高。
2. **补齐一致性与对账**（R3）：明确崩溃语义，提供存储/元数据对账机制。
3. **修复 R1 并建立回归网**（R6）：CI 接入真实 PostgreSQL 与 Redis，把 skip 升级为 fail。
4. **重画存储 seam**（R4、R5）：提升到对象语义，`Exists` 返回错误。
5. **重建文档基线**（R7）：需求文档与代码同提交更新。
6. **结构性收敛**（R8、R9、R10、R12、R13）：分层、接口化、上下文贯穿、schema 单一来源。
7. **低优先级项**（R11、R14-R17）按迭代排期。
