# 代码审查问题跟踪 (Issues)

> 代码审查发现的 10 个主要问题 + 4 个细节项，已全部记录到 GitHub Issues 并修复关闭。
> 提交代码时在 commit message 中引用编号（如 `fix(security): closes #55`）。
>
> 第二轮设计评审（2026-09-11）的发现见文末"第二轮评审登记表"，完整报告见
> [docs/design-review-20260911.md](docs/design-review-20260911.md)。

## GitHub Issue 编号映射

| 本地编号 | GitHub Issue | 标题 |
|---------|-------------|------|
| #1  | [#55](https://github.com/sosoxu/fssvrgo/issues/55) | [P0][Security] 认证默认关闭 + CORS 允许所有源 |
| #2  | [#56](https://github.com/sosoxu/fssvrgo/issues/56) | [P0][Security] 分布式锁无续期，长操作易被并发抢占 |
| #3  | [#57](https://github.com/sosoxu/fssvrgo/issues/57) | [P1][Integrity] 分段上传缺少字节区间完整性校验 |
| #4  | [#58](https://github.com/sosoxu/fssvrgo/issues/58) | [P1][Security] gRPC 缺少 RBAC 角色校验 |
| #5  | [#59](https://github.com/sosoxu/fssvrgo/issues/59) | [P2][Security] AES 密钥直接使用 passphrase 截断 |
| #6  | [#60](https://github.com/sosoxu/fssvrgo/issues/60) | [P2][Security] JWT 刷新令牌可重放 |
| #7  | [#61](https://github.com/sosoxu/fssvrgo/issues/61) | [P2][Security] API Key 与 JWT 认证耦合 + 身份伪造 |
| #8  | [#62](https://github.com/sosoxu/fssvrgo/issues/62) | [P2][Feature] 列表接口缺少 recursive 参数 |
| #9  | [#63](https://github.com/sosoxu/fssvrgo/issues/63) | [P2][Security] 上传会话无配额上限（DoS 风险） |
| #10 | [#64](https://github.com/sosoxu/fssvrgo/issues/64) | [P3][Audit] 审计日志未记录真实调用者 |
| 细节 | [#65](https://github.com/sosoxu/fssvrgo/issues/65) | [P3][Hardening] 多项安全加固细节项 |

## P0 - 严重安全问题

### #1 认证默认关闭 + CORS 允许所有源 (GitHub #55)
- **状态**: 已修复
- **文件**: `config.yaml`, `internal/api/http/server.go`
- **问题**: `auth.enabled` 默认为 `false`，任何客户端可匿名访问；`cors_allowed_origins` 默认 `"*"` 允许任意跨域。
- **修复**: `config.yaml` 设置 `auth.enabled: true`、`cors_allowed_origins: ""`；`corsMiddleware` 仅在显式配置时发送 CORS 头。

### #2 分布式锁无续期，长操作易被并发抢占 (GitHub #56)
- **状态**: 已修复
- **文件**: `internal/distributed/lock.go`, `internal/service/filemanager/manager.go`
- **问题**: 10s TTL 锁在大文件上传期间过期，其他实例可获取锁导致并发写冲突。
- **修复**: 新增 `AcquireLockWithRenewal`，后台 goroutine 每 TTL/3 续期；`UploadFile`/`UploadFileFromReader` 使用续期锁。

## P1 - 重要问题

### #3 分段上传缺少字节区间完整性校验 (GitHub #57)
- **状态**: 已修复
- **文件**: `internal/service/transfer/multipart.go`
- **问题**: 仅靠 `uploadedSize` 计数无法检测重复/重叠上传留下的零填充间隙。
- **修复**: 新增 `byteRange`/`coveredRanges` 跟踪已写字节区间；`addCoveredRange` 合并排序区间；`CompleteMultipartUpload` 调用 `isFullyCovered` 校验 `[0, TotalSize)` 完整覆盖。

### #4 gRPC 缺少 RBAC 角色校验 (GitHub #58)
- **状态**: 已修复
- **文件**: `internal/api/grpc/server.go`
- **问题**: gRPC 拦截器仅认证不授权，任何已认证用户可调用所有 RPC（包括管理操作）。
- **修复**: 新增 `methodPermissions` 映射表（按 proto `FullMethodName` 映射），`authorize` 方法按方法校验角色，拦截器先认证后授权。

## P2 - 中等问题

### #5 AES 密钥直接使用 passphrase 截断 (GitHub #59)
- **状态**: 已修复
- **文件**: `internal/crypto/crypto.go`
- **问题**: `Init` 直接取 passphrase 前 32 字节作为 AES-256 密钥，短密码导致弱密钥。
- **修复**: 使用 scrypt（N=32768, r=8, p=1）派生 32 字节密钥。

### #6 JWT 刷新令牌可重放 (GitHub #60)
- **状态**: 已修复
- **文件**: `internal/auth/jwt.go`
- **问题**: Refresh token 未一次性轮换，被盗令牌可无限刷新。
- **修复**: `GenerateTokenPair` 为 access/refresh 生成唯一 `jti`；`RefreshToken` 将旧 refresh 的 jti 加入黑名单；`ValidateToken` 检查黑名单；后台 `cleanupBlacklist` 清理过期条目。

### #7 API Key 与 JWT 认证耦合 + 身份伪造 (GitHub #61)
- **状态**: 已修复
- **文件**: `internal/auth/auth.go`, `internal/api/http/server.go`
- **问题**: `ValidateApiKey` 回退到 JWT 验证导致逻辑混乱；`handleGenerateToken` 允许非 admin 指定任意用户身份。
- **修复**: 移除 `ValidateApiKey`/`GetUserByApiKey` 的 JWT 回退；`authMiddleware` 增加 JWT 独立验证路径；`handleGenerateToken` 非 admin 强制使用真实身份。

### #8 列表接口缺少 recursive 参数 (GitHub #62)
- **状态**: 已修复
- **文件**: `internal/api/http/server.go`, `internal/service/filelist/service.go`
- **问题**: `/api/v1/files` 无法控制是否递归列出子目录。
- **修复**: `handleList` 解析 `recursive` 参数并传递给 `ListFiles`。

### #9 上传会话无配额上限（DoS 风险） (GitHub #63)
- **状态**: 已修复
- **文件**: `internal/service/transfer/service.go`, `internal/service/transfer/multipart.go`
- **问题**: 无限并发上传会话可耗尽内存/磁盘/文件描述符。
- **修复**: `FileTransferService` 新增 `maxSessions`（默认 1000）和 `sessionCount`（atomic）；`acquireSessionSlot`/`releaseSessionSlot` 管理配额；`CreateUploadSession`/`CreateMultipartUpload` 获取槽位，所有终止路径（成功/错误/中止）释放槽位。

## P3 - 建议改进

### #10 审计日志未记录真实调用者 (GitHub #64)
- **状态**: 已修复
- **文件**: `internal/api/http/server.go`
- **问题**: `auditLog` 方法未从 context 获取已认证用户，审计记录缺少操作主体。
- **修复**: `auditLog` 从 gin context 读取 `user` 值（`*auth.User`），记录 `user:<ID>` 而非 `api-key:***`/`bearer:***`。

### 细节项 (GitHub #65)
- **状态**: 已修复
- `fileLocks` 内存清理：`main.go` 新增后台 goroutine 每 10 分钟调用 `fm.CleanFileLocks()`
- `Content-Disposition` 转义：`server.go` 下载响应头转义双引号并添加 `filename*=UTF-8''` URL 编码
- 符号链接绕过 `ValidatePath`：`storage/local.go` 添加 `EvalSymlinks` 解析 + `resolveExistingAncestor` 处理新文件路径
- JWT secret 注释修正：`auth.go` 注释准确描述密钥复用情况并建议域分离改进

---

# 第二轮评审登记表（2026-09-11）

> 基线：`a4abea6`（分支 `review/acceptance-20260911`）
> 评审报告：[docs/design-review-20260911.md](docs/design-review-20260911.md)
> 方法：文档-实现对照走查 + 全仓取证 + 真库实测（Go 1.25.14 + PostgreSQL 12.6）
> GitHub Issue：已创建，编号 **#106 - #122**（下表均已回填链接）。

## 登记表

> **状态更新（2026-09-12）**：17 条中 13 条已修复并推送到 `origin/review/acceptance-20260911`；
> 剩余 R8、R10、R12、R14 仍待处理。
> 验证口径：`go build ./...`、`go vet ./...` 通过；`go test -count=1 ./internal/...`
> 连 PostgreSQL 12.6 全绿且 `internal/service/*` 无 SKIP。

| 编号 | 级别 | 标题 | 证据位置 | GitHub Issue | 状态 |
|---|---|---|---|---|---|
| R1 | P0 | filelist 计数查询缺子查询别名，PostgreSQL 上必然失败 | `internal/database/filelist_store.go:90`（SQL 已下沉） | [#106](https://github.com/sosoxu/fssvrgo/issues/106) | 已修复 `1b28ff4` |
| R2 | P0 | 一致性与服务发现模块未接线（死代码） | `cmd/fsserver/main.go:235,271` | [#107](https://github.com/sosoxu/fssvrgo/issues/107) | 已修复 `d8fc23a` |
| R3 | P0 | 存储与元数据之间缺少原子性保障与对账补偿 | `internal/service/transfer/service.go:336` | [#108](https://github.com/sosoxu/fssvrgo/issues/108) | 已修复 `8d4d880` |
| R4 | P1 | `StorageAdapter` 接口层次过低且含死方法 | `internal/storage/local.go:12` | [#109](https://github.com/sosoxu/fssvrgo/issues/109) | 已修复 `8ba90cf` |
| R5 | P1 | `StorageAdapter.Exists` 吞掉错误 | `internal/storage/local.go:21` | [#110](https://github.com/sosoxu/fssvrgo/issues/110) | 已修复 `326f816` |
| R6 | P1 | CI 无真实 PostgreSQL/Redis，跳过而非失败，掩盖缺陷 | `.github/workflows/ci.yml` | [#111](https://github.com/sosoxu/fssvrgo/issues/111) | 已修复 `8886c00` |
| R7 | P1 | 需求、设计、README 与实现三方漂移 | `docs/requirements.md` 第 5 节 | [#112](https://github.com/sosoxu/fssvrgo/issues/112) | 已修复 `e6d2d56` |
| R8 | P2 | HTTP 层与持久化耦合，单文件 1717 行 | `internal/api/http/server.go:56,1022,1176` | [#113](https://github.com/sosoxu/fssvrgo/issues/113) | 待处理 |
| R9 | P2 | 服务层依赖具体 `*database.DB`，单元测试必须起真库 | `internal/service/filemanager/manager.go:41`（构造注入） | [#114](https://github.com/sosoxu/fssvrgo/issues/114) | 已修复 `194538c`、`f5cc5dc`、`fd6f580`、`b393887` |
| R10 | P2 | 请求上下文未贯穿（53 处 `context.Background()`） | `internal/service/filemanager/manager.go:63` | [#115](https://github.com/sosoxu/fssvrgo/issues/115) | 待处理 |
| R11 | P2 | SQL 方言翻译采用文本替换，语义不安全 | `internal/database/dialect.go:40` | [#116](https://github.com/sosoxu/fssvrgo/issues/116) | 已修复 `762e3e3` |
| R12 | P2 | 进程内双层锁与锁序不统一，存储层锁表无回收 | `internal/storage/local.go:33`、`internal/service/directory/manager.go:64` | [#117](https://github.com/sosoxu/fssvrgo/issues/117) | 待处理 |
| R13 | P2 | 数据库 schema 定义重复两份 | `cmd/fsserver/main.go:74`、`internal/database/metadata.go:104` | [#118](https://github.com/sosoxu/fssvrgo/issues/118) | 已修复 `b298f18` |
| R14 | P3 | 加密路径全量入内存，并发下有内存放大 | `internal/api/http/server.go:487` | [#119](https://github.com/sosoxu/fssvrgo/issues/119) | 待处理 |
| R15 | P3 | 同机多实例启动清理会误删其它实例临时目录 | `cmd/fsserver/main.go:183` | [#120](https://github.com/sosoxu/fssvrgo/issues/120) | 已修复 `adb88cf` |
| R16 | P3 | 预编译语句缓存错误路径泄漏 | `internal/database/db.go:66` | [#121](https://github.com/sosoxu/fssvrgo/issues/121) | 已修复 `f59f017` |
| R17 | P3 | 审计日志异步批写的可见性窗口 | `internal/database/audit_writer.go:15` | [#122](https://github.com/sosoxu/fssvrgo/issues/122) | 已修复 `8ba910c` |

## 本轮已实测确认的失败

`go test ./internal/... -count=1`（连 PostgreSQL 12.6，非 `-short`）：
**12 个包通过，`internal/service/filelist` 失败**，14 个用例报同一条错误：

```
pq: FROM 中的子查询必须有一个别名 at column 22 (42601)
```

对应 R1（[#106](https://github.com/sosoxu/fssvrgo/issues/106)）。该缺陷在 CI 的 `-short` 模式下被 `t.Skipf` 掩盖。

**已修复**（`1b28ff4`）：COUNT 子查询补上 `AS t` 别名；随后 filelist 的 SQL 整体下沉到
`internal/database/filelist_store.go`（`194538c`）。2026-09-12 复测 `go test -count=1
./internal/...`（PostgreSQL 12.6）全部通过，`internal/service/*` 无 SKIP。
