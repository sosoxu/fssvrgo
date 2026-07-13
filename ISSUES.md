# 代码审查问题跟踪 (Issues)

> 代码审查发现的 10 个主要问题 + 4 个细节项，已全部记录到 GitHub Issues 并修复关闭。
> 提交代码时在 commit message 中引用编号（如 `fix(security): closes #55`）。

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
