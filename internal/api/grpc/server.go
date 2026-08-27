package grpc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/metrics"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/utils"
	pb "github.com/sosoxu/fssvrgo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// contextKey is an unexported type used to scope context values in this
// package, avoiding collisions with keys defined by other packages.
type contextKey string

const userContextKey contextKey = "authUser"

// methodPermissions maps gRPC full method names to the (resource, action)
// pair required to invoke them. Methods absent from this map are allowed by
// default to preserve backward compatibility.
var methodPermissions = map[string]struct{ resource, action string }{
	"/fsserver.FileService/UploadFile":      {"files", "write"},
	"/fsserver.FileService/DownloadFile":    {"files", "read"},
	"/fsserver.FileService/ListFiles":       {"files", "read"},
	"/fsserver.FileService/DeleteFile":      {"files", "write"},
	"/fsserver.FileService/RenameFile":      {"files", "write"},
	"/fsserver.FileService/CreateDirectory": {"files", "write"},
	"/fsserver.FileService/DeleteDirectory": {"files", "write"},
	"/fsserver.FileService/RenameDirectory": {"files", "write"},
	"/fsserver.FileService/GetMetadata":     {"files", "read"},
}

type Server struct {
	pb.UnimplementedFileServiceServer
	config      config.ServerConfig
	grpcServer  *grpc.Server
	fm          *filemanager.FileManager
	dirSvc      *directory.DirectoryManager
	flSvc       *filelist.FileListService
	transferSvc *transfer.FileTransferService
	authSvc     *auth.AuthService
	cryptoSvc   *crypto.CryptoService
	metricsSvc  *metrics.Metrics
	auditWriter *database.AuditWriter
}

func NewServer(cfg config.ServerConfig, fm *filemanager.FileManager, dirSvc *directory.DirectoryManager, flSvc *filelist.FileListService, transferSvc *transfer.FileTransferService, authSvc *auth.AuthService, cryptoSvc *crypto.CryptoService, metricsSvc *metrics.Metrics, db ...*database.DB) *Server {
	s := &Server{
		config:      cfg,
		fm:          fm,
		dirSvc:      dirSvc,
		flSvc:       flSvc,
		transferSvc: transferSvc,
		authSvc:     authSvc,
		cryptoSvc:   cryptoSvc,
		metricsSvc:  metricsSvc,
	}
	if len(db) > 0 && db[0] != nil {
		s.auditWriter = database.NewAuditWriter(db[0], 100, time.Second)
	}

	s.grpcServer = grpc.NewServer(
		grpc.ChainUnaryInterceptor(s.unaryMetricsInterceptor, s.unaryAuditInterceptor, s.unaryAuthInterceptor),
		grpc.ChainStreamInterceptor(s.streamMetricsInterceptor, s.streamAuditInterceptor, s.streamAuthInterceptor),
	)
	pb.RegisterFileServiceServer(s.grpcServer, s)
	return s
}

func (s *Server) unaryAuditInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	s.auditRPC(ctx, info.FullMethod, err)
	return resp, err
}

func (s *Server) streamAuditInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	s.auditRPC(ss.Context(), info.FullMethod, err)
	return err
}

func (s *Server) auditRPC(ctx context.Context, method string, err error) {
	if s.auditWriter == nil {
		return
	}
	clientIP := ""
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		clientIP = p.Addr.String()
	}
	userIdentifier := "anonymous"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md.Get("x-api-key")) > 0 {
			userIdentifier = "api-key:***"
		} else if len(md.Get("authorization")) > 0 {
			userIdentifier = "authorization:***"
		}
	}
	code := status.Code(err)
	s.auditWriter.Submit(&database.AuditLog{
		ID:             utils.GenerateUUID(),
		Timestamp:      utils.GetCurrentTimestamp(),
		Operation:      "grpc_request",
		ResourcePath:   method,
		UserIdentifier: userIdentifier,
		ClientIP:       clientIP,
		UserAgent:      "grpc",
		Success:        err == nil,
		Details:        "code=" + code.String(),
	})
}

func (s *Server) unaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *Server) streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := s.authenticate(ss.Context())
	if err != nil {
		return err
	}
	if err := s.authorize(ctx, info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *Server) unaryMetricsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if s.metricsSvc != nil {
		start := time.Now()
		resp, err := handler(ctx, req)
		s.metricsSvc.RecordGRPCRequest(info.FullMethod, status.Code(err).String(), time.Since(start))
		return resp, err
	}
	return handler(ctx, req)
}

func (s *Server) streamMetricsInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if s.metricsSvc != nil {
		start := time.Now()
		err := handler(srv, ss)
		s.metricsSvc.RecordGRPCRequest(info.FullMethod, status.Code(err).String(), time.Since(start))
		return err
	}
	return handler(srv, ss)
}

func (s *Server) authenticate(ctx context.Context) (context.Context, error) {
	if s.authSvc == nil {
		return ctx, nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		if !s.authSvc.ValidateApiKey("") {
			return ctx, status.Error(codes.Unauthenticated, "authentication required")
		}
		return ctx, nil
	}

	apiKeys := md.Get("x-api-key")
	if len(apiKeys) > 0 && apiKeys[0] != "" {
		if s.authSvc.ValidateApiKey(apiKeys[0]) {
			return s.withUser(ctx, apiKeys[0]), nil
		}
		return ctx, status.Error(codes.Unauthenticated, "invalid API key")
	}

	authHeaders := md.Get("authorization")
	for _, ah := range authHeaders {
		if strings.HasPrefix(ah, "Bearer ") {
			token := strings.TrimPrefix(ah, "Bearer ")
			if s.authSvc.ValidateApiKey(token) {
				return s.withUser(ctx, token), nil
			}
			// After Bearer token API key check fails, try JWT
			if s.authSvc.GetJWTService() != nil {
				claims, err := s.authSvc.GetJWTService().ValidateToken(token)
				// 只接受 access token（或无类型标记的兼容旧 token），拒绝 refresh token
				// 被当作 access token 使用，与 HTTP 处理器行为一致。
				if err == nil && claims != nil && (claims.TokenType == "" || claims.TokenType == "access") {
					return s.withUser(ctx, token), nil
				}
			}
			return ctx, status.Error(codes.Unauthenticated, "invalid token")
		}
		if strings.HasPrefix(ah, "Api-Key ") {
			key := strings.TrimPrefix(ah, "Api-Key ")
			if s.authSvc.ValidateApiKey(key) {
				return s.withUser(ctx, key), nil
			}
			return ctx, status.Error(codes.Unauthenticated, "invalid API key")
		}
	}

	if !s.authSvc.ValidateApiKey("") {
		return ctx, status.Error(codes.Unauthenticated, "authentication required")
	}
	return ctx, nil
}

// withUser resolves the *auth.User for the given API key/token and stores it
// in the context for downstream RBAC checks. If the user cannot be resolved
// the original context is returned unchanged.
func (s *Server) withUser(ctx context.Context, apiKey string) context.Context {
	if s.authSvc == nil {
		return ctx
	}
	if user := s.authSvc.GetUserByApiKey(apiKey); user != nil {
		return context.WithValue(ctx, userContextKey, user)
	}
	return ctx
}

// authorize enforces method-level RBAC. It reads the *auth.User stored by
// authenticate from the context and checks whether the user's role permits
// the (resource, action) required by the given gRPC method. Methods absent
// from methodPermissions are allowed by default. When auth is disabled or
// authSvc is nil, all methods are allowed.
func (s *Server) authorize(ctx context.Context, method string) error {
	if s.authSvc == nil {
		return nil
	}
	// Auth disabled: allow all.
	if s.authSvc.ValidateApiKey("") {
		return nil
	}

	perm, ok := methodPermissions[method]
	if !ok {
		// Methods not in the map are allowed by default (backward compat).
		return nil
	}

	val := ctx.Value(userContextKey)
	user, ok := val.(*auth.User)
	if !ok || user == nil || !user.Enabled {
		return status.Error(codes.PermissionDenied, "permission denied: no authenticated user")
	}

	// admin role can access all methods.
	if user.Role == "admin" {
		return nil
	}
	// user role can only access files read/write.
	if user.Role == "user" && perm.resource == "files" && (perm.action == "read" || perm.action == "write") {
		return nil
	}
	return status.Error(codes.PermissionDenied, "permission denied")
}

func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.config.GRPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	logger.Info("gRPC server listening on %s", addr)

	if err := s.grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("gRPC server error: %w", err)
	}

	return nil
}

func (s *Server) Stop() {
	if s.grpcServer != nil {
		logger.Info("Stopping gRPC server (graceful)...")
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			logger.Info("gRPC server stopped gracefully")
		case <-time.After(10 * time.Second):
			logger.Warn("gRPC server graceful stop timed out, forcing stop")
			s.grpcServer.Stop()
		}
	}
	if s.auditWriter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.auditWriter.Close(ctx); err != nil {
			logger.Warn("failed to flush gRPC audit logs: %v", err)
		}
	}
}

func (s *Server) UploadFile(stream grpc.ClientStreamingServer[pb.UploadRequest, pb.UploadResponse]) error {
	if s.metricsSvc != nil {
		s.metricsSvc.IncActiveUploads()
		defer s.metricsSvc.DecActiveUploads()
	}
	// 读取元数据（必须为第一条消息）
	req, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive metadata: %w", err)
	}
	metaMsg, ok := req.Data.(*pb.UploadRequest_Metadata)
	if !ok {
		return fmt.Errorf("metadata must be sent first in upload stream")
	}
	meta := metaMsg.Metadata
	if !utils.IsValidFilePath(meta.Path) {
		return status.Error(codes.InvalidArgument, "invalid file path")
	}
	if !utils.IsValidFileName(meta.Name) {
		return status.Error(codes.InvalidArgument, "invalid file name")
	}
	// 校验 TotalSize：防止超大文件预分配耗尽磁盘，与 HTTP handleUpload 一致。
	maxUploadSize := int64(s.config.MaxUploadSizeMB) * 1024 * 1024
	if meta.TotalSize <= 0 {
		return status.Error(codes.InvalidArgument, "total_size must be positive")
	}
	if maxUploadSize > 0 && meta.TotalSize > maxUploadSize {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("total_size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
	}

	// 读取第一个分块
	req, err = stream.Recv()
	if err == io.EOF {
		return fmt.Errorf("no chunk received in upload stream")
	}
	if err != nil {
		return fmt.Errorf("failed to receive first chunk: %w", err)
	}
	chunkMsg, ok := req.Data.(*pb.UploadRequest_Chunk)
	if !ok {
		return fmt.Errorf("expected chunk after metadata")
	}
	firstChunk := chunkMsg.Chunk

	// 探测下一条消息。若为 EOF 且首块从 offset 0 开始覆盖整个文件，则为
	// 单分块完整上传（小文件常见场景），走快速路径直接调用 FileManager，
	// 绕过 transferSvc 的会话机制。会话机制为断点续传大文件设计，对 1KB
	// 小文件会产生大量额外开销：临时文件创建/预分配/fsync/重命名/删除、
	// 会话级分布式锁（额外 Redis 往返）、会话 Redis 存取、冗余元数据查询。
	// 绕过后 Redis 操作从 ~6 降至 ~2，磁盘操作从 ~6 降至 1，无 fsync。
	//
	// 大小守卫：仅当单块大小不超过 grpcFastPathMaxSize 时才走快速路径，
	// 否则继续走会话机制（基于临时文件，内存占用受控）。当前 gRPC 默认
	// 单消息上限 4MB 已隐式约束，但显式守卫可防御未来调大 MaxRecvMsgSize
	// 的配置变更导致大文件全量驻留内存。
	nextReq, nextErr := stream.Recv()
	if nextErr == io.EOF && firstChunk.Offset == 0 && int64(len(firstChunk.Data)) == meta.TotalSize && int64(len(firstChunk.Data)) <= grpcFastPathMaxSize && (s.cryptoSvc == nil || !s.cryptoSvc.IsEnabled()) {
		return s.uploadFileFastPath(stream, meta, firstChunk.Data)
	}

	// 多分块上传：走会话机制（断点续传大文件场景），需回放已消费的首块和探测消息
	sessionID, err := s.transferSvc.CreateUploadSession(meta.Path, meta.Name, meta.TotalSize, "", meta.Hash)
	if err != nil {
		return fmt.Errorf("failed to create upload session: %w", err)
	}

	// 回放第一个分块
	if err := s.transferSvc.UploadChunk(sessionID, firstChunk.Data, firstChunk.Offset); err != nil {
		s.transferSvc.AbortUpload(sessionID)
		return fmt.Errorf("failed to upload chunk: %w", err)
	}

	// 回放探测时读取的消息（若有且为分块）
	if nextErr == nil {
		if rc, ok := nextReq.Data.(*pb.UploadRequest_Chunk); ok {
			if err := s.transferSvc.UploadChunk(sessionID, rc.Chunk.Data, rc.Chunk.Offset); err != nil {
				s.transferSvc.AbortUpload(sessionID)
				return fmt.Errorf("failed to upload chunk: %w", err)
			}
		}
	} else if nextErr != io.EOF {
		s.transferSvc.AbortUpload(sessionID)
		return fmt.Errorf("failed to receive upload request: %w", nextErr)
	}

	// 继续读取剩余分块
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.transferSvc.AbortUpload(sessionID)
			return fmt.Errorf("failed to receive upload request: %w", err)
		}
		cm, ok := req.Data.(*pb.UploadRequest_Chunk)
		if !ok {
			s.transferSvc.AbortUpload(sessionID)
			return fmt.Errorf("expected chunk in multi-chunk upload")
		}
		if err := s.transferSvc.UploadChunk(sessionID, cm.Chunk.Data, cm.Chunk.Offset); err != nil {
			s.transferSvc.AbortUpload(sessionID)
			return fmt.Errorf("failed to upload chunk: %w", err)
		}
	}

	result, err := s.transferSvc.CompleteUpload(sessionID)
	if err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}
	if s.metricsSvc != nil {
		s.metricsSvc.RecordUpload(float64(result.UploadedSize))
	}

	createdAt, _ := parseTimestamp(result.CreatedAt)

	return stream.SendAndClose(&pb.UploadResponse{
		Id:        result.FileID,
		Path:      result.FilePath,
		Name:      result.FileName,
		Size:      result.UploadedSize,
		Hash:      result.FileHash,
		CreatedAt: createdAt,
	})
}

// grpcFastPathMaxSize is the upper bound on a single chunk for the fast path.
// Uploads whose single chunk exceeds this fall back to the session-based
// path (temp file on disk) to keep memory bounded. Set above gRPC's default
// per-message limit (4MB) so legitimate single-chunk uploads under the
// default config still take the fast path, but a future bump of
// MaxRecvMsgSize will not let large files fill memory unguarded.
const grpcFastPathMaxSize = 16 * 1024 * 1024 // 16 MB

// uploadFileFastPath 处理单分块完整上传（小文件），直接调用 FileManager，
// 绕过 transferSvc 的会话机制。若客户端提供了 hash 则先校验再写入，保持
// 与会话路径一致的完整性校验语义。
func (s *Server) uploadFileFastPath(stream grpc.ClientStreamingServer[pb.UploadRequest, pb.UploadResponse], meta *pb.UploadMetadata, data []byte) error {
	if meta.Hash != "" {
		computed := fmt.Sprintf("%x", sha256.Sum256(data))
		if computed != meta.Hash {
			return status.Error(codes.InvalidArgument, fmt.Sprintf("hash mismatch: expected %s, got %s", meta.Hash, computed))
		}
	}
	fileMeta, err := s.fm.UploadFile(meta.Path, data)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	if s.metricsSvc != nil {
		s.metricsSvc.RecordUpload(float64(fileMeta.Size))
	}
	createdAt, _ := parseTimestamp(fileMeta.CreatedAt)
	return stream.SendAndClose(&pb.UploadResponse{
		Id:        fileMeta.ID,
		Path:      fileMeta.Path,
		Name:      fileMeta.Name,
		Size:      fileMeta.Size,
		Hash:      fileMeta.Hash,
		CreatedAt: createdAt,
	})
}

func (s *Server) DownloadFile(req *pb.DownloadRequest, stream grpc.ServerStreamingServer[pb.DownloadResponse]) error {
	if !utils.IsValidFilePath(req.Path) {
		return status.Error(codes.InvalidArgument, "invalid file path")
	}
	meta, releaseRead, err := s.fm.BeginRead(req.Path)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}
	defer releaseRead()

	chunkSize := int(req.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = 1024 * 1024 // 1MB default
	}
	// 上限保护：防止客户端传超大 chunkSize 触发 make([]byte, size) 的 2GB 内存分配
	// 导致 OOM，与 HTTP handleRangeDownload 的 32MB 上限一致。
	const maxDownloadChunkSize = 32 * 1024 * 1024
	if chunkSize > maxDownloadChunkSize {
		chunkSize = maxDownloadChunkSize
	}

	var offset int64
	if req.Offset > 0 {
		offset = req.Offset
	}

	var encryptedSessionID string
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		encryptedSessionID, err = s.transferSvc.CreateDownloadSession(req.Path, "")
		if err != nil {
			return fmt.Errorf("failed to create encrypted download session: %w", err)
		}
		defer s.transferSvc.CompleteDownload(encryptedSessionID)
	}

	for offset < meta.Size {
		var data []byte
		if encryptedSessionID != "" {
			data, err = s.transferSvc.DownloadChunk(encryptedSessionID, chunkSize, offset)
		} else {
			data, err = s.fm.DownloadFileDataAt(meta, chunkSize, offset)
		}
		if err != nil {
			return fmt.Errorf("failed to read file chunk: %w", err)
		}
		if len(data) == 0 {
			break
		}

		if err := stream.Send(&pb.DownloadResponse{
			Data:      data,
			Offset:    offset,
			TotalSize: meta.Size,
		}); err != nil {
			return fmt.Errorf("failed to send download chunk: %w", err)
		}

		offset += int64(len(data))
	}

	return nil
}

func (s *Server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	// gRPC clients depend on the Total field in ListFilesResponse (the proto
	// has no include_total/has_more toggle), so we always compute the exact
	// total here to preserve backward compatibility. HTTP clients that only
	// need pagination can use the ListFiles path via the REST API instead.
	page := int(req.Page)
	pageSize := int(req.PageSize)
	// 上限保护：防止客户端传超大 pageSize 触发海量行返回导致 OOM，
	// 与 HTTP handleList 的 maxPageSize 上限一致。
	if maxPageSize := s.config.MaxPageSize; maxPageSize > 0 && pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	result, err := s.flSvc.ListFilesWithTotal(req.Path, req.Recursive, page, pageSize, req.SortBy, req.SortOrder)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	items := make([]*pb.FileInfo, 0, len(result.Items))
	for _, item := range result.Items {
		createdAt, _ := parseTimestamp(item.CreatedAt)
		items = append(items, &pb.FileInfo{
			Id:        item.ID,
			Path:      item.Path,
			Name:      item.Name,
			Size:      item.Size,
			Type:      item.Type,
			CreatedAt: createdAt,
		})
	}

	return &pb.ListFilesResponse{
		Total:    int32(result.Total),
		Page:     int32(result.Page),
		PageSize: int32(result.PageSize),
		Items:    items,
	}, nil
}

// internalError 记录详细错误到日志，返回通用消息给客户端，避免泄漏 SQL 错误、
// 文件路径、堆栈线索等内部信息。op 用于日志标识操作类型。
func internalError(op string, err error) error {
	logger.Error("gRPC %s failed: %v", op, err)
	return status.Error(codes.Internal, "internal server error")
}

func (s *Server) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid file path")
	}
	if err := s.fm.DeleteFile(req.Path); err != nil {
		return nil, internalError("DeleteFile", err)
	}
	return &pb.DeleteFileResponse{Success: true, Message: "File deleted successfully"}, nil
}

func (s *Server) RenameFile(ctx context.Context, req *pb.RenameFileRequest) (*pb.RenameFileResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "new_name is required")
	}
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid file path")
	}
	if !utils.IsValidFileName(req.NewName) {
		return nil, status.Error(codes.InvalidArgument, "invalid new name")
	}
	if err := s.fm.RenameFile(req.Path, req.NewName); err != nil {
		return nil, internalError("RenameFile", err)
	}
	return &pb.RenameFileResponse{Success: true, Message: "File renamed successfully"}, nil
}

func (s *Server) CreateDirectory(ctx context.Context, req *pb.CreateDirectoryRequest) (*pb.CreateDirectoryResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid directory path")
	}
	if err := s.dirSvc.CreateDirectory(req.Path); err != nil {
		return nil, internalError("CreateDirectory", err)
	}
	return &pb.CreateDirectoryResponse{Success: true, Message: "Directory created successfully"}, nil
}

func (s *Server) GetMetadata(ctx context.Context, req *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid path")
	}
	// Try to get file metadata first
	fileMeta, err := s.fm.GetFileMetadata(req.Path)
	if err == nil && fileMeta != nil {
		createdAt, _ := parseTimestamp(fileMeta.CreatedAt)
		updatedAt, _ := parseTimestamp(fileMeta.UpdatedAt)
		return &pb.GetMetadataResponse{
			Id:          fileMeta.ID,
			Path:        fileMeta.Path,
			Name:        fileMeta.Name,
			Size:        fileMeta.Size,
			Type:        "file",
			Hash:        fileMeta.Hash,
			StorageType: fileMeta.StorageType,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
		}, nil
	}

	// Try directory metadata
	dirMeta, err := s.dirSvc.GetDirectoryMetadata(req.Path)
	if err == nil && dirMeta != nil {
		createdAt, _ := parseTimestamp(dirMeta.CreatedAt)
		updatedAt, _ := parseTimestamp(dirMeta.UpdatedAt)
		return &pb.GetMetadataResponse{
			Id:          dirMeta.ID,
			Path:        dirMeta.Path,
			Name:        dirMeta.Name,
			Size:        0,
			Hash:        "",
			Type:        "directory",
			StorageType: "",
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
		}, nil
	}

	return nil, status.Error(codes.NotFound, fmt.Sprintf("path not found: %s", req.Path))
}

func (s *Server) DeleteDirectory(ctx context.Context, req *pb.DeleteDirectoryRequest) (*pb.DeleteDirectoryResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid directory path")
	}
	if err := s.dirSvc.DeleteDirectory(req.Path, req.Recursive); err != nil {
		return nil, internalError("DeleteDirectory", err)
	}
	return &pb.DeleteDirectoryResponse{Success: true, Message: "Directory deleted successfully"}, nil
}

func (s *Server) RenameDirectory(ctx context.Context, req *pb.RenameDirectoryRequest) (*pb.RenameDirectoryResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "new_name is required")
	}
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid directory path")
	}
	if !utils.IsValidFileName(req.NewName) {
		return nil, status.Error(codes.InvalidArgument, "invalid new name")
	}
	if err := s.dirSvc.RenameDirectory(req.Path, req.NewName); err != nil {
		return nil, internalError("RenameDirectory", err)
	}
	return &pb.RenameDirectoryResponse{Success: true, Message: "Directory renamed successfully"}, nil
}

func parseTimestamp(s string) (*timestamppb.Timestamp, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		return nil, err
	}
	return timestamppb.New(t), nil
}

func computeNewPath(oldPath, newName string) string {
	dir := ""
	for i := len(oldPath) - 1; i >= 0; i-- {
		if oldPath[i] == '/' {
			dir = oldPath[:i]
			break
		}
	}
	if dir == "" {
		return newName
	}
	return dir + "/" + newName
}
