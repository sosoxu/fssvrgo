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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
}

func NewServer(cfg config.ServerConfig, fm *filemanager.FileManager, dirSvc *directory.DirectoryManager, flSvc *filelist.FileListService, transferSvc *transfer.FileTransferService, authSvc *auth.AuthService, cryptoSvc *crypto.CryptoService, metricsSvc *metrics.Metrics) *Server {
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

	s.grpcServer = grpc.NewServer(
		grpc.ChainUnaryInterceptor(s.unaryAuthInterceptor, s.unaryMetricsInterceptor),
		grpc.ChainStreamInterceptor(s.streamAuthInterceptor, s.streamMetricsInterceptor),
	)
	pb.RegisterFileServiceServer(s.grpcServer, s)
	return s
}

func (s *Server) unaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *Server) streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.authenticate(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *Server) unaryMetricsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if s.metricsSvc != nil {
		start := time.Now()
		resp, err := handler(ctx, req)
		s.metricsSvc.RecordHTTPRequest("gRPC", info.FullMethod, statusCodeFromErr(err), time.Since(start))
		return resp, err
	}
	return handler(ctx, req)
}

func (s *Server) streamMetricsInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if s.metricsSvc != nil {
		start := time.Now()
		err := handler(srv, ss)
		s.metricsSvc.RecordHTTPRequest("gRPC", info.FullMethod, statusCodeFromErr(err), time.Since(start))
		return err
	}
	return handler(srv, ss)
}

func statusCodeFromErr(err error) int {
	if err == nil {
		return 200
	}
	return 500
}

func (s *Server) authenticate(ctx context.Context) error {
	if s.authSvc == nil {
		return nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		if !s.authSvc.ValidateApiKey("") {
			return status.Error(codes.Unauthenticated, "authentication required")
		}
		return nil
	}

	apiKeys := md.Get("x-api-key")
	if len(apiKeys) > 0 && apiKeys[0] != "" {
		if s.authSvc.ValidateApiKey(apiKeys[0]) {
			return nil
		}
		return status.Error(codes.Unauthenticated, "invalid API key")
	}

	authHeaders := md.Get("authorization")
	for _, ah := range authHeaders {
		if strings.HasPrefix(ah, "Bearer ") {
			token := strings.TrimPrefix(ah, "Bearer ")
			if s.authSvc.ValidateApiKey(token) {
				return nil
			}
			// After Bearer token API key check fails, try JWT
			if s.authSvc.GetJWTService() != nil {
				claims, err := s.authSvc.GetJWTService().ValidateToken(token)
				if err == nil && claims != nil {
					return nil
				}
			}
			return status.Error(codes.Unauthenticated, "invalid token")
		}
		if strings.HasPrefix(ah, "Api-Key ") {
			key := strings.TrimPrefix(ah, "Api-Key ")
			if s.authSvc.ValidateApiKey(key) {
				return nil
			}
			return status.Error(codes.Unauthenticated, "invalid API key")
		}
	}

	if !s.authSvc.ValidateApiKey("") {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	return nil
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
}

func (s *Server) UploadFile(stream grpc.ClientStreamingServer[pb.UploadRequest, pb.UploadResponse]) error {
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
	nextReq, nextErr := stream.Recv()
	if nextErr == io.EOF && firstChunk.Offset == 0 && int64(len(firstChunk.Data)) == meta.TotalSize {
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
	_ = result // hash verification status is surfaced via HTTP API; gRPC response already carries file metadata

	fileMeta, err := s.fm.GetFileMetadata(meta.Path)
	if err != nil {
		return fmt.Errorf("failed to get file metadata after upload: %w", err)
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
	meta, err := s.fm.GetFileMetadata(req.Path)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}

	chunkSize := int(req.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = 1024 * 1024 // 1MB default
	}

	var offset int64
	if req.Offset > 0 {
		offset = req.Offset
	}

	for offset < meta.Size {
		data, err := s.fm.DownloadFileAt(req.Path, chunkSize, offset)
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
	result, err := s.flSvc.ListFiles(req.Path, req.Recursive, int(req.Page), int(req.PageSize), req.SortBy, req.SortOrder)
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

func (s *Server) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if !utils.IsValidFilePath(req.Path) {
		return nil, status.Error(codes.InvalidArgument, "invalid file path")
	}
	if err := s.fm.DeleteFile(req.Path); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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
		return nil, status.Error(codes.Internal, err.Error())
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
		return nil, status.Error(codes.Internal, err.Error())
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
		return nil, status.Error(codes.Internal, err.Error())
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
		return nil, status.Error(codes.Internal, err.Error())
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
