package grpc

import (
	"context"
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
	var sessionID string
	var filePath string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive upload request: %w", err)
		}

		switch data := req.Data.(type) {
		case *pb.UploadRequest_Metadata:
			meta := data.Metadata
			if !utils.IsValidFilePath(meta.Path) {
				return status.Error(codes.InvalidArgument, "invalid file path")
			}
			if !utils.IsValidFileName(meta.Name) {
				return status.Error(codes.InvalidArgument, "invalid file name")
			}
			filePath = meta.Path
			sessionID, err = s.transferSvc.CreateUploadSession(meta.Path, meta.Name, meta.TotalSize, "", meta.Hash)
			if err != nil {
				return fmt.Errorf("failed to create upload session: %w", err)
			}
		case *pb.UploadRequest_Chunk:
			if sessionID == "" {
				return fmt.Errorf("metadata must be sent before chunks")
			}
			chunk := data.Chunk
			if err := s.transferSvc.UploadChunk(sessionID, chunk.Data, chunk.Offset); err != nil {
				s.transferSvc.AbortUpload(sessionID)
				return fmt.Errorf("failed to upload chunk: %w", err)
			}
		}
	}

	if sessionID == "" {
		return fmt.Errorf("no metadata received in upload stream")
	}

	if err := s.transferSvc.CompleteUpload(sessionID); err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}

	meta, err := s.fm.GetFileMetadata(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file metadata after upload: %w", err)
	}

	createdAt, _ := parseTimestamp(meta.CreatedAt)

	return stream.SendAndClose(&pb.UploadResponse{
		Id:        meta.ID,
		Path:      meta.Path,
		Name:      meta.Name,
		Size:      meta.Size,
		Hash:      meta.Hash,
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
