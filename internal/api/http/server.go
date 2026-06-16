package http

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/metrics"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type Server struct {
	config         config.ServerConfig
	tlsCfg         config.TLSConfig
	engine         *gin.Engine
	httpServer     *http.Server
	fm             *filemanager.FileManager
	dirSvc         *directory.DirectoryManager
	flSvc          *filelist.FileListService
	transferSvc    *transfer.FileTransferService
	authSvc        *auth.AuthService
	cryptoSvc      *crypto.CryptoService
	store          storage.StorageAdapter
	cacheSvc       *cache.Cache
	metricsSvc     *metrics.Metrics
	db             *database.DB
	corsOrigins    string
	maxUploadSize  int64
	maxChunkSize   int64
	maxPageSize    int
	startTime      time.Time
	concurrencySem chan struct{}
}

func NewServer(cfg config.ServerConfig, tlsCfg config.TLSConfig, fm *filemanager.FileManager, dirSvc *directory.DirectoryManager, flSvc *filelist.FileListService, transferSvc *transfer.FileTransferService, authSvc *auth.AuthService, cryptoSvc *crypto.CryptoService, store storage.StorageAdapter, cacheSvc *cache.Cache, metricsSvc *metrics.Metrics, db *database.DB) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	s := &Server{
		config:        cfg,
		tlsCfg:        tlsCfg,
		engine:        engine,
		fm:            fm,
		dirSvc:        dirSvc,
		flSvc:         flSvc,
		transferSvc:   transferSvc,
		authSvc:       authSvc,
		cryptoSvc:     cryptoSvc,
		store:         store,
		cacheSvc:      cacheSvc,
		metricsSvc:    metricsSvc,
		db:            db,
		corsOrigins:   cfg.CORSAllowedOrigins,
		maxUploadSize: int64(cfg.MaxUploadSizeMB) * 1024 * 1024,
		maxChunkSize:  int64(cfg.MaxChunkSizeMB) * 1024 * 1024,
		maxPageSize:     cfg.MaxPageSize,
		startTime:       time.Now(),
		concurrencySem:  make(chan struct{}, cfg.Workers*4),
	}

	engine.MaxMultipartMemory = 32 << 20 // 32MB in-memory cache for multipart forms; excess spills to temp files
	s.setupRoutes()

	s.httpServer = &http.Server{
		Addr:           fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:        engine,
		MaxHeaderBytes: 1 << 20, // 1MB max header size
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   120 * time.Second,
		IdleTimeout:    120 * time.Second,
	}

	return s
}

func (s *Server) setupRoutes() {
	s.engine.Use(s.corsMiddleware())
	s.engine.Use(s.concurrencyMiddleware())

	api := s.engine.Group("/api/v1")
	api.Use(s.metricsMiddleware())
	api.Use(s.authMiddleware())
	{
		api.GET("/health", s.handleHealth)
		api.POST("/files", s.handleUpload)
		api.GET("/files/*path", s.handleDownload)
		api.GET("/files", s.handleList)
		api.DELETE("/files/*path", s.handleDelete)
		api.PATCH("/files/*path", s.handleRename)
		api.POST("/directories", s.handleCreateDirectory)
		api.GET("/metadata/*path", s.handleGetMetadata)

		uploads := api.Group("/uploads")
		{
			uploads.POST("", s.handleCreateUploadSession)
			uploads.PUT("/:id/chunk", s.handleUploadChunk)
			uploads.GET("/:id/progress", s.handleGetUploadProgress)
			uploads.POST("/:id/complete", s.handleCompleteUpload)
			uploads.DELETE("/:id", s.handleAbortUpload)
		}

		multipartUploads := api.Group("/multipart-uploads")
		{
			multipartUploads.POST("", s.handleCreateMultipartUpload)
			multipartUploads.PUT("/:id/parts/:partNumber", s.handleUploadPart)
			multipartUploads.GET("/:id", s.handleGetMultipartUploadStatus)
			multipartUploads.POST("/:id/complete", s.handleCompleteMultipartUpload)
			multipartUploads.DELETE("/:id", s.handleAbortMultipartUpload)
		}
	}

	s.engine.GET("/metrics", s.authMiddleware(), s.handleMetrics)
}

func (s *Server) corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if s.corsOrigins == "*" || s.corsOrigins == "" {
			c.Header("Access-Control-Allow-Origin", "*")
		} else if origin != "" {
			allowed := strings.Split(s.corsOrigins, ",")
			for _, o := range allowed {
				if strings.TrimSpace(o) == origin {
					c.Header("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}

		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
		c.Header("Access-Control-Max-Age", "86400")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func (s *Server) concurrencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case s.concurrencySem <- struct{}{}:
			defer func() { <-s.concurrencySem }()
			c.Next()
		default:
			sendError(c, http.StatusServiceUnavailable, "Server is busy, please try again later")
			c.Abort()
		}
	}
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		if s.authSvc.IsRateLimited(clientIP) {
			sendError(c, http.StatusTooManyRequests, "Too many authentication failures")
			c.Abort()
			return
		}

		if !s.authSvc.ValidateApiKey("") {
			authHeader := c.GetHeader("Authorization")
			apiKeyHeader := c.GetHeader("X-API-Key")

			var apiKey string
			if apiKeyHeader != "" {
				apiKey = apiKeyHeader
			} else if strings.HasPrefix(authHeader, "Bearer ") {
				apiKey = strings.TrimPrefix(authHeader, "Bearer ")
			} else if strings.HasPrefix(authHeader, "Api-Key ") {
				apiKey = strings.TrimPrefix(authHeader, "Api-Key ")
			}

			if apiKey == "" {
				s.authSvc.RecordAuthFailure(clientIP)
				sendError(c, http.StatusUnauthorized, "Authentication required")
				c.Abort()
				return
			}

			if !s.authSvc.ValidateApiKey(apiKey) {
				s.authSvc.RecordAuthFailure(clientIP)
				sendError(c, http.StatusUnauthorized, "Invalid API key")
				c.Abort()
				return
			}

			s.authSvc.ClearAuthFailure(clientIP)
		}

		c.Next()
	}
}

func (s *Server) metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.metricsSvc != nil {
			start := time.Now()
			c.Next()
			duration := time.Since(start)
			s.metricsSvc.RecordHTTPRequest(c.Request.Method, c.FullPath(), c.Writer.Status(), duration)
		} else {
			c.Next()
		}
	}
}

func (s *Server) handleHealth(c *gin.Context) {
	status := gin.H{
		"status":    "ok",
		"timestamp": utils.GetCurrentTimestamp(),
		"uptime":    time.Since(s.startTime).String(),
	}

	if s.db != nil {
		var one int
		err := s.db.QueryRow("SELECT 1").Scan(&one)
		if err != nil {
			status["database"] = "error"
			status["status"] = "degraded"
		} else {
			status["database"] = "ok"
		}
	}

	if s.store != nil {
		healthPath := fmt.Sprintf("/health-check-%d", time.Now().UnixNano())
		healthData := []byte("health")
		if err := s.store.Write(healthPath, healthData); err == nil {
			if readData, err := s.store.Read(healthPath); err == nil && string(readData) == "health" {
				s.store.Remove(healthPath)
				status["storage"] = "ok"
			} else {
				status["storage"] = "error"
				status["status"] = "degraded"
				s.store.Remove(healthPath)
			}
		} else {
			status["storage"] = "error"
			status["status"] = "degraded"
		}
	}

	code := http.StatusOK
	if status["status"] != "ok" {
		code = http.StatusServiceUnavailable
	}

	c.JSON(code, status)
}

func (s *Server) handleUpload(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		sendError(c, http.StatusBadRequest, "No file provided")
		return
	}
	defer file.Close()

	if header.Size > s.maxUploadSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return
	}

	fileName := header.Filename
	if !isValidFileName(fileName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return
	}

	filePath := c.PostForm("path")
	if filePath == "" {
		filePath = "/" + fileName
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	data, err := io.ReadAll(io.LimitReader(file, s.maxUploadSize+1))
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read file data")
		return
	}

	if int64(len(data)) > s.maxUploadSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return
	}

	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		encrypted, err := s.cryptoSvc.Encrypt(string(data))
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to encrypt file")
			return
		}
		data = []byte(encrypted)
	}

	meta, err := s.fm.UploadFile(filePath, data)
	if err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("upload", filePath, c, true, "")
	c.JSON(http.StatusCreated, meta)
}

func (s *Server) handleDownload(c *gin.Context) {
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "File path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	meta, err := s.fm.GetFileMetadata(filePath)
	if err != nil {
		sendError(c, http.StatusNotFound, "File not found")
		return
	}

	rangeHeader := c.GetHeader("Range")
	if rangeHeader != "" {
		s.handleRangeDownload(c, meta, filePath, rangeHeader)
		return
	}

	fileSize := meta.Size
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", meta.Name))
	c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
	c.Header("Accept-Ranges", "bytes")

	if fileSize > 32*1024*1024 {
		reader, err := s.store.OpenReader(filePath)
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to open file")
			return
		}
		defer reader.Close()
		if seekReader, ok := reader.(io.ReadSeeker); ok {
			http.ServeContent(c.Writer, c.Request, meta.Name, time.Time{}, seekReader)
		} else {
			io.Copy(c.Writer, reader)
		}
		return
	}

	data, err := s.fm.DownloadFile(filePath)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read file")
		return
	}

	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		decrypted, err := s.cryptoSvc.Decrypt(string(data))
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to decrypt file")
			return
		}
		data = []byte(decrypted)
	}

	c.Data(http.StatusOK, "application/octet-stream", data)
}

func (s *Server) handleRangeDownload(c *gin.Context, meta *database.FileMetadata, filePath, rangeHeader string) {
	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
		return
	}

	var start, end int64
	var err error

	if parts[0] == "" {
		suffixLength, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil {
			sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
			return
		}
		start = meta.Size - suffixLength
		end = meta.Size - 1
	} else {
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
			return
		}
		if parts[1] == "" {
			end = meta.Size - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
				return
			}
		}
	}

	if start < 0 || start >= meta.Size || end >= meta.Size || start > end {
		sendError(c, http.StatusRequestedRangeNotSatisfiable, "Range out of bounds")
		return
	}

	chunkSize := int(end - start + 1)
	if chunkSize > 32*1024*1024 {
		sendError(c, http.StatusRequestedRangeNotSatisfiable, "Range too large, maximum 32MB per request")
		return
	}

	data, err := s.fm.DownloadFileAt(filePath, chunkSize, start)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read file range")
		return
	}

	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.Size))
	c.Header("Content-Length", strconv.Itoa(len(data)))
	c.Header("Accept-Ranges", "bytes")
	c.Data(http.StatusPartialContent, "application/octet-stream", data)
}

func (s *Server) handleList(c *gin.Context) {
	dirPath := c.Query("path")
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "20")
	sortBy := c.DefaultQuery("sort_by", "name")
	sortOrder := c.DefaultQuery("sort_order", "asc")

	if dirPath != "" && !isValidFilePath(dirPath) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
		return
	}

	page, _ := strconv.Atoi(pageStr)
	pageSize, _ := strconv.Atoi(pageSizeStr)

	if pageSize > s.maxPageSize {
		pageSize = s.maxPageSize
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if page < 1 {
		page = 1
	}

	result, err := s.flSvc.ListFiles(dirPath, false, page, pageSize, sortBy, sortOrder)
	if err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, result)
}

func (s *Server) handleDelete(c *gin.Context) {
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "File path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	if err := s.fm.DeleteFile(filePath); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("delete", filePath, c, true, "")
	c.JSON(http.StatusOK, gin.H{"message": "File deleted successfully"})
}

func (s *Server) handleRename(c *gin.Context) {
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "File path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	newName := c.PostForm("new_name")
	if newName == "" {
		var req struct {
			NewName string `json:"new_name"`
		}
		if err := c.ShouldBindJSON(&req); err == nil && req.NewName != "" {
			newName = req.NewName
		}
	}

	if newName == "" {
		sendError(c, http.StatusBadRequest, "New name is required")
		return
	}

	if !isValidFileName(newName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return
	}

	if err := s.fm.RenameFile(filePath, newName); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("rename", filePath, c, true, fmt.Sprintf("new_name=%s", newName))
	c.JSON(http.StatusOK, gin.H{"message": "File renamed successfully"})
}

func (s *Server) handleCreateDirectory(c *gin.Context) {
	var req struct {
		Path string `json:"path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Directory path is required")
		return
	}

	if !isValidFilePath(req.Path) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
		return
	}

	if err := s.dirSvc.CreateDirectory(req.Path); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("create_directory", req.Path, c, true, "")
	c.JSON(http.StatusCreated, gin.H{"message": "Directory created successfully"})
}

func (s *Server) handleGetMetadata(c *gin.Context) {
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "Path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	if s.cacheSvc != nil {
		cacheKey := fmt.Sprintf("metadata:%s", filePath)
		if cached, ok := s.cacheSvc.Get(cacheKey); ok {
			if metaMap, ok := cached.(map[string]interface{}); ok {
				c.JSON(http.StatusOK, metaMap)
				return
			}
		}
	}

	meta, err := s.fm.GetFileMetadata(filePath)
	if err == nil {
		if s.cacheSvc != nil {
			cacheKey := fmt.Sprintf("metadata:%s", filePath)
			s.cacheSvc.Set(cacheKey, gin.H{"type": "file", "metadata": meta})
		}
		c.JSON(http.StatusOK, gin.H{"type": "file", "metadata": meta})
		return
	}

	dirMeta, err := s.dirSvc.GetDirectoryMetadata(filePath)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"type": "directory", "metadata": dirMeta})
		return
	}

	sendError(c, http.StatusNotFound, "Path not found")
}

func (s *Server) handleCreateUploadSession(c *gin.Context) {
	var req struct {
		FilePath  string `json:"file_path" binding:"required"`
		FileName  string `json:"file_name" binding:"required"`
		TotalSize int64  `json:"total_size" binding:"required"`
		Hash      string `json:"hash"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	if !isValidFileName(req.FileName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return
	}

	if !isValidFilePath(req.FilePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	if req.TotalSize <= 0 {
		sendError(c, http.StatusBadRequest, "Total size must be positive")
		return
	}

	if req.TotalSize > s.maxUploadSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return
	}

	clientID := c.ClientIP()
	sessionID, err := s.transferSvc.CreateUploadSession(req.FilePath, req.FileName, req.TotalSize, clientID, req.Hash)
	if err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("create_upload_session", req.FilePath, c, true, fmt.Sprintf("session_id=%s", sessionID))
	c.JSON(http.StatusCreated, gin.H{"session_id": sessionID})
}

func (s *Server) handleUploadChunk(c *gin.Context) {
	sessionID := c.Param("id")

	file, _, err := c.Request.FormFile("data")
	if err != nil {
		sendError(c, http.StatusBadRequest, "No chunk data provided")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, s.maxChunkSize+1))
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read chunk data")
		return
	}

	if int64(len(data)) > s.maxChunkSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("Chunk size exceeds maximum allowed size of %d MB", s.config.MaxChunkSizeMB))
		return
	}

	offsetStr := c.PostForm("offset")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		sendError(c, http.StatusBadRequest, "Invalid offset value")
		return
	}

	if err := s.transferSvc.UploadChunk(sessionID, data, offset); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Chunk uploaded successfully"})
}

func (s *Server) handleGetUploadProgress(c *gin.Context) {
	sessionID := c.Param("id")
	progress := s.transferSvc.GetUploadProgress(sessionID)
	c.JSON(http.StatusOK, gin.H{"uploaded_bytes": progress})
}

func (s *Server) handleCompleteUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.CompleteUpload(sessionID); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("complete_upload", "", c, true, fmt.Sprintf("session_id=%s", sessionID))
	c.JSON(http.StatusOK, gin.H{"message": "Upload completed successfully"})
}

func (s *Server) handleAbortUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.AbortUpload(sessionID); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Upload aborted"})
}

func (s *Server) handleMetrics(c *gin.Context) {
	if s.metricsSvc != nil {
		s.metricsSvc.Handler().ServeHTTP(c.Writer, c.Request)
	} else {
		c.JSON(http.StatusOK, gin.H{"uptime": time.Since(s.startTime).String(), "version": "1.0.0"})
	}
}

func (s *Server) Handler() http.Handler {
	return s.engine
}

func (s *Server) ListenAndServe() error {
	if s.tlsCfg.Enabled {
		logger.Info("HTTP server listening on %s (TLS)", s.httpServer.Addr)
		return s.httpServer.ListenAndServeTLS(s.tlsCfg.CertFile, s.tlsCfg.KeyFile)
	}
	logger.Info("HTTP server listening on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

func (s *Server) Serve(ln net.Listener) error {
	logger.Info("HTTP server listening on %s", ln.Addr().String())
	return s.httpServer.Serve(ln)
}

func (s *Server) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down HTTP server...")
	return s.httpServer.Shutdown(ctx)
}

func isValidFileName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	if name == "." || name == ".." {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	for _, c := range name {
		if c == '/' || c == '\\' || c == '\x00' || c == '\n' || c == '\r' {
			return false
		}
		if c < 0x20 {
			return false
		}
	}
	return true
}

func isValidFilePath(path string) bool {
	if path == "" {
		return false
	}
	cleaned := path
	for strings.Contains(cleaned, "..") {
		cleaned = strings.ReplaceAll(cleaned, "..", "")
	}
	if strings.Contains(path, "..") {
		return false
	}
	if strings.Contains(path, "\\") {
		return false
	}
	return true
}

func auditLog(operation, resourcePath string, c *gin.Context, success bool, details string) {
	clientIP := c.ClientIP()
	userAgent := c.GetHeader("User-Agent")

	userIdentifier := "anonymous"
	apiKeyHeader := c.GetHeader("X-API-Key")
	authHeader := c.GetHeader("Authorization")
	if apiKeyHeader != "" {
		userIdentifier = "api-key:***"
	} else if strings.HasPrefix(authHeader, "Bearer ") {
		userIdentifier = "bearer:***"
	}

	logger.Info("AUDIT: operation=%s resource=%s user=%s ip=%s ua=%s success=%v details=%s",
		operation, resourcePath, userIdentifier, clientIP, userAgent, success, details)
}

func sendError(c *gin.Context, status int, msg string) {
	c.JSON(status, gin.H{"error": msg})
}

func (s *Server) handleCreateMultipartUpload(c *gin.Context) {
	var req struct {
		FilePath  string `json:"file_path" binding:"required"`
		FileName  string `json:"file_name" binding:"required"`
		TotalSize int64  `json:"total_size" binding:"required"`
		Hash      string `json:"hash"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	if !isValidFileName(req.FileName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return
	}

	if !isValidFilePath(req.FilePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	if req.TotalSize <= 0 {
		sendError(c, http.StatusBadRequest, "Total size must be positive")
		return
	}

	if req.TotalSize > s.maxUploadSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return
	}

	clientID := c.ClientIP()
	sessionID, partSize, err := s.transferSvc.CreateMultipartUpload(req.FilePath, req.FileName, req.TotalSize, clientID, req.Hash)
	if err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("create_multipart_upload", req.FilePath, c, true, fmt.Sprintf("session_id=%s part_size=%d", sessionID, partSize))
	c.JSON(http.StatusCreated, gin.H{
		"session_id": sessionID,
		"part_size":  partSize,
	})
}

func (s *Server) handleUploadPart(c *gin.Context) {
	sessionID := c.Param("id")
	partNumber, err := strconv.Atoi(c.Param("partNumber"))
	if err != nil || partNumber < 1 {
		sendError(c, http.StatusBadRequest, "Part number must be a positive integer")
		return
	}

	if partNumber > 10000 {
		sendError(c, http.StatusBadRequest, "Part number exceeds maximum (10000)")
		return
	}

	file, _, err := c.Request.FormFile("data")
	if err != nil {
		sendError(c, http.StatusBadRequest, "No part data provided")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, s.maxChunkSize+1))
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read part data")
		return
	}

	if int64(len(data)) > s.maxChunkSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("Part size exceeds maximum allowed size of %d MB", s.config.MaxChunkSizeMB))
		return
	}

	offsetStr := c.PostForm("offset")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		sendError(c, http.StatusBadRequest, "Invalid offset value")
		return
	}

	if err := s.transferSvc.UploadPartData(sessionID, partNumber, offset, data); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":     "Part uploaded successfully",
		"part_number": partNumber,
		"size":        len(data),
	})
}

func (s *Server) handleGetMultipartUploadStatus(c *gin.Context) {
	sessionID := c.Param("id")
	uploaded, total, completedParts := s.transferSvc.GetMultipartUploadProgress(sessionID)
	c.JSON(http.StatusOK, gin.H{
		"uploaded_bytes":  uploaded,
		"total_bytes":     total,
		"completed_parts": completedParts,
	})
}

func (s *Server) handleCompleteMultipartUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.CompleteMultipartUpload(sessionID); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	auditLog("complete_multipart_upload", "", c, true, fmt.Sprintf("session_id=%s", sessionID))
	c.JSON(http.StatusOK, gin.H{"message": "Multipart upload completed successfully"})
}

func (s *Server) handleAbortMultipartUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.AbortMultipartUpload(sessionID); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Multipart upload aborted"})
}
