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
	httpsServer    *http.Server
	fm             *filemanager.FileManager
	dirSvc         *directory.DirectoryManager
	flSvc          *filelist.FileListService
	transferSvc    *transfer.FileTransferService
	authSvc        *auth.AuthService
	cryptoSvc      *crypto.CryptoService
	store          storage.StorageAdapter
	cacheSvc       cache.CacheAdapter
	metricsSvc     *metrics.Metrics
	db             *database.DB
	corsOrigins    string
	maxUploadSize  int64
	maxChunkSize   int64
	maxPageSize    int
	startTime      time.Time
	concurrencySem chan struct{}
}

func NewServer(cfg config.ServerConfig, tlsCfg config.TLSConfig, fm *filemanager.FileManager, dirSvc *directory.DirectoryManager, flSvc *filelist.FileListService, transferSvc *transfer.FileTransferService, authSvc *auth.AuthService, cryptoSvc *crypto.CryptoService, store storage.StorageAdapter, cacheSvc cache.CacheAdapter, metricsSvc *metrics.Metrics, db *database.DB) *Server {
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

	if tlsCfg.Enabled {
		s.httpsServer = &http.Server{
			Addr:           fmt.Sprintf(":%d", cfg.HTTPSPort),
			Handler:        engine,
			MaxHeaderBytes: 1 << 20,
			ReadTimeout:    60 * time.Second,
			WriteTimeout:   120 * time.Second,
			IdleTimeout:    120 * time.Second,
		}
	}

	return s
}

func (s *Server) setupRoutes() {
	s.engine.Use(s.corsMiddleware())
	s.engine.Use(s.concurrencyMiddleware())

	// Health and readiness checks must be publicly accessible (no auth) so that
	// load balancers and Kubernetes probes can verify service health.
	s.engine.GET("/health", s.handleHealth)
	s.engine.GET("/ready", s.handleHealth)

	api := s.engine.Group("/api/v1")
	api.Use(s.metricsMiddleware())
	api.Use(s.authMiddleware())
	{
		api.POST("/files", s.requirePermission("files", "write"), s.handleUpload)
		api.GET("/files/*path", s.requirePermission("files", "read"), s.handleDownload)
		api.GET("/files", s.requirePermission("files", "read"), s.handleList)
		api.DELETE("/files/*path", s.requirePermission("files", "write"), s.handleDelete)
		api.PATCH("/files/*path", s.requirePermission("files", "write"), s.handleRename)
		api.POST("/directories", s.requirePermission("files", "write"), s.handleCreateDirectory)
		api.DELETE("/directories/*path", s.requirePermission("files", "write"), s.handleDeleteDirectory)
		api.PATCH("/directories/*path", s.requirePermission("files", "write"), s.handleRenameDirectory)
		api.GET("/metadata/*path", s.requirePermission("files", "read"), s.handleGetMetadata)
		api.GET("/audit-logs", s.requireAdmin(), s.handleListAuditLogs)

		uploads := api.Group("/uploads")
		{
			uploads.POST("", s.requirePermission("files", "write"), s.handleCreateUploadSession)
			uploads.PUT("/:id/chunk", s.requirePermission("files", "write"), s.handleUploadChunk)
			uploads.GET("/:id/progress", s.requirePermission("files", "read"), s.handleGetUploadProgress)
			uploads.POST("/:id/complete", s.requirePermission("files", "write"), s.handleCompleteUpload)
			uploads.DELETE("/:id", s.requirePermission("files", "write"), s.handleAbortUpload)
		}

		apiKeys := api.Group("/api-keys")
		{
			apiKeys.POST("", s.requireAdmin(), s.handleCreateApiKey)
			apiKeys.GET("", s.requireAdmin(), s.handleListApiKeys)
			apiKeys.GET("/:id", s.requireAdmin(), s.handleGetApiKey)
			apiKeys.PATCH("/:id", s.requireAdmin(), s.handleUpdateApiKey)
			apiKeys.DELETE("/:id", s.requireAdmin(), s.handleDeleteApiKey)
		}

		multipartUploads := api.Group("/multipart-uploads")
		{
			multipartUploads.POST("", s.requirePermission("files", "write"), s.handleCreateMultipartUpload)
			multipartUploads.PUT("/:id/parts/:partNumber", s.requirePermission("files", "write"), s.handleUploadPart)
			multipartUploads.GET("/:id", s.requirePermission("files", "read"), s.handleGetMultipartUploadStatus)
			multipartUploads.POST("/:id/complete", s.requirePermission("files", "write"), s.handleCompleteMultipartUpload)
			multipartUploads.DELETE("/:id", s.requirePermission("files", "write"), s.handleAbortMultipartUpload)
		}

		authGroup := api.Group("/auth")
		{
			authGroup.POST("/token", s.handleGenerateToken)
			authGroup.POST("/refresh", s.handleRefreshToken)
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

			// Store the resolved user in the context for downstream RBAC checks.
			if user := s.authSvc.GetUserByApiKey(apiKey); user != nil {
				c.Set("user", user)
				c.Set("api_key", apiKey)
			}
		}

		c.Next()
	}
}

// requirePermission returns a middleware that enforces RBAC on the given resource/action.
func (s *Server) requirePermission(resource, action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.authSvc.ValidateApiKey("") {
			// Auth is enabled; resolve the user from the context (set by authMiddleware).
			val, exists := c.Get("user")
			if !exists {
				sendError(c, http.StatusForbidden, "Permission denied: no authenticated user")
				c.Abort()
				return
			}
			user, ok := val.(*auth.User)
			if !ok || user == nil {
				sendError(c, http.StatusForbidden, "Permission denied")
				c.Abort()
				return
			}
			if user.Role == "admin" {
				c.Next()
				return
			}
			if !(user.Role == "user" && resource == "files" && (action == "read" || action == "write")) {
				sendError(c, http.StatusForbidden, "Permission denied")
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

// requireAdmin returns a middleware that restricts access to admin users only.
func (s *Server) requireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.authSvc.ValidateApiKey("") {
			val, exists := c.Get("user")
			if !exists {
				sendError(c, http.StatusForbidden, "Admin permission required")
				c.Abort()
				return
			}
			user, ok := val.(*auth.User)
			if !ok || user == nil || user.Role != "admin" {
				sendError(c, http.StatusForbidden, "Admin permission required")
				c.Abort()
				return
			}
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

	s.auditLog("upload", filePath, c, true, "")
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

	s.auditLog("delete", filePath, c, true, "")
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

	s.auditLog("rename", filePath, c, true, fmt.Sprintf("new_name=%s", newName))
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

	s.auditLog("create_directory", req.Path, c, true, "")
	c.JSON(http.StatusCreated, gin.H{"message": "Directory created successfully"})
}

func (s *Server) handleDeleteDirectory(c *gin.Context) {
	dirPath := c.Param("path")
	if dirPath == "" {
		sendError(c, http.StatusBadRequest, "Directory path is required")
		return
	}

	if !isValidFilePath(dirPath) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
		return
	}

	recursive := c.Query("recursive") == "true"

	if err := s.dirSvc.DeleteDirectory(dirPath, recursive); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	s.auditLog("delete_directory", dirPath, c, true, fmt.Sprintf("recursive=%v", recursive))
	c.JSON(http.StatusOK, gin.H{"message": "Directory deleted successfully"})
}

func (s *Server) handleRenameDirectory(c *gin.Context) {
	dirPath := c.Param("path")
	if dirPath == "" {
		sendError(c, http.StatusBadRequest, "Directory path is required")
		return
	}

	if !isValidFilePath(dirPath) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
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
		sendError(c, http.StatusBadRequest, "Invalid directory name")
		return
	}

	if err := s.dirSvc.RenameDirectory(dirPath, newName); err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	s.auditLog("rename_directory", dirPath, c, true, fmt.Sprintf("new_name=%s", newName))
	c.JSON(http.StatusOK, gin.H{"message": "Directory renamed successfully"})
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

func (s *Server) handleListAuditLogs(c *gin.Context) {
	if s.db == nil {
		sendError(c, http.StatusServiceUnavailable, "Database not available")
		return
	}

	operation := c.Query("operation")
	resourcePath := c.Query("resource_path")
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "20")

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

	auditLogSvc := database.NewAuditLogService(s.db)
	logs, err := auditLogSvc.List(operation, resourcePath, page, pageSize)
	if err != nil {
		sendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	if logs == nil {
		logs = []database.AuditLog{}
	}

	c.JSON(http.StatusOK, gin.H{
		"logs":      logs,
		"page":      page,
		"page_size": pageSize,
	})
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

	s.auditLog("create_upload_session", req.FilePath, c, true, fmt.Sprintf("session_id=%s", sessionID))
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

	s.auditLog("complete_upload", "", c, true, fmt.Sprintf("session_id=%s", sessionID))
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

func (s *Server) handleCreateApiKey(c *gin.Context) {
	var req struct {
		Name        string `json:"name" binding:"required"`
		Description string `json:"description"`
		Permissions string `json:"permissions"`
		ExpiresAt   string `json:"expires_at"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Name is required")
		return
	}

	keySvc := database.NewApiKeyService(s.db)

	id := utils.GenerateUUID()
	plainKey := s.authSvc.GenerateApiKey(id)
	keyHash := utils.SHA256(plainKey)

	apiKey := &database.ApiKey{
		ID:          id,
		KeyHash:     keyHash,
		Name:        req.Name,
		Description: req.Description,
		Permissions: req.Permissions,
		CreatedAt:   utils.GetCurrentTimestamp(),
		ExpiresAt:   req.ExpiresAt,
		IsActive:    true,
	}

	if err := keySvc.Create(apiKey); err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to create API key")
		return
	}

	s.auditLog("create_api_key", id, c, true, fmt.Sprintf("name=%s", req.Name))
	c.JSON(http.StatusCreated, gin.H{
		"id":          apiKey.ID,
		"name":        apiKey.Name,
		"description": apiKey.Description,
		"permissions": apiKey.Permissions,
		"created_at":  apiKey.CreatedAt,
		"expires_at":  apiKey.ExpiresAt,
		"is_active":   apiKey.IsActive,
		"key":         plainKey,
	})
}

func (s *Server) handleListApiKeys(c *gin.Context) {
	activeOnly := c.Query("active_only") == "true"
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "20")

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

	keySvc := database.NewApiKeyService(s.db)

	keys, err := keySvc.List(activeOnly, page, pageSize)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to list API keys")
		return
	}

	result := make([]gin.H, 0, len(keys))
	for _, k := range keys {
		result = append(result, gin.H{
			"id":          k.ID,
			"name":        k.Name,
			"description": k.Description,
			"permissions": k.Permissions,
			"created_at":  k.CreatedAt,
			"expires_at":  k.ExpiresAt,
			"last_used_at": k.LastUsedAt,
			"is_active":   k.IsActive,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"keys": result,
		"page": page,
		"page_size": pageSize,
	})
}

func (s *Server) handleGetApiKey(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		sendError(c, http.StatusBadRequest, "API key ID is required")
		return
	}

	keySvc := database.NewApiKeyService(s.db)

	key, err := keySvc.GetById(id)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to get API key")
		return
	}
	if key == nil {
		sendError(c, http.StatusNotFound, "API key not found")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":          key.ID,
		"name":        key.Name,
		"description": key.Description,
		"permissions": key.Permissions,
		"created_at":  key.CreatedAt,
		"expires_at":  key.ExpiresAt,
		"last_used_at": key.LastUsedAt,
		"is_active":   key.IsActive,
	})
}

func (s *Server) handleUpdateApiKey(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		sendError(c, http.StatusBadRequest, "API key ID is required")
		return
	}

	var req struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
		IsActive    *bool   `json:"is_active"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	keySvc := database.NewApiKeyService(s.db)

	key, err := keySvc.GetById(id)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to get API key")
		return
	}
	if key == nil {
		sendError(c, http.StatusNotFound, "API key not found")
		return
	}

	if req.Name != nil {
		key.Name = *req.Name
	}
	if req.Description != nil {
		key.Description = *req.Description
	}
	if req.IsActive != nil {
		key.IsActive = *req.IsActive
	}

	if err := keySvc.Update(key); err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to update API key")
		return
	}

	s.auditLog("update_api_key", id, c, true, fmt.Sprintf("name=%s is_active=%v", key.Name, key.IsActive))
	c.JSON(http.StatusOK, gin.H{
		"id":          key.ID,
		"name":        key.Name,
		"description": key.Description,
		"permissions": key.Permissions,
		"created_at":  key.CreatedAt,
		"expires_at":  key.ExpiresAt,
		"last_used_at": key.LastUsedAt,
		"is_active":   key.IsActive,
	})
}

func (s *Server) handleDeleteApiKey(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		sendError(c, http.StatusBadRequest, "API key ID is required")
		return
	}

	keySvc := database.NewApiKeyService(s.db)

	key, err := keySvc.GetById(id)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to get API key")
		return
	}
	if key == nil {
		sendError(c, http.StatusNotFound, "API key not found")
		return
	}

	if err := keySvc.Remove(id); err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to delete API key")
		return
	}

	s.auditLog("delete_api_key", id, c, true, fmt.Sprintf("name=%s", key.Name))
	c.JSON(http.StatusOK, gin.H{"message": "API key deleted successfully"})
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
	logger.Info("HTTP server listening on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

func (s *Server) ListenAndServeTLS() error {
	if s.httpsServer != nil {
		logger.Info("HTTPS server listening on %s (TLS)", s.httpsServer.Addr)
		go func() {
			if err := s.httpsServer.ListenAndServeTLS(s.tlsCfg.CertFile, s.tlsCfg.KeyFile); err != nil && err != http.ErrServerClosed {
				logger.Error("HTTPS server error: %v", err)
			}
		}()
	}
	return s.ListenAndServe()
}

func (s *Server) Serve(ln net.Listener) error {
	logger.Info("HTTP server listening on %s", ln.Addr().String())
	return s.httpServer.Serve(ln)
}

func (s *Server) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down HTTP server...")
	var err error
	if s.httpsServer != nil {
		if e := s.httpsServer.Shutdown(ctx); e != nil {
			logger.Error("HTTPS server shutdown error: %v", e)
		}
	}
	err = s.httpServer.Shutdown(ctx)
	return err
}

func isValidFileName(name string) bool {
	return utils.IsValidFileName(name)
}

func isValidFilePath(p string) bool {
	return utils.IsValidFilePath(p)
}

func (s *Server) auditLog(operation, resourcePath string, c *gin.Context, success bool, details string) {
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

	if s.db != nil {
		auditLogSvc := database.NewAuditLogService(s.db)
		entry := &database.AuditLog{
			ID:             utils.GenerateUUID(),
			Timestamp:      utils.GetCurrentTimestamp(),
			Operation:      operation,
			ResourcePath:   resourcePath,
			UserIdentifier: userIdentifier,
			ClientIP:       clientIP,
			UserAgent:      userAgent,
			Success:        success,
			Details:        details,
		}
		if err := auditLogSvc.Create(entry); err != nil {
			logger.Error("Failed to persist audit log: %v", err)
		}
	}
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

	s.auditLog("create_multipart_upload", req.FilePath, c, true, fmt.Sprintf("session_id=%s part_size=%d", sessionID, partSize))
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

	s.auditLog("complete_multipart_upload", "", c, true, fmt.Sprintf("session_id=%s", sessionID))
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

func (s *Server) handleGenerateToken(c *gin.Context) {
	if s.authSvc == nil || s.authSvc.GetJWTService() == nil {
		sendError(c, http.StatusServiceUnavailable, "JWT authentication not configured")
		return
	}

	var req struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Resolve the caller's identity from the auth middleware. The caller cannot
	// self-elevate: only an admin may issue admin tokens.
	callerRole := "user"
	callerID := req.UserID
	if val, exists := c.Get("user"); exists {
		if user, ok := val.(*auth.User); ok && user != nil {
			callerRole = user.Role
			if callerID == "" {
				callerID = user.ID
			}
		}
	}

	if callerID == "" {
		sendError(c, http.StatusBadRequest, "user_id is required")
		return
	}

	// Non-admin callers always receive a "user" token regardless of what they request.
	grantRole := "user"
	if callerRole == "admin" && (req.Role == "admin" || req.Role == "user") {
		grantRole = req.Role
	}

	tokenPair, err := s.authSvc.GetJWTService().GenerateTokenPair(callerID, grantRole)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to generate token")
		return
	}

	s.auditLog("issue_token", callerID, c, true, fmt.Sprintf("role=%s", grantRole))
	c.JSON(http.StatusOK, tokenPair)
}

func (s *Server) handleRefreshToken(c *gin.Context) {
	if s.authSvc == nil || s.authSvc.GetJWTService() == nil {
		sendError(c, http.StatusServiceUnavailable, "JWT authentication not configured")
		return
	}

	var req struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request: refresh_token is required")
		return
	}

	tokenPair, err := s.authSvc.GetJWTService().RefreshToken(req.RefreshToken)
	if err != nil {
		sendError(c, http.StatusUnauthorized, "Invalid or expired refresh token")
		return
	}

	c.JSON(http.StatusOK, tokenPair)
}
