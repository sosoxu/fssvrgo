// Package http exposes the REST API. It owns routing, request validation,
// response mapping, and error translation; business logic lives in the
// internal/service packages and persistence behind their narrow store
// interfaces.
//
// The package is split by concern: server.go (construction, routes, lifecycle),
// middleware.go (cross-cutting request handling), files.go / download.go /
// directories.go (the file namespace), uploads.go (resumable and multipart
// uploads), apikeys.go, audit.go, tokens.go, health.go, and response.go
// (shared helpers).
package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/metrics"
	"github.com/sosoxu/fssvrgo/internal/service/apikey"
	"github.com/sosoxu/fssvrgo/internal/service/auditlog"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// dbProbe is the narrow contract the liveness probe needs from the database.
// *database.DB satisfies it; nil disables the database check.
type dbProbe interface {
	PingContext(ctx context.Context) error
}

// Deps is the dependency set of the HTTP server. Grouping the dependencies in
// a struct keeps NewServer readable as wiring grows (it previously took twelve
// positional arguments) and lets tests set only the fields they care about.
type Deps struct {
	Config      config.ServerConfig
	TLS         config.TLSConfig
	Files       *filemanager.FileManager
	Directories *directory.DirectoryManager
	Lists       *filelist.FileListService
	Transfers   *transfer.FileTransferService
	Auth        *auth.AuthService
	Crypto      *crypto.CryptoService
	Storage     storage.StorageAdapter
	Cache       cache.CacheAdapter
	Metrics     *metrics.Metrics
	Audit       *auditlog.Service
	ApiKeys     *apikey.Service
	// DB backs the liveness probe only. Nil disables the database check.
	DB dbProbe
}

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
	auditSvc       *auditlog.Service
	apiKeySvc      *apikey.Service
	db             dbProbe
	corsOrigins    string
	maxUploadSize  int64
	maxChunkSize   int64
	maxPageSize    int
	startTime      time.Time
	concurrencySem chan struct{}
}

func NewServer(deps Deps) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// Mirror config.applyDefaults: when NewServer is constructed directly
	// (e.g. in tests that bypass config.Load), Workers may be 0, which
	// would make the concurrency semaphore unbuffered and reject every
	// request with 503. Guard against that footgun.
	workers := deps.Config.Workers
	if workers < 1 {
		workers = 8
	}

	s := &Server{
		config:         deps.Config,
		tlsCfg:         deps.TLS,
		engine:         engine,
		fm:             deps.Files,
		dirSvc:         deps.Directories,
		flSvc:          deps.Lists,
		transferSvc:    deps.Transfers,
		authSvc:        deps.Auth,
		cryptoSvc:      deps.Crypto,
		store:          deps.Storage,
		cacheSvc:       deps.Cache,
		metricsSvc:     deps.Metrics,
		auditSvc:       deps.Audit,
		apiKeySvc:      deps.ApiKeys,
		db:             deps.DB,
		corsOrigins:    deps.Config.CORSAllowedOrigins,
		maxUploadSize:  int64(deps.Config.MaxUploadSizeMB) * 1024 * 1024,
		maxChunkSize:   int64(deps.Config.MaxChunkSizeMB) * 1024 * 1024,
		maxPageSize:    deps.Config.MaxPageSize,
		startTime:      time.Now(),
		concurrencySem: make(chan struct{}, workers*4),
	}

	engine.MaxMultipartMemory = 32 << 20 // 32MB in-memory cache for multipart forms; excess spills to temp files
	s.setupRoutes()

	s.httpServer = &http.Server{
		Addr:           fmt.Sprintf(":%d", deps.Config.HTTPPort),
		Handler:        engine,
		MaxHeaderBytes: 1 << 20, // 1MB max header size
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   120 * time.Second,
		IdleTimeout:    120 * time.Second,
	}

	if deps.TLS.Enabled {
		s.httpsServer = &http.Server{
			Addr:           fmt.Sprintf(":%d", deps.Config.HTTPSPort),
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
	// Request ID must run before everything else so logs, audit entries, and
	// error responses can all reference the same correlation id.
	s.engine.Use(s.requestIDMiddleware())
	s.engine.Use(s.corsMiddleware())
	s.engine.Use(s.concurrencyMiddleware())

	// Health and readiness checks must be publicly accessible (no auth) so that
	// load balancers and Kubernetes probes can verify service health. /health is
	// a cheap liveness probe (DB only); /ready additionally probes storage
	// reachability, so it should be used as the readiness probe.
	s.engine.GET("/health", s.handleHealth)
	s.engine.GET("/ready", s.handleReady)

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
	// Flush the audit writer so pending audit entries are persisted before the
	// process exits. Use the same shutdown deadline; if the DB is slow the
	// caller's ctx expires and remaining entries are logged-and-dropped.
	if s.auditSvc != nil {
		if e := s.auditSvc.Close(ctx); e != nil {
			logger.Error("Audit writer shutdown error: %v", e)
		}
	}
	return err
}
