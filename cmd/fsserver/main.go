package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sosoxu/fssvrgo/internal/api/grpc"
	"github.com/sosoxu/fssvrgo/internal/api/http"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/consistency"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/discovery"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/etcd"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/metrics"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg := &config.Config{}
	if err := cfg.Load(configPath); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		os.Exit(1)
	}

	if err := logger.Initialize(cfg.Logging.File, cfg.Logging.Level); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting fsserver...")
	logger.Info("Config loaded from %s", configPath)

	// Database
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(cfg.Database); err != nil {
		logger.Error("Failed to connect to database: %v", err)
		os.Exit(1)
	}
	defer dbObj.Close()
	logger.Info("Database connected (%s)", cfg.Database.Type)

	queryDB := dbObj.GetQueryDB()
	migrationMgr := database.NewMigrationManager(queryDB)
	database.RegisterBaseMigration(migrationMgr)
	if err := migrationMgr.RunMigrations(); err != nil {
		logger.Error("Failed to run database migrations: %v", err)
		os.Exit(1)
	}

	// Storage
	var store storage.StorageAdapter
	switch cfg.Storage.Type {
	case "minio":
		minioStore, err := storage.NewMinIOStorage(storage.MinIOConfig{
			Endpoint:  cfg.Storage.MinIO.Endpoint,
			AccessKey: cfg.Storage.MinIO.AccessKey,
			SecretKey: cfg.Storage.MinIO.SecretKey,
			Bucket:    cfg.Storage.MinIO.Bucket,
			UseSSL:    cfg.Storage.MinIO.UseSSL,
		})
		if err != nil {
			logger.Error("Failed to initialize MinIO storage: %v", err)
			os.Exit(1)
		}
		store = minioStore
		logger.Info("Storage: MinIO (%s)", cfg.Storage.MinIO.Endpoint)
	default:
		if err := os.MkdirAll(cfg.Storage.Local.RootDir, 0755); err != nil {
			logger.Error("Failed to create storage directory: %v", err)
			os.Exit(1)
		}
		store = storage.NewLocalStorage(cfg.Storage.Local.RootDir)
		logger.Info("Storage: Local (%s)", cfg.Storage.Local.RootDir)
	}

	// Clean up orphaned upload temp directories from previous runs. Only
	// directories older than orphanTempDirMinAge are removed, because other
	// fsserver instances on the same host share os.TempDir() and may still be
	// uploading into theirs. A configured (shared) storage.temp_dir is managed
	// by the operator and is never touched here.
	if cfg.Storage.TempDir == "" {
		if removed, err := cleanupOrphanedUploadDirs(os.TempDir(), orphanTempDirMinAge, time.Now()); err != nil {
			logger.Warn("Failed to clean up orphaned upload temp directories: %v", err)
		} else if removed > 0 {
			logger.Info("Removed %d orphaned upload temp director(ies) from %s", removed, os.TempDir())
		}
	}

	// Distributed components
	var distLock distributed.DistributedLock
	var sessionStore distributed.SessionStore
	var redisManager *distributed.RedisManager

	if cfg.Redis.Enabled {
		var err error
		redisManager, err = distributed.NewRedisManager(
			cfg.Redis.Address, cfg.Redis.Password,
			cfg.Redis.DB, cfg.Redis.PoolSize,
		)
		if err != nil {
			logger.Error("Failed to connect to Redis: %v", err)
			os.Exit(1)
		}
		defer redisManager.Close()
		distLock = redisManager.GetLock()
		sessionStore = redisManager.GetSessionStore()
		logger.Info("Redis connected (%s)", cfg.Redis.Address)
	} else {
		distLock = distributed.NewLocalDistributedLock()
		sessionStore = distributed.NewMemorySessionStore()
		logger.Info("Using local distributed lock and memory session store")
	}

	// Etcd
	var etcdMgr *etcd.EtcdManager
	if cfg.Etcd.Enabled {
		var err error
		etcdMgr, err = etcd.NewEtcdManager(cfg.Etcd.Endpoints, cfg.Etcd.Prefix)
		if err != nil {
			logger.Error("Failed to connect to etcd: %v", err)
			os.Exit(1)
		}
		defer etcdMgr.Close()
		logger.Info("Etcd connected (%v)", cfg.Etcd.Endpoints)
	}

	// Service Discovery
	var discoverySvc *discovery.ServiceDiscovery
	if cfg.Discovery.Enabled {
		var err error
		discoverySvc, err = discovery.NewServiceDiscovery(
			cfg.Etcd.Endpoints,
			cfg.Etcd.Prefix,
			cfg.Discovery.Interval,
		)
		if err != nil {
			logger.Error("Failed to initialize service discovery: %v", err)
			os.Exit(1)
		}
		defer discoverySvc.Close()

		// Register this instance
		hostname, _ := os.Hostname()
		instance := &discovery.ServiceInstance{
			ID:      hostname + "-" + fmt.Sprintf("%d", cfg.Server.HTTPPort),
			Name:    "fsserver",
			Address: hostname,
			Port:    cfg.Server.HTTPPort,
			Metadata: map[string]string{
				"grpc_port": fmt.Sprintf("%d", cfg.Server.GRPCPort),
			},
		}
		if err := discoverySvc.Register(instance); err != nil {
			logger.Error("Failed to register service: %v", err)
			os.Exit(1)
		}
		logger.Info("Service discovery enabled (type=%s)", cfg.Discovery.Type)
		// Registration is one-way today: nothing in the process consumes
		// Discover/Watch, so peers are not resolved from etcd yet.
		logger.Warn("service discovery registration succeeded but no component consumes Discover/Watch yet; instances are not yet resolved from etcd")
	}

	// AuthConsistency
	var consistencyMgr *consistency.ConsistencyManager
	if cfg.Consistency.Level != "none" && cfg.Consistency.Level != "" {
		if err := consistency.ValidateQuorum(cfg.Consistency.ReplicaCount, cfg.Consistency.ReadQuorum, cfg.Consistency.WriteQuorum); err != nil {
			logger.Error("Invalid consistency configuration: %v", err)
			os.Exit(1)
		}
		consistencyMgr = consistency.NewConsistencyManager(
			cfg.Consistency.Level,
			cfg.Consistency.ReplicaCount,
			cfg.Consistency.ReadQuorum,
			cfg.Consistency.WriteQuorum,
			cfg.Consistency.SyncIntervalMs,
		)
		defer consistencyMgr.Stop()
		// The consistency manager is constructed so that quorum configuration is
		// validated and the future wiring point is explicit, but no read/write
		// path consults it yet. Say so loudly rather than letting operators
		// believe a consistency level is being enforced.
		logger.Warn("consistency level %q is configured but the module is not wired into any read/write path yet; the setting currently has no runtime effect", cfg.Consistency.Level)
	}

	// Auth
	authSvc := auth.NewAuthService()
	authSvc.Init(cfg.Auth.Enabled, cfg.Auth.Secret)
	// Wire API key lookup so keys created via the management API are validated against the database.
	authSvc.SetApiKeyLookup(func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		return database.NewApiKeyService(queryDB).GetByKeyHash(keyHash)
	})

	// Crypto
	cryptoSvc := crypto.NewCryptoService()
	if cfg.Crypto.Enabled {
		key := cfg.Crypto.Passphrase
		if cfg.Crypto.KeyFile != "" {
			keyData, err := os.ReadFile(cfg.Crypto.KeyFile)
			if err != nil {
				logger.Error("Failed to read crypto key file: %v", err)
				os.Exit(1)
			}
			key = string(keyData)
		}
		if err := cryptoSvc.Init(key); err != nil {
			logger.Error("Failed to initialize crypto service: %v", err)
			os.Exit(1)
		}
		logger.Info("Encryption enabled (AES-256-GCM)")
	}

	// Cache
	var cacheSvc cache.CacheAdapter
	if cfg.Cache.Enabled {
		if cfg.Cache.Type == "redis" && cfg.Redis.Enabled {
			cacheSvc = cache.NewRedisCache(cfg.Redis.Address, cfg.Redis.Password, cfg.Redis.DB, cfg.Redis.PoolSize, int64(cfg.Cache.TTL))
			logger.Info("Cache enabled (type=redis, addr=%s)", cfg.Redis.Address)
		} else {
			c := cache.NewCache(int64(cfg.Cache.TTL), cfg.Cache.MaxSize)
			cacheSvc = c
			logger.Info("Cache enabled (type=memory, ttl=%d, max_size=%d)", cfg.Cache.TTL, cfg.Cache.MaxSize)
		}
	}

	// Metrics
	var metricsSvc *metrics.Metrics
	metricsSvc = metrics.NewMetrics()

	// Services
	fm := filemanager.NewFileManagerWithDistLock(store, queryDB, distLock)
	dirSvc := directory.NewDirectoryManagerWithDistLock(queryDB, store, distLock)
	flSvc := filelist.NewFileListServiceFromDB(queryDB)
	transferSvc := transfer.NewFileTransferServiceWithRedis(store, queryDB, sessionStore, distLock)
	transferSvc.SetCryptoService(cryptoSvc)

	// Override the upload temp directory when configured. When set to a shared
	// volume (e.g. NFS) mounted at the same path on every instance, upload
	// sessions become resumable across instances — the temp file written by the
	// instance that started the session can be opened by the instance that
	// completes it. When left empty, each instance keeps a private temp dir and
	// cross-instance resume falls back to metadata-only (bytes are lost).
	if cfg.Storage.TempDir != "" {
		if err := transferSvc.SetTempDir(cfg.Storage.TempDir); err != nil {
			logger.Error("Failed to set upload temp directory: %v", err)
			os.Exit(1)
		}
		logger.Info("Upload temp dir: %s (shared-volume resume enabled)", cfg.Storage.TempDir)
	}

	// Start session cleanup
	transferSvc.StartCleanupThread(60, 7200)
	defer transferSvc.StopCleanupThread()

	// Cleanup service
	cleanupSvc := database.NewCleanupService(queryDB, store, 60, 30) // every 60 min, 30 day retention
	// Metadata/storage reconciliation: always report once at startup (read-only
	// unless repair is explicitly enabled), then optionally keep running
	// alongside the cleanup cycle.
	if cfg.Maintenance.ReconcileEnabled {
		cleanupSvc.EnableReconcile(cfg.Maintenance.ReconcileRepair)
	}
	if _, err := cleanupSvc.ReconcileNow(context.Background()); err != nil {
		logger.Warn("startup reconciliation failed: %v", err)
	}
	cleanupSvc.Start()
	defer cleanupSvc.Stop()

	// HTTP server
	httpServer := http.NewServer(
		cfg.Server, cfg.TLS,
		fm, dirSvc, flSvc, transferSvc,
		authSvc, cryptoSvc,
		store, cacheSvc, metricsSvc, queryDB,
	)

	// gRPC server
	var grpcServer *grpc.Server
	if cfg.Server.GRPCEnabled {
		grpcServer = grpc.NewServer(
			cfg.Server,
			fm, dirSvc, flSvc, transferSvc,
			authSvc, cryptoSvc,
			metricsSvc,
		)
	}

	// Start servers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Periodically clean up unused in-memory file locks to prevent unbounded
	// growth of the fileLocks sync.Map in long-running processes.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fm.CleanFileLocks()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		if cfg.TLS.Enabled {
			if err := httpServer.ListenAndServeTLS(); err != nil {
				logger.Error("HTTP/HTTPS server error: %v", err)
				cancel()
			}
		} else {
			if err := httpServer.ListenAndServe(); err != nil {
				logger.Error("HTTP server error: %v", err)
				cancel()
			}
		}
	}()

	if grpcServer != nil {
		go func() {
			if err := grpcServer.Start(); err != nil {
				logger.Error("gRPC server error: %v", err)
				cancel()
			}
		}()
	}

	logger.Info("fsserver started successfully (HTTP:%d gRPC:%d)", cfg.Server.HTTPPort, cfg.Server.GRPCPort)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("Received signal: %v", sig)
		cancel() // Cancel context to stop background goroutines
	case <-ctx.Done():
		logger.Info("Server context cancelled")
	}

	logger.Info("Shutting down fsserver...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error: %v", err)
	}

	if grpcServer != nil {
		grpcServer.Stop()
	}

	logger.Info("fsserver stopped")
}
