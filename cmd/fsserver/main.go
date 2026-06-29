package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
	migrationMgr.Register(database.Migration{
		Version: 1,
		Name:    "initial_schema",
		Up: func() error {
			statements := []string{
				`CREATE TABLE IF NOT EXISTS files (
					id VARCHAR(36) PRIMARY KEY,
					path VARCHAR(1024) UNIQUE NOT NULL,
					name VARCHAR(255) NOT NULL,
					size BIGINT NOT NULL DEFAULT 0,
					hash VARCHAR(64),
					storage_type VARCHAR(32) NOT NULL DEFAULT 'local',
					storage_location VARCHAR(512),
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					is_deleted BOOLEAN NOT NULL DEFAULT FALSE
				)`,
				`CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)`,
				`CREATE INDEX IF NOT EXISTS idx_files_name ON files(name)`,
				`CREATE INDEX IF NOT EXISTS idx_files_is_deleted ON files(is_deleted)`,
				`CREATE TABLE IF NOT EXISTS directories (
					id VARCHAR(36) PRIMARY KEY,
					path VARCHAR(1024) UNIQUE NOT NULL,
					name VARCHAR(255) NOT NULL,
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					is_deleted BOOLEAN NOT NULL DEFAULT FALSE
				)`,
				`CREATE INDEX IF NOT EXISTS idx_dirs_path ON directories(path)`,
				`CREATE INDEX IF NOT EXISTS idx_dirs_is_deleted ON directories(is_deleted)`,
				`CREATE TABLE IF NOT EXISTS transfer_tasks (
					id VARCHAR(36) PRIMARY KEY,
					type VARCHAR(16) NOT NULL,
					file_id VARCHAR(36),
					client_id VARCHAR(128),
					"offset" BIGINT NOT NULL DEFAULT 0,
					total_size BIGINT NOT NULL DEFAULT 0,
					status VARCHAR(16) NOT NULL DEFAULT 'pending',
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
				)`,
				`CREATE INDEX IF NOT EXISTS idx_tasks_file_id ON transfer_tasks(file_id)`,
				`CREATE INDEX IF NOT EXISTS idx_tasks_client_id ON transfer_tasks(client_id)`,
				`CREATE INDEX IF NOT EXISTS idx_tasks_status ON transfer_tasks(status)`,
				`CREATE TABLE IF NOT EXISTS audit_log (
					id VARCHAR(36) PRIMARY KEY,
					timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					operation VARCHAR(32) NOT NULL,
					resource_path VARCHAR(1024) NOT NULL,
					user_identifier VARCHAR(128),
					client_ip VARCHAR(64),
					user_agent VARCHAR(256),
					success BOOLEAN NOT NULL DEFAULT TRUE,
					details TEXT
				)`,
				`CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp)`,
				`CREATE INDEX IF NOT EXISTS idx_audit_operation ON audit_log(operation)`,
				`CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_log(resource_path)`,
				`CREATE TABLE IF NOT EXISTS api_keys (
					id VARCHAR(36) PRIMARY KEY,
					key_hash VARCHAR(256) NOT NULL UNIQUE,
					name VARCHAR(128) NOT NULL,
					description VARCHAR(512),
					permissions TEXT,
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
					expires_at TIMESTAMP,
					last_used_at TIMESTAMP,
					is_active BOOLEAN NOT NULL DEFAULT TRUE
				)`,
				`CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)`,
				`CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active)`,
			}
			for _, stmt := range statements {
				if _, err := queryDB.Exec(stmt); err != nil {
					return fmt.Errorf("failed to execute migration statement: %w", err)
				}
			}
			return nil
		},
	})
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

	// Clean up orphaned temp directories from previous runs
	tempDir := os.TempDir()
	entries, _ := os.ReadDir(tempDir)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "fsserver-uploads-") {
			oldPath := filepath.Join(tempDir, entry.Name())
			if err := os.RemoveAll(oldPath); err != nil {
				logger.Warn("Failed to clean up orphaned temp directory %s: %v", oldPath, err)
			}
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
	dirSvc := directory.NewDirectoryManagerWithStore(queryDB, store)
	flSvc := filelist.NewFileListService(queryDB)
	transferSvc := transfer.NewFileTransferServiceWithRedis(store, queryDB, sessionStore, distLock)
	transferSvc.SetCryptoService(cryptoSvc)

	// Start session cleanup
	transferSvc.StartCleanupThread(60, 7200)
	defer transferSvc.StopCleanupThread()

	// Cleanup service
	cleanupSvc := database.NewCleanupService(queryDB, store, 60, 30) // every 60 min, 30 day retention
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
