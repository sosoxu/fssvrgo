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
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
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
	if err := database.InitTables(queryDB); err != nil {
		logger.Error("Failed to initialize database tables: %v", err)
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

	// Auth
	authSvc := auth.NewAuthService()
	authSvc.Init(cfg.Auth.Enabled, cfg.Auth.Secret)

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
	var cacheSvc *cache.Cache
	if cfg.Cache.Enabled {
		cacheSvc = cache.NewCache(cfg.Cache.TTL, cfg.Cache.MaxSize)
		logger.Info("Cache enabled (type=%s, ttl=%d, max_size=%d)", cfg.Cache.Type, cfg.Cache.TTL, cfg.Cache.MaxSize)
	}

	// Metrics
	var metricsSvc *metrics.Metrics
	metricsSvc = metrics.NewMetrics()

	// Services
	fm := filemanager.NewFileManagerWithDistLock(store, queryDB, distLock)
	dirSvc := directory.NewDirectoryManager(queryDB)
	flSvc := filelist.NewFileListService(queryDB)
	transferSvc := transfer.NewFileTransferServiceWithRedis(store, queryDB, sessionStore, distLock)

	// Start session cleanup
	transferSvc.StartCleanupThread(60, 7200)
	defer transferSvc.StopCleanupThread()

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
