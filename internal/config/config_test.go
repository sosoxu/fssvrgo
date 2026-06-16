package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	return path
}

func validConfig() *Config {
	return &Config{
		Server: ServerConfig{
			HTTPPort:        8080,
			HTTPSPort:       8443,
			GRPCPort:        9090,
			GRPCEnabled:     true,
			Workers:         8,
			MaxUploadSizeMB: 1024,
			MaxChunkSizeMB:  64,
			MaxPageSize:     1000,
		},
		Storage: StorageConfig{
			Type: "local",
			Local: LocalStorageConfig{
				RootDir: "/data/test",
			},
		},
		Database: DatabaseConfig{
			Type:     "sqlite",
			Path:     "/data/test.db",
			PoolSize: 10,
		},
	}
}

func TestValidConfig(t *testing.T) {
	path := writeTestConfig(t, `server:
  http_port: 8080
  https_port: 8443
  grpc_port: 9090
  grpc_enabled: true
  workers: 8
  max_upload_size_mb: 1024
  max_chunk_size_mb: 64
  max_page_size: 1000
storage:
  type: local
  local:
    root_dir: /data/test
database:
  type: sqlite
  path: /data/test.db
  pool_size: 10
logging:
  level: info
auth:
  enabled: false
crypto:
  enabled: false
`)

	cfg := &Config{}
	if err := cfg.Load(path); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
}

func TestInvalidPort(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"zero", 0},
		{"too large", 65536},
		{"negative", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Server.HTTPPort = tt.port
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected validation error for port %d", tt.port)
			}
		})
	}
}

func TestInvalidWorkers(t *testing.T) {
	cfg := validConfig()
	cfg.Server.Workers = 0
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error for workers=0")
	}
}

func TestInvalidSizeLimits(t *testing.T) {
	cfg := validConfig()
	cfg.Server.MaxUploadSizeMB = 0
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error for max_upload_size_mb=0")
	}
}

func TestChunkExceedsUploadSize(t *testing.T) {
	cfg := validConfig()
	cfg.Server.MaxChunkSizeMB = 2048
	cfg.Server.MaxUploadSizeMB = 1024
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error when chunk size exceeds upload size")
	}
}

func TestInvalidPageSize(t *testing.T) {
	tests := []struct {
		name     string
		pageSize int
	}{
		{"zero", 0},
		{"too large", 10001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Server.MaxPageSize = tt.pageSize
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected validation error for page_size=%d", tt.pageSize)
			}
		})
	}
}

func TestTLSWithoutCert(t *testing.T) {
	cfg := validConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = ""
	cfg.TLS.KeyFile = ""
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error when TLS enabled without cert")
	}
}

func TestInvalidStorageType(t *testing.T) {
	cfg := validConfig()
	cfg.Storage.Type = "s3"
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error for storage type s3")
	}
}

func TestEmptyRootDir(t *testing.T) {
	cfg := validConfig()
	cfg.Storage.Local.RootDir = ""
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error for empty root_dir")
	}
}

func TestInvalidDatabaseType(t *testing.T) {
	cfg := validConfig()
	cfg.Database.Type = "mysql"
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error for database type mysql")
	}
}

func TestInvalidPoolSize(t *testing.T) {
	cfg := validConfig()
	cfg.Database.PoolSize = 0
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected validation error for pool_size=0")
	}
}

func TestConfigFieldAccess(t *testing.T) {
	cfg := validConfig()
	cfg.TLS = TLSConfig{Enabled: true, CertFile: "/cert.pem", KeyFile: "/key.pem"}
	cfg.Logging = LoggingConfig{Level: "debug", File: "/var/log/test.log", Format: "json"}
	cfg.Cache = CacheConfig{Enabled: true, Type: "memory", TTL: 300, MaxSize: 1000}
	cfg.Redis = RedisConfig{Enabled: false, Address: "localhost:6379", PoolSize: 10}
	cfg.Etcd = EtcdConfig{Enabled: false, Endpoints: []string{"http://localhost:2379"}}
	cfg.Consistency = ConsistencyConfig{Level: "strong", ReplicaCount: 3}
	cfg.Discovery = DiscoveryConfig{Enabled: false, Type: "etcd", Address: "http://localhost:2379"}
	cfg.Auth = AuthConfig{Enabled: true, Secret: "mysecret", TokenExpiry: 3600, RefreshExpiry: 86400}
	cfg.Crypto = CryptoConfig{Enabled: false, Algorithm: "aes-256-gcm"}

	if cfg.GetServer().HTTPPort != 8080 {
		t.Errorf("GetServer().HTTPPort expected 8080, got %d", cfg.GetServer().HTTPPort)
	}
	if cfg.GetTLS().CertFile != "/cert.pem" {
		t.Errorf("GetTLS().CertFile expected /cert.pem, got %s", cfg.GetTLS().CertFile)
	}
	if cfg.GetStorage().Type != "local" {
		t.Errorf("GetStorage().Type expected local, got %s", cfg.GetStorage().Type)
	}
	if cfg.GetDatabase().Type != "sqlite" {
		t.Errorf("GetDatabase().Type expected sqlite, got %s", cfg.GetDatabase().Type)
	}
	if cfg.GetLogging().Level != "debug" {
		t.Errorf("GetLogging().Level expected debug, got %s", cfg.GetLogging().Level)
	}
	if cfg.GetCache().Type != "memory" {
		t.Errorf("GetCache().Type expected memory, got %s", cfg.GetCache().Type)
	}
	if cfg.GetRedis().Address != "localhost:6379" {
		t.Errorf("GetRedis().Address expected localhost:6379, got %s", cfg.GetRedis().Address)
	}
	if len(cfg.GetEtcd().Endpoints) != 1 {
		t.Errorf("GetEtcd().Endpoints expected 1, got %d", len(cfg.GetEtcd().Endpoints))
	}
	if cfg.GetConsistency().Level != "strong" {
		t.Errorf("GetConsistency().Level expected strong, got %s", cfg.GetConsistency().Level)
	}
	if cfg.GetDiscovery().Type != "etcd" {
		t.Errorf("GetDiscovery().Type expected etcd, got %s", cfg.GetDiscovery().Type)
	}
	if cfg.GetAuth().Secret != "mysecret" {
		t.Errorf("GetAuth().Secret expected mysecret, got %s", cfg.GetAuth().Secret)
	}
	if cfg.GetCrypto().Algorithm != "aes-256-gcm" {
		t.Errorf("GetCrypto().Algorithm expected aes-256-gcm, got %s", cfg.GetCrypto().Algorithm)
	}
}
