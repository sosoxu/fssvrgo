package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type ServerConfig struct {
	HTTPPort           int      `yaml:"http_port"`
	HTTPSPort          int      `yaml:"https_port"`
	GRPCPort           int      `yaml:"grpc_port"`
	GRPCEnabled        bool     `yaml:"grpc_enabled"`
	Workers            int      `yaml:"workers"`
	MaxUploadSizeMB    int      `yaml:"max_upload_size_mb"`
	MaxChunkSizeMB     int      `yaml:"max_chunk_size_mb"`
	MaxPageSize        int      `yaml:"max_page_size"`
	CORSAllowedOrigins string   `yaml:"cors_allowed_origins"`
	TrustedProxies     []string `yaml:"trusted_proxies"`
}

type LocalStorageConfig struct {
	RootDir string `yaml:"root_dir"`
}

type MinIOStorageConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type StorageConfig struct {
	Type  string             `yaml:"type"`
	Local LocalStorageConfig `yaml:"local"`
	MinIO MinIOStorageConfig `yaml:"minio"`
}

type DatabaseConfig struct {
	Type               string `yaml:"type"`
	Path               string `yaml:"path"`
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
	Name               string `yaml:"name"`
	User               string `yaml:"user"`
	Password           string `yaml:"password"`
	SSLMode            string `yaml:"sslmode"`
	PoolSize           int    `yaml:"pool_size"`
	ConnectionTimeoutMs int   `yaml:"connection_timeout_ms"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	File   string `yaml:"file"`
	Format string `yaml:"format"`
}

type CacheConfig struct {
	Enabled bool   `yaml:"enabled"`
	Type    string `yaml:"type"`
	TTL     int    `yaml:"ttl"`
	MaxSize int    `yaml:"max_size"`
}

type RedisConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	PoolSize int    `yaml:"pool_size"`
}

type EtcdConfig struct {
	Enabled   bool     `yaml:"enabled"`
	Endpoints []string `yaml:"endpoints"`
	Prefix    string   `yaml:"prefix"`
}

type ConsistencyConfig struct {
	Level          string `yaml:"level"`
	ReplicaCount   int    `yaml:"replica_count"`
	ReadQuorum     int    `yaml:"read_quorum"`
	WriteQuorum    int    `yaml:"write_quorum"`
	SyncIntervalMs int    `yaml:"sync_interval_ms"`
}

type DiscoveryConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Type     string `yaml:"type"`
	Address  string `yaml:"address"`
	Interval int    `yaml:"interval"`
}

type AuthConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Secret        string `yaml:"secret"`
	TokenExpiry   int    `yaml:"token_expiry"`
	RefreshExpiry int    `yaml:"refresh_expiry"`
}

type CryptoConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Algorithm  string `yaml:"algorithm"`
	KeyFile    string `yaml:"key_file"`
	Passphrase string `yaml:"passphrase"`
}

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	TLS         TLSConfig         `yaml:"tls"`
	Storage     StorageConfig     `yaml:"storage"`
	Database    DatabaseConfig    `yaml:"database"`
	Logging     LoggingConfig     `yaml:"logging"`
	Cache       CacheConfig       `yaml:"cache"`
	Redis       RedisConfig       `yaml:"redis"`
	Etcd        EtcdConfig        `yaml:"etcd"`
	Consistency ConsistencyConfig `yaml:"consistency"`
	Discovery   DiscoveryConfig   `yaml:"discovery"`
	Auth        AuthConfig        `yaml:"auth"`
	Crypto      CryptoConfig      `yaml:"crypto"`
}

func (c *Config) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	c.applyDefaults()
	return nil
}

func (c *Config) applyDefaults() {
	if c.Server.HTTPPort == 0 {
		c.Server.HTTPPort = 8080
	}
	if c.Server.HTTPSPort == 0 {
		c.Server.HTTPSPort = 8443
	}
	if c.Server.GRPCPort == 0 {
		c.Server.GRPCPort = 9090
	}
	if !c.Server.GRPCEnabled {
		c.Server.GRPCEnabled = true
	}
	if c.Server.Workers == 0 {
		c.Server.Workers = 8
	}
	if c.Server.MaxUploadSizeMB == 0 {
		c.Server.MaxUploadSizeMB = 1024
	}
	if c.Server.MaxChunkSizeMB == 0 {
		c.Server.MaxChunkSizeMB = 64
	}
	if c.Server.MaxPageSize == 0 {
		c.Server.MaxPageSize = 1000
	}
	if c.Server.CORSAllowedOrigins == "" {
		c.Server.CORSAllowedOrigins = "*"
	}
	if c.Storage.Type == "" {
		c.Storage.Type = "local"
	}
	if c.Storage.Local.RootDir == "" {
		c.Storage.Local.RootDir = "/data/fsserver"
	}
	if c.Database.Type == "" {
		c.Database.Type = "sqlite"
	}
	if c.Database.PoolSize == 0 {
		c.Database.PoolSize = 10
	}
	if c.Database.ConnectionTimeoutMs == 0 {
		c.Database.ConnectionTimeoutMs = 5000
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "console"
	}
	if c.Redis.Enabled && c.Redis.PoolSize == 0 {
		c.Redis.PoolSize = 10
	}
}

func (c *Config) Validate() error {
	if err := c.validateServer(); err != nil {
		return err
	}
	if err := c.validateTLS(); err != nil {
		return err
	}
	if err := c.validateStorage(); err != nil {
		return err
	}
	if err := c.validateDatabase(); err != nil {
		return err
	}
	if err := c.validateCache(); err != nil {
		return err
	}
	if err := c.validateRedis(); err != nil {
		return err
	}
	if err := c.validateEtcd(); err != nil {
		return err
	}
	if err := c.validateConsistency(); err != nil {
		return err
	}
	if err := c.validateDiscovery(); err != nil {
		return err
	}
	if err := c.validateAuth(); err != nil {
		return err
	}
	if err := c.validateCrypto(); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateServer() error {
	if c.Server.HTTPPort < 1 || c.Server.HTTPPort > 65535 {
		return fmt.Errorf("server.http_port must be between 1 and 65535, got %d", c.Server.HTTPPort)
	}
	if c.Server.HTTPSPort < 1 || c.Server.HTTPSPort > 65535 {
		return fmt.Errorf("server.https_port must be between 1 and 65535, got %d", c.Server.HTTPSPort)
	}
	if c.Server.GRPCPort < 1 || c.Server.GRPCPort > 65535 {
		return fmt.Errorf("server.grpc_port must be between 1 and 65535, got %d", c.Server.GRPCPort)
	}
	if c.Server.Workers < 1 {
		return fmt.Errorf("server.workers must be at least 1, got %d", c.Server.Workers)
	}
	if c.Server.MaxUploadSizeMB < 1 {
		return fmt.Errorf("server.max_upload_size_mb must be at least 1, got %d", c.Server.MaxUploadSizeMB)
	}
	if c.Server.MaxChunkSizeMB < 1 {
		return fmt.Errorf("server.max_chunk_size_mb must be at least 1, got %d", c.Server.MaxChunkSizeMB)
	}
	if c.Server.MaxChunkSizeMB > c.Server.MaxUploadSizeMB {
		return fmt.Errorf("server.max_chunk_size_mb (%d) must not exceed max_upload_size_mb (%d)", c.Server.MaxChunkSizeMB, c.Server.MaxUploadSizeMB)
	}
	if c.Server.MaxPageSize < 1 || c.Server.MaxPageSize > 10000 {
		return fmt.Errorf("server.max_page_size must be between 1 and 10000, got %d", c.Server.MaxPageSize)
	}
	return nil
}

func (c *Config) validateTLS() error {
	if c.TLS.Enabled {
		if c.TLS.CertFile == "" {
			return fmt.Errorf("tls.cert_file is required when TLS is enabled")
		}
		if c.TLS.KeyFile == "" {
			return fmt.Errorf("tls.key_file is required when TLS is enabled")
		}
	}
	return nil
}

func (c *Config) validateStorage() error {
	validTypes := map[string]bool{"local": true, "minio": true}
	if !validTypes[c.Storage.Type] {
		return fmt.Errorf("storage.type must be \"local\" or \"minio\", got %q", c.Storage.Type)
	}

	if c.Storage.Type == "local" {
		if c.Storage.Local.RootDir == "" || c.Storage.Local.RootDir == "/" {
			return fmt.Errorf("storage.local.root_dir cannot be empty or root '/'")
		}
	}

	if c.Storage.Type == "minio" {
		if c.Storage.MinIO.Endpoint == "" {
			return fmt.Errorf("storage.minio.endpoint is required when storage type is \"minio\"")
		}
		if c.Storage.MinIO.AccessKey == "" {
			return fmt.Errorf("storage.minio.access_key is required when storage type is \"minio\"")
		}
		if c.Storage.MinIO.SecretKey == "" {
			return fmt.Errorf("storage.minio.secret_key is required when storage type is \"minio\"")
		}
		if c.Storage.MinIO.Bucket == "" {
			return fmt.Errorf("storage.minio.bucket is required when storage type is \"minio\"")
		}
	}

	return nil
}

func (c *Config) validateDatabase() error {
	if c.Database.Type != "sqlite" && c.Database.Type != "postgresql" {
		return fmt.Errorf("database.type must be \"sqlite\" or \"postgresql\", got %q", c.Database.Type)
	}
	if c.Database.Type == "sqlite" && c.Database.Path == "" {
		return fmt.Errorf("database.path is required when database.type is \"sqlite\"")
	}
	if c.Database.Type == "postgresql" {
		if c.Database.Host == "" {
			return fmt.Errorf("database.host is required when database.type is \"postgresql\"")
		}
		if c.Database.Port < 1 || c.Database.Port > 65535 {
			return fmt.Errorf("database.port must be between 1 and 65535, got %d", c.Database.Port)
		}
		if c.Database.Name == "" {
			return fmt.Errorf("database.name is required when database.type is \"postgresql\"")
		}
		if c.Database.User == "" {
			return fmt.Errorf("database.user is required when database.type is \"postgresql\"")
		}
	}
	if c.Database.PoolSize < 1 {
		return fmt.Errorf("database.pool_size must be at least 1, got %d", c.Database.PoolSize)
	}
	return nil
}

func (c *Config) validateCache() error {
	if c.Cache.Enabled {
		if c.Cache.Type == "" {
			return fmt.Errorf("cache.type is required when cache is enabled")
		}
		if c.Cache.TTL < 0 {
			return fmt.Errorf("cache.ttl must not be negative, got %d", c.Cache.TTL)
		}
		if c.Cache.MaxSize < 0 {
			return fmt.Errorf("cache.max_size must not be negative, got %d", c.Cache.MaxSize)
		}
	}
	return nil
}

func (c *Config) validateRedis() error {
	if c.Redis.Enabled {
		if c.Redis.Address == "" {
			return fmt.Errorf("redis.address is required when Redis is enabled")
		}
		if c.Redis.PoolSize < 1 {
			return fmt.Errorf("redis.pool_size must be at least 1, got %d", c.Redis.PoolSize)
		}
	}
	return nil
}

func (c *Config) validateEtcd() error {
	if c.Etcd.Enabled {
		if len(c.Etcd.Endpoints) == 0 {
			return fmt.Errorf("etcd.endpoints must not be empty when etcd is enabled")
		}
	}
	return nil
}

func (c *Config) validateConsistency() error {
	if c.Consistency.Level != "" {
		valid := map[string]bool{"none": true, "eventual": true, "strong": true}
		if !valid[c.Consistency.Level] {
			return fmt.Errorf("consistency.level must be \"none\", \"eventual\", or \"strong\", got %q", c.Consistency.Level)
		}
	}
	if c.Consistency.Level != "none" && c.Consistency.Level != "" {
		if c.Consistency.SyncIntervalMs < 1 {
			c.Consistency.SyncIntervalMs = 5000 // default 5 seconds
		}
	}
	return nil
}

func (c *Config) validateDiscovery() error {
	if c.Discovery.Enabled {
		if c.Discovery.Type == "" {
			return fmt.Errorf("discovery.type is required when discovery is enabled")
		}
		if c.Discovery.Address == "" {
			return fmt.Errorf("discovery.address is required when discovery is enabled")
		}
		if c.Discovery.Interval < 1 {
			return fmt.Errorf("discovery.interval must be at least 1, got %d", c.Discovery.Interval)
		}
	}
	return nil
}

func (c *Config) validateAuth() error {
	if c.Auth.Enabled {
		if c.Auth.Secret == "" {
			return fmt.Errorf("auth.secret is required when auth is enabled")
		}
		if c.Auth.TokenExpiry < 1 {
			return fmt.Errorf("auth.token_expiry must be at least 1, got %d", c.Auth.TokenExpiry)
		}
		if c.Auth.RefreshExpiry < 1 {
			return fmt.Errorf("auth.refresh_expiry must be at least 1, got %d", c.Auth.RefreshExpiry)
		}
	}
	return nil
}

func (c *Config) validateCrypto() error {
	if c.Crypto.Enabled {
		if c.Crypto.Algorithm == "" {
			return fmt.Errorf("crypto.algorithm is required when crypto is enabled")
		}
		if c.Crypto.KeyFile == "" && c.Crypto.Passphrase == "" {
			return fmt.Errorf("crypto.key_file or crypto.passphrase is required when crypto is enabled")
		}
	}
	return nil
}

func (c *Config) GetServer() *ServerConfig {
	return &c.Server
}

func (c *Config) GetTLS() *TLSConfig {
	return &c.TLS
}

func (c *Config) GetStorage() *StorageConfig {
	return &c.Storage
}

func (c *Config) GetDatabase() *DatabaseConfig {
	return &c.Database
}

func (c *Config) GetLogging() *LoggingConfig {
	return &c.Logging
}

func (c *Config) GetCache() *CacheConfig {
	return &c.Cache
}

func (c *Config) GetRedis() *RedisConfig {
	return &c.Redis
}

func (c *Config) GetEtcd() *EtcdConfig {
	return &c.Etcd
}

func (c *Config) GetConsistency() *ConsistencyConfig {
	return &c.Consistency
}

func (c *Config) GetDiscovery() *DiscoveryConfig {
	return &c.Discovery
}

func (c *Config) GetAuth() *AuthConfig {
	return &c.Auth
}

func (c *Config) GetCrypto() *CryptoConfig {
	return &c.Crypto
}
