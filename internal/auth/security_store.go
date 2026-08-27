package auth

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type SecurityStateStore interface {
	IsRefreshRevoked(ctx context.Context, jti string) (bool, error)
	ConsumeRefresh(ctx context.Context, jti string, ttl time.Duration) (bool, error)
	AuthFailureCount(ctx context.Context, clientIP string) (int, error)
	RecordAuthFailure(ctx context.Context, clientIP string, ttl time.Duration) error
	ClearAuthFailure(ctx context.Context, clientIP string) error
}

type RedisSecurityStateStore struct {
	client *redis.Client
}

func NewRedisSecurityStateStore(client *redis.Client) *RedisSecurityStateStore {
	return &RedisSecurityStateStore{client: client}
}

func (s *RedisSecurityStateStore) IsRefreshRevoked(ctx context.Context, jti string) (bool, error) {
	n, err := s.client.Exists(ctx, "fsserver:auth:refresh-revoked:"+jti).Result()
	return n > 0, err
}

func (s *RedisSecurityStateStore) ConsumeRefresh(ctx context.Context, jti string, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, "fsserver:auth:refresh-revoked:"+jti, "1", ttl).Result()
}

func authFailureKey(clientIP string) string {
	return "fsserver:auth:failures:" + clientIP
}

func (s *RedisSecurityStateStore) AuthFailureCount(ctx context.Context, clientIP string) (int, error) {
	value, err := s.client.Get(ctx, authFailureKey(clientIP)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(value)
}

var recordFailureScript = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return count
`)

func (s *RedisSecurityStateStore) RecordAuthFailure(ctx context.Context, clientIP string, ttl time.Duration) error {
	return recordFailureScript.Run(ctx, s.client, []string{authFailureKey(clientIP)}, ttl.Milliseconds()).Err()
}

func (s *RedisSecurityStateStore) ClearAuthFailure(ctx context.Context, clientIP string) error {
	return s.client.Del(ctx, authFailureKey(clientIP)).Err()
}

type memorySecurityEntry struct {
	count  int
	expiry time.Time
}

type MemorySecurityStateStore struct {
	mu       sync.Mutex
	revoked  map[string]time.Time
	failures map[string]memorySecurityEntry
}

func NewMemorySecurityStateStore() *MemorySecurityStateStore {
	return &MemorySecurityStateStore{revoked: make(map[string]time.Time), failures: make(map[string]memorySecurityEntry)}
}

func (s *MemorySecurityStateStore) IsRefreshRevoked(_ context.Context, jti string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expiry, ok := s.revoked[jti]
	if ok && time.Now().After(expiry) {
		delete(s.revoked, jti)
		return false, nil
	}
	return ok, nil
}

func (s *MemorySecurityStateStore) ConsumeRefresh(_ context.Context, jti string, ttl time.Duration) (bool, error) {
	if jti == "" {
		return false, fmt.Errorf("refresh token id cannot be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if expiry, ok := s.revoked[jti]; ok && time.Now().Before(expiry) {
		return false, nil
	}
	s.revoked[jti] = time.Now().Add(ttl)
	return true, nil
}

func (s *MemorySecurityStateStore) AuthFailureCount(_ context.Context, clientIP string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.failures[clientIP]
	if !ok || time.Now().After(entry.expiry) {
		delete(s.failures, clientIP)
		return 0, nil
	}
	return entry.count, nil
}

func (s *MemorySecurityStateStore) RecordAuthFailure(_ context.Context, clientIP string, ttl time.Duration) error {
	s.mu.Lock()
	entry := s.failures[clientIP]
	if time.Now().After(entry.expiry) {
		entry.count = 0
	}
	entry.count++
	entry.expiry = time.Now().Add(ttl)
	s.failures[clientIP] = entry
	s.mu.Unlock()
	return nil
}

func (s *MemorySecurityStateStore) ClearAuthFailure(_ context.Context, clientIP string) error {
	s.mu.Lock()
	delete(s.failures, clientIP)
	s.mu.Unlock()
	return nil
}
