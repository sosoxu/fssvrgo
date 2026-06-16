package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type SessionStore interface {
	Set(ctx context.Context, prefix, key string, value interface{}, ttl time.Duration) error
	Get(ctx context.Context, prefix, key string, dest interface{}) error
	Delete(ctx context.Context, prefix, key string) error
	Exists(ctx context.Context, prefix, key string) (bool, error)
}

type RedisSessionStore struct {
	client *redis.Client
}

func NewRedisSessionStore(client *redis.Client) *RedisSessionStore {
	return &RedisSessionStore{client: client}
}

func (s *RedisSessionStore) Set(ctx context.Context, prefix, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}
	redisKey := fmt.Sprintf("fsserver:session:%s:%s", prefix, key)
	return s.client.Set(ctx, redisKey, data, ttl).Err()
}

func (s *RedisSessionStore) Get(ctx context.Context, prefix, key string, dest interface{}) error {
	redisKey := fmt.Sprintf("fsserver:session:%s:%s", prefix, key)
	data, err := s.client.Get(ctx, redisKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("session not found: %s", key)
		}
		return fmt.Errorf("failed to get session: %w", err)
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("failed to unmarshal session: %w", err)
	}
	return nil
}

func (s *RedisSessionStore) Delete(ctx context.Context, prefix, key string) error {
	redisKey := fmt.Sprintf("fsserver:session:%s:%s", prefix, key)
	return s.client.Del(ctx, redisKey).Err()
}

func (s *RedisSessionStore) Exists(ctx context.Context, prefix, key string) (bool, error) {
	redisKey := fmt.Sprintf("fsserver:session:%s:%s", prefix, key)
	result, err := s.client.Exists(ctx, redisKey).Result()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

type MemorySessionStore struct {
	mu   sync.RWMutex
	data map[string][]byte
	ttls map[string]time.Time
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		data: make(map[string][]byte),
		ttls: make(map[string]time.Time),
	}
}

func (s *MemorySessionStore) Set(ctx context.Context, prefix, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}
	storeKey := fmt.Sprintf("%s:%s", prefix, key)
	s.mu.Lock()
	s.data[storeKey] = data
	if ttl > 0 {
		s.ttls[storeKey] = time.Now().Add(ttl)
	}
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionStore) Get(ctx context.Context, prefix, key string, dest interface{}) error {
	storeKey := fmt.Sprintf("%s:%s", prefix, key)
	s.mu.RLock()
	data, exists := s.data[storeKey]
	if !exists {
		s.mu.RUnlock()
		return fmt.Errorf("session not found: %s", key)
	}
	if ttl, hasTTL := s.ttls[storeKey]; hasTTL && time.Now().After(ttl) {
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, storeKey)
		delete(s.ttls, storeKey)
		s.mu.Unlock()
		return fmt.Errorf("session not found: %s", key)
	}
	s.mu.RUnlock()
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("failed to unmarshal session: %w", err)
	}
	return nil
}

func (s *MemorySessionStore) Delete(ctx context.Context, prefix, key string) error {
	storeKey := fmt.Sprintf("%s:%s", prefix, key)
	s.mu.Lock()
	delete(s.data, storeKey)
	delete(s.ttls, storeKey)
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionStore) Exists(ctx context.Context, prefix, key string) (bool, error) {
	storeKey := fmt.Sprintf("%s:%s", prefix, key)
	s.mu.RLock()
	_, exists := s.data[storeKey]
	if !exists {
		s.mu.RUnlock()
		return false, nil
	}
	if ttl, hasTTL := s.ttls[storeKey]; hasTTL && time.Now().After(ttl) {
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, storeKey)
		delete(s.ttls, storeKey)
		s.mu.Unlock()
		return false, nil
	}
	s.mu.RUnlock()
	return true, nil
}
