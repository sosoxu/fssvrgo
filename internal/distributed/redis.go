package distributed

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisManager struct {
	client *redis.Client
}

func NewRedisManager(addr, password string, db, poolSize int) (*RedisManager, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     poolSize,
		MinIdleConns: poolSize / 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisManager{client: client}, nil
}

func (m *RedisManager) GetClient() *redis.Client {
	return m.client
}

func (m *RedisManager) GetLock() DistributedLock {
	return NewRedisDistributedLock(m.client)
}

func (m *RedisManager) GetSessionStore() SessionStore {
	return NewRedisSessionStore(m.client)
}

func (m *RedisManager) Close() error {
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}
