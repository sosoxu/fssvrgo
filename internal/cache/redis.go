package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisCache(addr, password string, db, poolSize int, ttl int64) *RedisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
		PoolSize: poolSize,
	})
	return &RedisCache{
		client: rdb,
		ttl:    time.Duration(ttl) * time.Second,
	}
}

func (c *RedisCache) Get(ctx context.Context, key string) (interface{}, bool) {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return nil, false
	}
	var result interface{}
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return nil, false
	}
	return result, true
}

func (c *RedisCache) Set(ctx context.Context, key string, value interface{}) {
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	c.client.Set(ctx, key, data, c.ttl)
}

func (c *RedisCache) Delete(ctx context.Context, key string) {
	c.client.Del(ctx, key)
}

func (c *RedisCache) Exists(ctx context.Context, key string) bool {
	val, err := c.client.Exists(ctx, key).Result()
	return err == nil && val > 0
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}
