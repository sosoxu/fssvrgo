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
	ctx    context.Context
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
		ctx:    context.Background(),
	}
}

func (c *RedisCache) Get(key string) (interface{}, bool) {
	val, err := c.client.Get(c.ctx, key).Result()
	if err != nil {
		return nil, false
	}
	var result interface{}
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return nil, false
	}
	return result, true
}

func (c *RedisCache) Set(key string, value interface{}) {
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	c.client.Set(c.ctx, key, data, c.ttl)
}

func (c *RedisCache) Delete(key string) {
	c.client.Del(c.ctx, key)
}

func (c *RedisCache) Exists(key string) bool {
	val, err := c.client.Exists(c.ctx, key).Result()
	return err == nil && val > 0
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}
