package cache

import (
	"sync"
	"time"
)

type entry struct {
	value     interface{}
	expiresAt time.Time
}

type Cache struct {
	mu      sync.RWMutex
	items   map[string]*entry
	ttl     int
	maxSize int
}

func NewCache(ttl, maxSize int) *Cache {
	return &Cache{
		items:   make(map[string]*entry),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists {
		return nil, false
	}

	if c.ttl > 0 && time.Now().After(item.expiresAt) {
		return nil, false
	}

	return item.value, true
}

func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.maxSize > 0 && len(c.items) >= c.maxSize {
		var oldestKey string
		var oldestTime time.Time
		for k, v := range c.items {
			if oldestKey == "" || v.expiresAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.expiresAt
			}
		}
		delete(c.items, oldestKey)
	}

	expiresAt := time.Time{}
	if c.ttl > 0 {
		expiresAt = time.Now().Add(time.Duration(c.ttl) * time.Second)
	}

	c.items[key] = &entry{
		value:     value,
		expiresAt: expiresAt,
	}
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*entry)
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *Cache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, item := range c.items {
		if c.ttl > 0 && now.After(item.expiresAt) {
			delete(c.items, key)
		}
	}
}

func (c *Cache) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists {
		return false
	}

	if c.ttl > 0 && time.Now().After(item.expiresAt) {
		return false
	}

	return true
}
