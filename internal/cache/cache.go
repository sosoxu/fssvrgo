package cache

import (
	"container/list"
	"sync"
	"time"
)

// CacheAdapter defines the interface for cache backends.
type CacheAdapter interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Delete(key string)
	Exists(key string) bool
}

// entry holds a cached value, its expiry, and its position in the LRU list.
type entry struct {
	key       string
	value     interface{}
	expiresAt time.Time
	elem      *list.Element // pointer into Cache.lru for O(1) removal
}

// Cache is an in-memory cache with TTL and a strict O(1) LRU eviction policy.
// When the number of entries reaches maxSize, the least-recently-used entry is
// evicted on the next Set. Get promotes the accessed entry to the front of the
// LRU list so frequently-accessed keys survive eviction.
//
// All operations are goroutine-safe via a single sync.RWMutex. The hot path
// (Set eviction, Get promotion) is O(1) — no map scans.
type Cache struct {
	mu      sync.RWMutex
	items   map[string]*entry
	lru     *list.List // front = most recently used; back = least recently used
	ttl     int64
	maxSize int
	stopCh  chan struct{}
}

func NewCache(ttl int64, maxSize int) *Cache {
	c := &Cache{
		items:   make(map[string]*entry),
		lru:     list.New(),
		ttl:     ttl,
		maxSize: maxSize,
		stopCh:  make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.Cleanup()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Cache) Stop() {
	close(c.stopCh)
}

// Get returns the cached value for key, or (nil, false) if absent or expired.
// A successful Get promotes the entry to the front of the LRU list.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		return nil, false
	}

	if c.ttl > 0 && time.Now().After(item.expiresAt) {
		// Expired: lazily evict so callers don't see stale entries.
		c.removeElement(item)
		return nil, false
	}

	// Promote to front (most recently used). O(1).
	c.lru.MoveToFront(item.elem)
	return item.value, true
}

// Set stores value under key, evicting the least-recently-used entry if the
// cache is at capacity. If key already exists, its value and expiry are updated
// and it is promoted to the front of the LRU list. All of this is O(1).
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing entry in place.
	if existing, ok := c.items[key]; ok {
		existing.value = value
		if c.ttl > 0 {
			existing.expiresAt = time.Now().Add(time.Duration(c.ttl) * time.Second)
		} else {
			existing.expiresAt = time.Time{}
		}
		c.lru.MoveToFront(existing.elem)
		return
	}

	// Evict LRU (back of list) if at capacity. Only evict when maxSize > 0;
	// maxSize == 0 means unbounded (legacy behavior preserved).
	if c.maxSize > 0 && len(c.items) >= c.maxSize {
		if back := c.lru.Back(); back != nil {
			c.removeElement(back.Value.(*entry))
		}
	}

	expiresAt := time.Time{}
	if c.ttl > 0 {
		expiresAt = time.Now().Add(time.Duration(c.ttl) * time.Second)
	}

	e := &entry{
		key:       key,
		value:     value,
		expiresAt: expiresAt,
	}
	e.elem = c.lru.PushFront(e)
	c.items[key] = e
}

// removeElement removes an entry from both the map and the LRU list. The caller
// must hold c.mu (write lock).
func (c *Cache) removeElement(e *entry) {
	c.lru.Remove(e.elem)
	delete(c.items, e.key)
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.removeElement(e)
	}
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*entry)
	c.lru.Init()
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Cleanup removes all expired entries. It scans the map once (O(n)) but is only
// called periodically (every minute) by the cleanup loop, so it does not affect
// the hot path.
func (c *Cache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for _, item := range c.items {
		if c.ttl > 0 && now.After(item.expiresAt) {
			c.removeElement(item)
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

func (c *Cache) Exists(key string) bool {
	return c.Has(key)
}
