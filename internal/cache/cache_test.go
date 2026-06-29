package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCacheGetSet(t *testing.T) {
	c := NewCache(0, 0)
	defer c.Stop()

	c.Set("key1", "value1")
	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("Get(key1) expected to find value, got not found")
	}
	if val != "value1" {
		t.Errorf("Get(key1) = %v, want %v", val, "value1")
	}

	if _, ok := c.Get("missing"); ok {
		t.Error("Get(missing) expected not found, got found")
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache(0, 0)
	defer c.Stop()

	c.Set("key", "value")
	if _, ok := c.Get("key"); !ok {
		t.Fatal("expected key to exist before delete")
	}

	c.Delete("key")
	if _, ok := c.Get("key"); ok {
		t.Error("Get(key) expected not found after Delete, got found")
	}

	// deleting a missing key should not panic
	c.Delete("not-exist")
}

func TestCacheClear(t *testing.T) {
	c := NewCache(0, 0)
	defer c.Stop()

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	if c.Size() != 3 {
		t.Fatalf("Size before clear = %d, want 3", c.Size())
	}

	c.Clear()
	if c.Size() != 0 {
		t.Errorf("Size after clear = %d, want 0", c.Size())
	}
	if _, ok := c.Get("a"); ok {
		t.Error("Get(a) expected not found after Clear")
	}
}

func TestCacheTTL(t *testing.T) {
	c := NewCache(1, 0) // 1 second TTL
	defer c.Stop()

	c.Set("temp", "data")
	if _, ok := c.Get("temp"); !ok {
		t.Fatal("Get immediately after Set expected found")
	}

	time.Sleep(1100 * time.Millisecond)
	if _, ok := c.Get("temp"); ok {
		t.Error("Get after TTL expiry expected not found, got found")
	}
}

func TestCacheMaxSize(t *testing.T) {
	c := NewCache(60, 2) // no expiry during test, max 2 items
	defer c.Stop()

	c.Set("k1", 1)
	time.Sleep(10 * time.Millisecond)
	c.Set("k2", 2)
	time.Sleep(10 * time.Millisecond)
	// adding k3 should evict the oldest (k1)
	c.Set("k3", 3)

	if c.Size() != 2 {
		t.Fatalf("Size = %d, want 2 after eviction", c.Size())
	}
	if _, ok := c.Get("k1"); ok {
		t.Error("k1 should have been evicted")
	}
	if _, ok := c.Get("k2"); !ok {
		t.Error("k2 should still exist")
	}
	if _, ok := c.Get("k3"); !ok {
		t.Error("k3 should exist")
	}
}

func TestCacheSize(t *testing.T) {
	c := NewCache(0, 0)
	defer c.Stop()

	if c.Size() != 0 {
		t.Fatalf("initial Size = %d, want 0", c.Size())
	}

	c.Set("a", 1)
	if c.Size() != 1 {
		t.Errorf("Size = %d, want 1", c.Size())
	}
	c.Set("b", 2)
	if c.Size() != 2 {
		t.Errorf("Size = %d, want 2", c.Size())
	}
	c.Delete("a")
	if c.Size() != 1 {
		t.Errorf("Size after delete = %d, want 1", c.Size())
	}
}

func TestCacheConcurrent(t *testing.T) {
	c := NewCache(0, 0)
	defer c.Stop()

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			c.Set(key, idx)
			if v, ok := c.Get(key); !ok || v != idx {
				t.Errorf("key %s: expected %d (ok=%v), got %v", key, idx, ok, v)
			}
		}(i)
	}
	wg.Wait()

	if c.Size() != n {
		t.Errorf("Size after concurrent writes = %d, want %d", c.Size(), n)
	}
}
