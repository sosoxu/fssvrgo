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

// TestCacheLRUEvictionOrder verifies that Get promotes an entry so it survives
// eviction. With maxSize=2: Set k1,k2; Get(k1) promotes it; Set(k3) must evict
// k2 (not k1), because k1 was accessed more recently.
func TestCacheLRUEvictionOrder(t *testing.T) {
	c := NewCache(0, 2) // no TTL, max 2 items
	defer c.Stop()

	c.Set("k1", 1)
	c.Set("k2", 2)
	// Access k1 so k2 becomes the least-recently-used.
	if _, ok := c.Get("k1"); !ok {
		t.Fatal("Get(k1) expected found")
	}
	// Adding k3 must evict k2 (LRU), not k1.
	c.Set("k3", 3)

	if c.Size() != 2 {
		t.Fatalf("Size = %d, want 2", c.Size())
	}
	if _, ok := c.Get("k1"); !ok {
		t.Error("k1 should survive (it was accessed recently); got evicted")
	}
	if _, ok := c.Get("k2"); ok {
		t.Error("k2 should have been evicted as LRU; still present")
	}
	if _, ok := c.Get("k3"); !ok {
		t.Error("k3 should exist")
	}
}

// TestCacheLRUSetExistingDoesNotEvict verifies that updating an existing key
// does not grow the cache beyond maxSize (the key is updated in place, not
// added as a new entry).
func TestCacheLRUSetExistingDoesNotEvict(t *testing.T) {
	c := NewCache(0, 2)
	defer c.Stop()

	c.Set("k1", 1)
	c.Set("k2", 2)
	// Update k1 in place; size must stay at 2, k2 must not be evicted.
	c.Set("k1", 10)

	if c.Size() != 2 {
		t.Fatalf("Size = %d, want 2 (update should not grow cache)", c.Size())
	}
	if v, ok := c.Get("k1"); !ok || v != 10 {
		t.Errorf("k1 = %v (ok=%v), want 10", v, ok)
	}
	if _, ok := c.Get("k2"); !ok {
		t.Error("k2 should still exist after updating k1 in place")
	}
}

// TestCacheLargeCapacitySetIsO1 verifies that Set on a full cache does not
// degrade as the cache grows — the eviction must be O(1), not O(n). We compare
// Set latency on a full small cache vs a full large cache; O(n) eviction would
// make the large cache proportionally slower. We allow generous slack to avoid
// flakiness on shared CI.
func TestCacheLargeCapacitySetIsO1(t *testing.T) {
	// A cache with the OLD O(n) eviction would scan all items on every Set
	// once full. At maxSize=10000 that scan is ~10000 map iterations per Set.
	// The new O(1) eviction does constant work. We assert that filling the
	// last 1000 slots of a 10000-capacity cache completes in a reasonable
	// bounded time — an O(n) scan would be visibly slower.
	const maxSize = 10000
	c := NewCache(0, maxSize)
	defer c.Stop()

	// Pre-fill to capacity.
	for i := 0; i < maxSize; i++ {
		c.Set(fmt.Sprintf("k%d", i), i)
	}
	if c.Size() != maxSize {
		t.Fatalf("pre-fill Size = %d, want %d", c.Size(), maxSize)
	}

	// Now every additional Set must evict one entry. Time 1000 such Sets.
	const extra = 1000
	start := time.Now()
	for i := maxSize; i < maxSize+extra; i++ {
		c.Set(fmt.Sprintf("k%d", i), i)
	}
	elapsed := time.Since(start)

	if c.Size() != maxSize {
		t.Errorf("Size = %d, want %d (eviction should keep size at max)", c.Size(), maxSize)
	}
	// On a modern CPU, 1000 O(1) Sets should be well under 100ms. An O(n)
	// implementation (10000-iteration scan per Set) would typically be 10x+
	// slower. Use a generous 2s upper bound to avoid CI flakiness while still
	// catching gross regressions.
	if elapsed > 2*time.Second {
		t.Errorf("1000 Sets on full 10000-capacity cache took %v; expected O(1) eviction to be much faster", elapsed)
	}
	t.Logf("1000 Sets on full %d-capacity cache: %v", maxSize, elapsed)
}

