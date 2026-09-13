package pathlock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockSerializesSameKey(t *testing.T) {
	locks := New()
	var concurrent, maxConcurrent int32

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release := locks.Lock(LevelFile, "/same.txt")
			defer release()

			now := atomic.AddInt32(&concurrent, 1)
			for {
				prev := atomic.LoadInt32(&maxConcurrent)
				if now <= prev || atomic.CompareAndSwapInt32(&maxConcurrent, prev, now) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
		}()
	}
	wg.Wait()

	if maxConcurrent != 1 {
		t.Errorf("max concurrent holders = %d, want 1 (same key must serialize)", maxConcurrent)
	}
}

func TestDifferentKeysDoNotBlock(t *testing.T) {
	locks := New()
	release := locks.Lock(LevelFile, "/a.txt")
	defer release()

	done := make(chan struct{})
	go func() {
		other := locks.Lock(LevelFile, "/b.txt")
		other()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a different path blocked on the held lock")
	}
}

func TestLevelsAreSeparateKeysForSamePath(t *testing.T) {
	locks := New()
	// A service holds the file level for a path and then calls into storage,
	// which takes the object level for the same path. Non-reentrant mutexes
	// require these to be distinct keys, or the call would self-deadlock.
	releaseFile := locks.Lock(LevelFile, "/a.txt")
	defer releaseFile()

	done := make(chan struct{})
	go func() {
		releaseObject := locks.Lock(LevelObject, "/a.txt")
		releaseObject()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("object-level lock for the same path must not collide with the file-level lock")
	}
}

// TestLockManyUsesGlobalOrder is a deadlock probe: one goroutine acquires
// {a, b} and another acquires {b, a}. Without a shared sort the two would
// deadlock; with it the second simply waits.
func TestLockManyUsesGlobalOrder(t *testing.T) {
	locks := New()
	var counter int32

	run := func(first, second string) {
		release := locks.LockMany(K(LevelObject, first), K(LevelObject, second))
		defer release()
		atomic.AddInt32(&counter, 1)
		time.Sleep(time.Millisecond)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				run("/a/one.txt", "/b/two.txt")
				return
			}
			run("/b/two.txt", "/a/one.txt")
		}(i)
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("LockMany deadlocked on opposite argument orders")
	}

	if got := atomic.LoadInt32(&counter); got != 16 {
		t.Errorf("critical sections = %d, want 16", got)
	}
}

func TestLockManyDeduplicatesAndSorts(t *testing.T) {
	got := sortedKeys([]Key{
		K(LevelObject, "/b.txt"),
		K(LevelFile, "/b.txt"),
		K(LevelObject, "/a.txt"),
		K(LevelObject, "/b.txt"), // duplicate
	})

	want := []Key{
		K(LevelFile, "/b.txt"),
		K(LevelObject, "/a.txt"),
		K(LevelObject, "/b.txt"),
	}
	if len(got) != len(want) {
		t.Fatalf("sortedKeys returned %d keys, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("key %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestCanonicalPathUnifiesSpellings(t *testing.T) {
	spellings := []string{"a/b.txt", "/a/b.txt", "/a/b.txt/", " /a/b.txt "}
	for _, s := range spellings {
		if got := CanonicalPath(s); got != "/a/b.txt" {
			t.Errorf("CanonicalPath(%q) = %q, want /a/b.txt", s, got)
		}
	}
	for _, s := range []string{"", "/", "///"} {
		if got := CanonicalPath(s); got != "/" {
			t.Errorf("CanonicalPath(%q) = %q, want /", s, got)
		}
	}

	// Two spellings of the same path must contend on the same lock.
	locks := New()
	release := locks.Lock(LevelFile, "a/b.txt")
	done := make(chan struct{})
	go func() {
		other := locks.Lock(LevelFile, "/a/b.txt")
		other()
		close(done)
	}()
	select {
	case <-done:
		t.Error("differently spelled paths did not share a lock")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	<-done
}

func TestReclaimDropsOnlyIdleEntries(t *testing.T) {
	locks := New()
	releaseHeld := locks.Lock(LevelFile, "/held.txt")

	// An entry with a waiter must survive the sweep too.
	waiterStarted := make(chan struct{})
	waiterDone := make(chan struct{})
	go func() {
		close(waiterStarted)
		release := locks.Lock(LevelFile, "/held.txt")
		release()
		close(waiterDone)
	}()
	<-waiterStarted
	time.Sleep(50 * time.Millisecond) // let the waiter register on the entry

	releaseIdle := locks.Lock(LevelFile, "/idle.txt")
	releaseIdle()

	if dropped := locks.Reclaim(); dropped != 1 {
		t.Errorf("Reclaim dropped %d entries, want 1 (only the idle one)", dropped)
	}
	if locks.Len() != 1 {
		t.Errorf("table holds %d entries, want 1 (the contended one)", locks.Len())
	}

	releaseHeld()
	<-waiterDone
	if locks.Len() != 1 {
		t.Errorf("table holds %d entries after release, want 1 (sweep has not run yet)", locks.Len())
	}
	if dropped := locks.Reclaim(); dropped != 1 {
		t.Errorf("second Reclaim dropped %d entries, want 1", dropped)
	}
	if locks.Len() != 0 {
		t.Errorf("table holds %d entries after sweep, want 0", locks.Len())
	}
}

// TestReclaimCannotCauseLockEscape reproduces the hazard the refcount exists
// for: while a goroutine waits on a key, the janitor sweeps the table. The
// waiter must still be mutually excluded from a later acquirer.
func TestReclaimCannotCauseLockEscape(t *testing.T) {
	locks := New()

	release := locks.Lock(LevelObject, "/contended.txt")

	var concurrent, maxConcurrent int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release := locks.Lock(LevelObject, "/contended.txt")
			defer release()
			now := atomic.AddInt32(&concurrent, 1)
			for {
				prev := atomic.LoadInt32(&maxConcurrent)
				if now <= prev || atomic.CompareAndSwapInt32(&maxConcurrent, prev, now) {
					break
				}
			}
			time.Sleep(200 * time.Microsecond)
			atomic.AddInt32(&concurrent, -1)
		}()
	}

	// Sweep repeatedly while the waiters queue up.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				locks.Reclaim()
				time.Sleep(20 * time.Microsecond)
			}
		}
	}()

	time.Sleep(2 * time.Millisecond)
	release()
	wg.Wait()
	close(stop)

	if maxConcurrent != 1 {
		t.Errorf("max concurrent holders = %d, want 1 (reclaim must not let a lock escape)", maxConcurrent)
	}
}
