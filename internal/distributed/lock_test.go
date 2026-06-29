package distributed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalLock(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	token, err := l.Lock(ctx, "k1", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	if err := l.Unlock(ctx, "k1", token); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestLocalLock_DoubleLock(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	token1, err := l.Lock(ctx, "dup", 10*time.Second)
	if err != nil {
		t.Fatalf("first Lock: %v", err)
	}
	if token1 == "" {
		t.Fatal("expected non-empty token")
	}

	// same key while held -> conflict
	if _, err := l.Lock(ctx, "dup", 10*time.Second); !errors.Is(err, ErrLockConflict) {
		t.Fatalf("expected ErrLockConflict on double lock, got %v", err)
	}

	if err := l.Unlock(ctx, "dup", token1); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// after release, re-acquire should succeed
	token2, err := l.Lock(ctx, "dup", 10*time.Second)
	if err != nil {
		t.Fatalf("re-acquire Lock: %v", err)
	}
	if token2 == "" {
		t.Fatal("expected non-empty token on re-acquire")
	}
	if err := l.Unlock(ctx, "dup", token2); err != nil {
		t.Fatalf("Unlock after re-acquire: %v", err)
	}
}

func TestLocalLock_UnlockWrongToken(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	token, err := l.Lock(ctx, "wt", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// wrong token -> not held by this owner
	if err := l.Unlock(ctx, "wt", "wrong-token"); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld on wrong token unlock, got %v", err)
	}

	// unlock unknown key -> not held
	if err := l.Unlock(ctx, "nope", token); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld on unknown key unlock, got %v", err)
	}

	// lock still held by correct token -> double lock conflicts
	if _, err := l.Lock(ctx, "wt", 10*time.Second); !errors.Is(err, ErrLockConflict) {
		t.Fatalf("expected lock still held after wrong-token unlock, got %v", err)
	}

	if err := l.Unlock(ctx, "wt", token); err != nil {
		t.Fatalf("correct Unlock: %v", err)
	}
}

func TestLocalLock_Extend(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	token, err := l.Lock(ctx, "ext", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// extend with correct token succeeds
	if err := l.Extend(ctx, "ext", token, 30*time.Second); err != nil {
		t.Fatalf("Extend: %v", err)
	}

	// extend with wrong token fails
	if err := l.Extend(ctx, "ext", "wrong", 30*time.Second); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld on wrong token extend, got %v", err)
	}

	// extend unknown key fails
	if err := l.Extend(ctx, "nope", token, 30*time.Second); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld on unknown key extend, got %v", err)
	}

	if err := l.Unlock(ctx, "ext", token); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// extend after unlock fails (lock released)
	if err := l.Extend(ctx, "ext", token, 30*time.Second); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld on extend after unlock, got %v", err)
	}
}

func TestAcquireLock(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	token, err := AcquireLock(ctx, l, "acq", 10*time.Second, 3, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	// lock is now held, a second acquire with no realistic retry window fails
	if _, err := AcquireLock(ctx, l, "acq", 10*time.Second, 2, time.Millisecond); !errors.Is(err, ErrLockConflict) {
		t.Fatalf("expected ErrLockConflict on contended AcquireLock, got %v", err)
	}

	if err := l.Unlock(ctx, "acq", token); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestAcquireLock_WithRetry(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	// hold the lock so AcquireLock must retry
	holder, err := l.Lock(ctx, "retry", 10*time.Second)
	if err != nil {
		t.Fatalf("holder Lock: %v", err)
	}

	type result struct {
		token string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		tok, err := AcquireLock(ctx, l, "retry", 10*time.Second, 20, 5*time.Millisecond)
		done <- result{tok, err}
	}()

	// release shortly so AcquireLock succeeds on a retry attempt
	time.Sleep(20 * time.Millisecond)
	if err := l.Unlock(ctx, "retry", holder); err != nil {
		t.Fatalf("holder Unlock: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("AcquireLock after retry: %v", res.err)
		}
		if res.token == "" {
			t.Fatal("expected non-empty token")
		}
		if err := l.Unlock(ctx, "retry", res.token); err != nil {
			t.Fatalf("final Unlock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireLock did not return in time")
	}
}

func TestLocalLock_Concurrent(t *testing.T) {
	l := NewLocalDistributedLock()
	ctx := context.Background()

	const n = 50
	var (
		wg       sync.WaitGroup
		success  int64
		conflict int64
		winMu    sync.Mutex
		winTok   string
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tok, err := l.Lock(ctx, "conc", 10*time.Second)
			if err == nil {
				atomic.AddInt64(&success, 1)
				winMu.Lock()
				winTok = tok
				winMu.Unlock()
			} else if errors.Is(err, ErrLockConflict) {
				atomic.AddInt64(&conflict, 1)
			}
		}()
	}
	wg.Wait()

	if success != 1 {
		t.Fatalf("expected exactly 1 successful lock, got %d", success)
	}
	if int(success+conflict) != n {
		t.Fatalf("expected %d accounted attempts, got success=%d conflict=%d", n, success, conflict)
	}

	if winTok != "" {
		if err := l.Unlock(ctx, "conc", winTok); err != nil {
			t.Fatalf("cleanup Unlock: %v", err)
		}
	}
}
