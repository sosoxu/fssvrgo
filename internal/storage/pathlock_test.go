package storage

import (
	"testing"

	"github.com/sosoxu/fssvrgo/internal/pathlock"
)

// TestProcessLockIsReclaimable pins the part of R12 that was previously broken:
// the storage-level lock table had no reclamation path in production, so an
// entry was added for every path ever touched and never dropped.
func TestProcessLockIsReclaimable(t *testing.T) {
	ls := NewLocalStorage(t.TempDir())
	locks := ls.ProcessLock()
	if locks == nil {
		t.Fatal("LocalStorage must expose a lock table")
	}

	paths := []string{"a.txt", "dir/b.txt", "dir/c.txt"}
	for _, p := range paths {
		if err := ls.Write(t.Context(), p, []byte("x")); err != nil {
			t.Fatalf("Write(%s): %v", p, err)
		}
	}

	if got := locks.Len(); got != len(paths) {
		t.Errorf("lock table holds %d entries after %d writes, want %d", got, len(paths), len(paths))
	}
	if dropped := locks.Reclaim(); dropped != len(paths) {
		t.Errorf("Reclaim dropped %d entries, want %d", dropped, len(paths))
	}
	if got := locks.Len(); got != 0 {
		t.Errorf("lock table holds %d entries after Reclaim, want 0", got)
	}
}

// TestProcessLockSharedBetweenCallsAndReclamation checks that the object-level
// lock is still held for the duration of an operation while the table remains
// the single reclamation point: a concurrent writer on the same path must wait,
// and Reclaim must not disturb it.
func TestProcessLockSharedBetweenCallsAndReclamation(t *testing.T) {
	ls := NewLocalStorage(t.TempDir())
	locks := ls.ProcessLock()

	release := locks.Lock(pathlock.LevelObject, "/contended.txt")
	if dropped := locks.Reclaim(); dropped != 0 {
		t.Errorf("Reclaim dropped %d entries while one was held, want 0", dropped)
	}

	done := make(chan struct{})
	go func() {
		if err := ls.Write(t.Context(), "contended.txt", []byte("payload")); err != nil {
			t.Errorf("concurrent Write: %v", err)
		}
		close(done)
	}()

	release()
	<-done

	data, err := ls.Read(t.Context(), "contended.txt")
	if err != nil {
		t.Fatalf("Read back: %v", err)
	}
	if string(data) != "payload" {
		t.Errorf("stored payload = %q, want payload", data)
	}
}

// TestStructLiteralBackendHasUsableLocks covers the adapters built with a
// struct literal (several backend tests do this): the zero-value table must
// work rather than panicking on a nil pointer.
func TestStructLiteralBackendHasUsableLocks(t *testing.T) {
	ms := &MinIOStorage{}
	release := ms.ProcessLock().Lock(pathlock.LevelObject, "/key")
	release()
	if dropped := ms.ProcessLock().Reclaim(); dropped != 1 {
		t.Errorf("Reclaim dropped %d entries, want 1", dropped)
	}
}
