package directory

import (
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/pathlock"
)

// TestDirectoryManagerSharesStorageLockTable pins R12's structural fix for the
// directory layer: the manager and its storage backend use one process-local
// table, so the process has a single place to reclaim idle lock entries.
func TestDirectoryManagerSharesStorageLockTable(t *testing.T) {
	store := newMockObjectStorage()
	db := newFakeDirectoryStore()
	dm := NewDirectoryManagerWithStores(db, db, store, nil)

	if dm.locks != store.ProcessLock() {
		t.Fatal("DirectoryManager must use the storage backend's lock table, not its own")
	}

	release := dm.locks.Lock(pathlock.LevelDirectory, "/ops")
	release()

	if dropped := store.ProcessLock().Reclaim(); dropped != 1 {
		t.Errorf("Reclaim dropped %d entries, want 1", dropped)
	}
}

// TestDirectoryOperationNestsInsideObjectLevel is the deadlock probe for the
// level hierarchy: a directory operation holds a directory-level lock and then
// calls into storage, which takes the object level for the same path. With two
// separate tables, or with one table and one level, the same goroutine would
// block on itself.
func TestDirectoryOperationNestsInsideObjectLevel(t *testing.T) {
	store := newMockObjectStorage()
	db := newFakeDirectoryStore()
	dm := NewDirectoryManagerWithStores(db, db, store, nil)

	done := make(chan error, 1)
	go func() {
		done <- dm.CreateDirectory(t.Context(), "/deep/nested/dir")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CreateDirectory: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CreateDirectory deadlocked: directory and object levels must be distinct keys")
	}
}

// TestConcurrentDirectoryOperationsSerialize verifies that same-path directory
// operations are mutually exclusive within one process even without a
// distributed lock configured (nil distLock is the single-process case).
func TestConcurrentDirectoryOperationsSerialize(t *testing.T) {
	store := newFakeDirectoryStore()
	dm := NewDirectoryManagerWithStores(store, store, nil, nil)

	const workers = 16
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() { errs <- dm.CreateDirectory(t.Context(), "same") }()
	}

	created, rejected := 0, 0
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			rejected++
			continue
		}
		created++
	}

	if created != 1 {
		t.Errorf("successful creates = %d, want exactly 1 (the check-then-act must be serialized)", created)
	}
	if rejected != workers-1 {
		t.Errorf("already-exists rejections = %d, want %d", rejected, workers-1)
	}
	if store.createCalls != 1 {
		t.Errorf("store.Create called %d times, want 1", store.createCalls)
	}
}
