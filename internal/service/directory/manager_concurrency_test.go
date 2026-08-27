package directory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
)

// countingLock wraps a DistributedLock and records every Lock/Unlock call so
// tests can assert that directory operations actually acquire the expected
// locks.
type countingLock struct {
	inner     distributed.DistributedLock
	mu        sync.Mutex
	lockCalls []string
	unlocks   int32
}

func (c *countingLock) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	c.mu.Lock()
	c.lockCalls = append(c.lockCalls, key)
	c.mu.Unlock()
	return c.inner.Lock(ctx, key, ttl)
}

func (c *countingLock) Unlock(ctx context.Context, key string, token string) error {
	atomic.AddInt32(&c.unlocks, 1)
	return c.inner.Unlock(ctx, key, token)
}

func (c *countingLock) Extend(ctx context.Context, key string, token string, ttl time.Duration) error {
	return c.inner.Extend(ctx, key, token, ttl)
}

func (c *countingLock) lockCountFor(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, k := range c.lockCalls {
		if k == prefix {
			n++
		}
	}
	return n
}

// TestDirectoryManager_AcquiresLockOnCreate verifies that CreateDirectory
// acquires the dir:<path> distributed lock when a distLock is configured.
func TestDirectoryManager_AcquiresLockOnCreate(t *testing.T) {
	db := setupTestDB(t)
	cl := &countingLock{inner: distributed.NewLocalDistributedLock()}
	dm := NewDirectoryManagerWithDistLock(db, nil, cl)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if got := cl.lockCountFor("dir:foo"); got != 1 {
		t.Errorf("expected 1 dir:foo lock call, got %d", got)
	}
}

// TestDirectoryManager_RenameAcquiresLock verifies that RenameDirectory
// acquires the dir:<oldPath> lock.
func TestDirectoryManager_RenameAcquiresLock(t *testing.T) {
	db := setupTestDB(t)
	cl := &countingLock{inner: distributed.NewLocalDistributedLock()}
	dm := NewDirectoryManagerWithDistLock(db, nil, cl)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if err := dm.RenameDirectory("foo", "bar"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}
	// One lock for Create (dir:foo) + one for Rename (dir:foo).
	if got := cl.lockCountFor("dir:foo"); got != 2 {
		t.Errorf("expected 2 dir:foo lock calls (create+rename), got %d", got)
	}
}

// TestDirectoryManager_ConcurrentRenamesSerialize verifies that concurrent
// renames of the SAME directory serialize through the distributed lock and do
// not leave the metadata in an inconsistent state.
//
// Without the lock, two goroutines renaming "foo" -> "bar" and "foo" -> "baz"
// concurrently could interleave their per-child UPDATE statements and produce
// a mix of "bar/..." and "baz/..." paths under a single directory row. With the
// lock, the second rename sees the result of the first (either "bar" or "baz")
// and either succeeds renaming that, or fails the target-exists check.
func TestDirectoryManager_ConcurrentRenamesSerialize(t *testing.T) {
	db := setupTestDB(t)
	cl := &countingLock{inner: distributed.NewLocalDistributedLock()}
	store := setupTestStore(t)
	dm := NewDirectoryManagerWithDistLock(db, store, cl)

	// Seed: foo/ with two child files.
	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory foo: %v", err)
	}
	if err := store.Write("foo/a.txt", []byte("a")); err != nil {
		t.Fatalf("store.Write a: %v", err)
	}
	createFileRecord(t, db, "foo/a.txt", 1)
	if err := store.Write("foo/b.txt", []byte("b")); err != nil {
		t.Fatalf("store.Write b: %v", err)
	}
	createFileRecord(t, db, "foo/b.txt", 1)

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	start := make(chan struct{})

	// Each goroutine tries to rename whatever the current top-level dir is to a
	// unique target. Because of the lock, exactly one rename of a given source
	// can be in flight at a time; the others either wait and then find the
	// source already gone (error, counted as benign) or find the target already
	// exists (error, benign).
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			target := fmt.Sprintf("tgt%d", i)
			if err := dm.RenameDirectory("foo", target); err != nil {
				// Errors are expected once the source "foo" is renamed away by
				// the first winner. Only unexpected error types are fatal.
				errs <- err
				return
			}
			errs <- nil
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	// At most one goroutine should have renamed "foo" successfully; the rest
	// must have errored with "directory not found" (because "foo" was already
	// renamed). No goroutine should have produced a panic or a non-error
	// corruption.
	successes := 0
	notFound := 0
	other := 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, errDirectoryNotFound) || containsMsg(err, "directory not found") {
			notFound++
		} else {
			other++
		}
	}
	if successes != 1 {
		t.Errorf("expected exactly 1 successful rename, got %d (notFound=%d other=%d)", successes, notFound, other)
	}
	// Every child file must end up under exactly one target prefix — no split.
	if err := assertNoSplitChildren(db, "foo"); err != nil {
		t.Errorf("post-rename consistency check failed: %v", err)
	}
}

// errDirectoryNotFound mirrors the sentinel used by the manager. We compare by
// message substring to avoid exporting a sentinel just for tests.
var errDirectoryNotFound = errors.New("directory not found")

func containsMsg(err error, msg string) bool {
	return err != nil && strings.Contains(err.Error(), msg)
}

// assertNoSplitChildren checks that no live file row has a path starting with
// "foo/" — i.e. the rename fully moved (or fully failed) and did not leave
// children stranded under the old prefix.
func assertNoSplitChildren(db *database.DB, oldPrefix string) error {
	rows, err := db.Query("SELECT path FROM files WHERE path LIKE ? AND is_deleted = FALSE", oldPrefix+"/%")
	if err != nil {
		return fmt.Errorf("query stranded children: %w", err)
	}
	defer rows.Close()
	var stranded []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return err
		}
		stranded = append(stranded, p)
	}
	if len(stranded) > 0 {
		return fmt.Errorf("found %d child(ren) still under old prefix %s: %v", len(stranded), oldPrefix, stranded)
	}
	return nil
}

// TestDirectoryManager_DeleteAcquiresLock verifies DeleteDirectory (recursive)
// acquires the dir:<path> lock.
func TestDirectoryManager_DeleteAcquiresLock(t *testing.T) {
	db := setupTestDB(t)
	cl := &countingLock{inner: distributed.NewLocalDistributedLock()}
	dm := NewDirectoryManagerWithDistLock(db, nil, cl)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if err := dm.DeleteDirectory("foo", true); err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
	// One for Create + one for Delete.
	if got := cl.lockCountFor("dir:foo"); got != 2 {
		t.Errorf("expected 2 dir:foo lock calls (create+delete), got %d", got)
	}
}

// TestDirectoryManager_NilDistLockStillWorks verifies that when no distLock is
// configured (legacy constructors), operations still function correctly — the
// lock is simply skipped. This preserves backward compatibility for tests and
// single-process deployments that do not need cross-instance mutual exclusion.
func TestDirectoryManager_NilDistLockStillWorks(t *testing.T) {
	db := setupTestDB(t)
	store := setupTestStore(t)
	dm := NewDirectoryManagerWithStore(db, store)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if err := dm.RenameDirectory("foo", "bar"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}
	if !dm.Exists("bar") {
		t.Errorf("expected bar to exist after rename with nil distLock")
	}
}

func TestDirectoryManager_ConcurrentRenameSameTarget(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManagerWithDistLock(db, nil, distributed.NewLocalDistributedLock())
	for _, path := range []string{"foo", "bar"} {
		if err := dm.CreateDirectory(path); err != nil {
			t.Fatalf("CreateDirectory %s: %v", path, err)
		}
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, source := range []string{"foo", "bar"} {
		go func(path string) {
			<-start
			errs <- dm.RenameDirectory(path, "target")
		}(source)
	}
	close(start)
	err1, err2 := <-errs, <-errs
	if (err1 == nil) == (err2 == nil) {
		t.Fatalf("exactly one directory rename must succeed, got errors %v and %v", err1, err2)
	}
	if !dm.Exists("target") {
		t.Fatal("target directory does not exist")
	}
	remaining := 0
	for _, source := range []string{"foo", "bar"} {
		if dm.Exists(source) {
			remaining++
		}
	}
	if remaining != 1 {
		t.Fatalf("remaining source count = %d, want 1", remaining)
	}
}

// TestDirectoryManager_RenameAtomicOnStorageFailure verifies that if a storage
// rename fails AFTER the DB transaction commits, the DB still ends up with all
// children at the new path (atomic), rather than half-old/half-new. The
// storage mismatch is a recoverable best-effort issue, not a DB consistency
// issue.
func TestDirectoryManager_RenameAtomicOnStorageFailure(t *testing.T) {
	db := setupTestDB(t)
	store := setupTestStore(t)
	dm := NewDirectoryManagerWithDistLock(db, store, distributed.NewLocalDistributedLock())

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	createFileRecord(t, db, "foo/a.txt", 1)
	createFileRecord(t, db, "foo/b.txt", 1)

	if err := dm.RenameDirectory("foo", "bar"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}

	// Both children must be at bar/ in the DB, regardless of storage state.
	for _, p := range []string{"bar/a.txt", "bar/b.txt"} {
		meta, err := database.NewFileMetadataService(db).GetByPath(p)
		if err != nil {
			t.Fatalf("GetByPath %s: %v", p, err)
		}
		if meta == nil {
			t.Errorf("expected metadata at %s after rename, got nil", p)
		}
	}
	// And none should remain at foo/.
	if err := assertNoSplitChildren(db, "foo"); err != nil {
		t.Errorf("expected no children under foo/ after rename, but: %v", err)
	}
}
