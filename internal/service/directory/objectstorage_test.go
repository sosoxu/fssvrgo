package directory

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// mockObjectStorage is a minimal StorageAdapter that reports itself as an
// object-storage backend (StorageType == "minio") and records every call to
// the directory-level and per-file storage operations so tests can assert
// which operations were/ were not invoked.
//
// It deliberately does NOT touch a real MinIO server — the only behaviors
// exercised by the directory manager under test are CreateDirectory /
// RemoveDirectory / Rename / Remove / StorageType, so the read/write methods
// are stubbed out.
type mockObjectStorage struct {
	mu sync.Mutex

	createDirCalls    []string
	removeDirCalls    []string
	renameCalls       []renameCall
	removeCalls       []string
	storageType       string
	objects           map[string][]byte // in-memory object store for per-file Remove verification
}

type renameCall struct {
	oldPath string
	newPath string
}

func newMockObjectStorage() *mockObjectStorage {
	return &mockObjectStorage{
		storageType: "minio",
		objects:     make(map[string][]byte),
	}
}

func (m *mockObjectStorage) StorageType() string { return m.storageType }

func (m *mockObjectStorage) CreateDirectory(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createDirCalls = append(m.createDirCalls, path)
	return nil
}

func (m *mockObjectStorage) RemoveDirectory(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeDirCalls = append(m.removeDirCalls, path)
	return nil
}

func (m *mockObjectStorage) Rename(oldPath, newPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renameCalls = append(m.renameCalls, renameCall{oldPath, newPath})
	return nil
}

func (m *mockObjectStorage) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCalls = append(m.removeCalls, path)
	delete(m.objects, path)
	return nil
}

// Write stores an object in-memory so that per-file Remove can be observed.
func (m *mockObjectStorage) Write(path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[path] = append([]byte(nil), data...)
	return nil
}

// --- stubs for the rest of the StorageAdapter interface ---

func (m *mockObjectStorage) WriteAt(path string, data []byte, offset int64) error {
	return fmt.Errorf("not supported")
}
func (m *mockObjectStorage) WriteFromTempFile(path string, tempFilePath string) error {
	return fmt.Errorf("not supported")
}
func (m *mockObjectStorage) WriteFromReader(path string, reader io.Reader) error {
	return fmt.Errorf("not supported")
}
func (m *mockObjectStorage) Read(path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.objects[path]; ok {
		return append([]byte(nil), data...), nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *mockObjectStorage) ReadAt(path string, size int, offset int64) ([]byte, error) {
	return nil, fmt.Errorf("not supported")
}
func (m *mockObjectStorage) OpenReader(path string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.objects[path]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *mockObjectStorage) Exists(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[path]
	return ok
}
func (m *mockObjectStorage) List(directory string) ([]string, error) {
	return nil, nil
}
func (m *mockObjectStorage) GetSize(path string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.objects[path]; ok {
		return int64(len(data)), nil
	}
	return 0, fmt.Errorf("not found")
}
func (m *mockObjectStorage) ValidatePath(path string) error { return nil }

// Compile-time check that mockObjectStorage satisfies StorageAdapter.
var _ storage.StorageAdapter = (*mockObjectStorage)(nil)

// TestDirectoryManager_ObjectStorage_SkipsCreateDirectoryMarker verifies that
// on an object-storage backend, CreateDirectory only writes the DB record and
// does NOT call store.CreateDirectory (a 0-byte marker would be meaningless).
func TestDirectoryManager_ObjectStorage_SkipsCreateDirectoryMarker(t *testing.T) {
	db := setupTestDB(t)
	store := newMockObjectStorage()
	dm := NewDirectoryManagerWithDistLock(db, store, distributed.NewLocalDistributedLock())

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	if got := len(store.createDirCalls); got != 0 {
		t.Errorf("expected 0 CreateDirectory storage calls on object storage, got %d (%v)", got, store.createDirCalls)
	}
	if !dm.Exists("foo") {
		t.Errorf("expected DB record for foo to exist")
	}
}

// TestDirectoryManager_ObjectStorage_RenameRejected verifies that renaming a
// directory on object storage returns an error, because the file DB Path is
// the storage key and a partial/non-atomic N-object copy+delete would break
// downloads.
func TestDirectoryManager_ObjectStorage_RenameRejected(t *testing.T) {
	db := setupTestDB(t)
	store := newMockObjectStorage()
	dm := NewDirectoryManagerWithDistLock(db, store, distributed.NewLocalDistributedLock())

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	err := dm.RenameDirectory("foo", "bar")
	if err == nil {
		t.Fatalf("expected RenameDirectory to fail on object storage, got nil")
	}
	if !strings.Contains(err.Error(), "not supported for object storage") {
		t.Errorf("expected object-storage rejection error, got: %v", err)
	}
	if got := len(store.renameCalls); got != 0 {
		t.Errorf("expected 0 Rename storage calls on object storage, got %d", got)
	}
}

// TestDirectoryManager_ObjectStorage_RecursiveDeleteSkipsRemoveDirectory
// verifies that recursive delete on object storage still removes every child
// file object (per-file Remove) but does NOT call RemoveDirectory (which would
// just re-list an already-empty prefix).
func TestDirectoryManager_ObjectStorage_RecursiveDeleteSkipsRemoveDirectory(t *testing.T) {
	db := setupTestDB(t)
	store := newMockObjectStorage()
	dm := NewDirectoryManagerWithDistLock(db, store, distributed.NewLocalDistributedLock())

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory foo: %v", err)
	}
	// Seed two child files in the mock object store + DB.
	for _, p := range []string{"foo/a.txt", "foo/b.txt"} {
		if err := store.Write(p, []byte("x")); err != nil {
			t.Fatalf("store.Write %s: %v", p, err)
		}
		createFileRecord(t, db, p, 1)
	}

	if err := dm.DeleteDirectory("foo", true); err != nil {
		t.Fatalf("DeleteDirectory recursive failed: %v", err)
	}

	// Every child file object must have been removed individually.
	if got := len(store.removeCalls); got != 2 {
		t.Errorf("expected 2 per-file Remove calls, got %d (%v)", got, store.removeCalls)
	}
	// RemoveDirectory must NOT have been called — it would just re-list the
	// now-empty prefix on object storage.
	if got := len(store.removeDirCalls); got != 0 {
		t.Errorf("expected 0 RemoveDirectory calls on object storage, got %d (%v)", got, store.removeDirCalls)
	}
	// CreateDirectory marker must also not have been created.
	if got := len(store.createDirCalls); got != 0 {
		t.Errorf("expected 0 CreateDirectory calls on object storage, got %d (%v)", got, store.createDirCalls)
	}
}

// TestDirectoryManager_LocalStorage_StillCallsDirectoryStorageOps is the
// counterpart for the local backend: CreateDirectory/Rename/Delete must still
// touch the storage layer. This guards against regressions where the backend
// gating accidentally disables local-storage directory operations too.
func TestDirectoryManager_LocalStorage_StillCallsDirectoryStorageOps(t *testing.T) {
	db := setupTestDB(t)
	store := setupTestStore(t)
	dm := NewDirectoryManagerWithDistLock(db, store, distributed.NewLocalDistributedLock())

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if !store.Exists("foo") {
		// LocalStorage.CreateDirectory makes the physical directory; a marker
		// file or the directory itself should be observable.
		t.Errorf("expected local backend to have created the directory marker")
	}

	if err := dm.RenameDirectory("foo", "bar"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}

	// Recursive delete should remove the directory on the local backend.
	if err := dm.DeleteDirectory("bar", true); err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
}

// TestDirectoryManager_ObjectStorage_SupportsDirOpsHelper directly asserts the
// backend gating helper, which is the linchpin of the object-storage behavior.
func TestDirectoryManager_ObjectStorage_SupportsDirOpsHelper(t *testing.T) {
	db := setupTestDB(t)

	// Object storage -> false
	objStore := newMockObjectStorage()
	objDM := NewDirectoryManagerWithDistLock(db, objStore, nil)
	if objDM.supportsDirectoryStorageOps() {
		t.Errorf("expected object storage to NOT support directory storage ops")
	}

	// Local storage -> true
	localStore := setupTestStore(t)
	localDM := NewDirectoryManagerWithDistLock(db, localStore, nil)
	if !localDM.supportsDirectoryStorageOps() {
		t.Errorf("expected local storage to support directory storage ops")
	}

	// nil store -> false (no store to operate on; but rename is still allowed
	// because there is no storage layer to keep in sync — see RenameDirectory).
	nilDM := NewDirectoryManagerWithDistLock(db, nil, nil)
	if nilDM.supportsDirectoryStorageOps() {
		t.Errorf("expected nil store to NOT support directory storage ops")
	}
}

// TestDirectoryManager_ObjectStorage_NonRecursiveDeleteOnlySoftDeletesDB
// verifies that non-recursive delete on an empty object-storage directory only
// removes the DB record and does not call RemoveDirectory.
func TestDirectoryManager_ObjectStorage_NonRecursiveDeleteOnlySoftDeletesDB(t *testing.T) {
	db := setupTestDB(t)
	store := newMockObjectStorage()
	dm := NewDirectoryManagerWithDistLock(db, store, distributed.NewLocalDistributedLock())

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if err := dm.DeleteDirectory("foo", false); err != nil {
		t.Fatalf("DeleteDirectory non-recursive failed: %v", err)
	}
	if got := len(store.removeDirCalls); got != 0 {
		t.Errorf("expected 0 RemoveDirectory calls on object storage, got %d", got)
	}
	if dm.Exists("foo") {
		t.Errorf("expected foo to be soft-deleted in DB")
	}
}

// keep these imports referenced even when some stubs are trimmed
var _ = database.NewFileMetadataService
