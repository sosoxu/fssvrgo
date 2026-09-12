package directory

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
)

// fakeDirectoryStore is one in-memory store that satisfies both metadataStore
// and treeStore over the same two "tables". In PostgreSQL the single-row
// metadata service and the cascade store read the same rows, so mirroring that
// with a single fake keeps the two views consistent. These tests exercise
// DirectoryManager without a PostgreSQL instance — the point of the narrow
// interfaces introduced for issue #114.
type fakeDirectoryStore struct {
	dirs  map[string]*database.DirectoryMetadata
	files map[string]database.PathEntry

	createCalls         int
	removeCalls         int
	softDeleteFileCalls []int
	softDeleteDirCalls  []int
	renames             []fakeRename
}

type fakeRename struct {
	files  []database.PathUpdate
	dirs   []database.PathUpdate
	target database.PathUpdate
}

func newFakeDirectoryStore() *fakeDirectoryStore {
	return &fakeDirectoryStore{
		dirs:  map[string]*database.DirectoryMetadata{},
		files: map[string]database.PathEntry{},
	}
}

func (f *fakeDirectoryStore) putDir(path string) *database.DirectoryMetadata {
	meta := &database.DirectoryMetadata{ID: "dir-" + path, Path: path, Name: path}
	f.dirs[path] = meta
	return meta
}

func (f *fakeDirectoryStore) putFile(path string) {
	f.files[path] = database.PathEntry{ID: "file-" + path, Path: path}
}

// --- metadataStore ---

func (f *fakeDirectoryStore) Create(meta *database.DirectoryMetadata) error {
	f.createCalls++
	clone := *meta
	f.dirs[meta.Path] = &clone
	return nil
}

func (f *fakeDirectoryStore) GetByPath(path string) (*database.DirectoryMetadata, error) {
	if meta, ok := f.dirs[path]; ok {
		return meta, nil
	}
	return nil, nil
}

func (f *fakeDirectoryStore) Remove(id string) error {
	for path, meta := range f.dirs {
		if meta.ID == id {
			delete(f.dirs, path)
			f.removeCalls++
			return nil
		}
	}
	return sql.ErrNoRows
}

func (f *fakeDirectoryStore) Exists(path string) (bool, error) {
	_, ok := f.dirs[path]
	return ok, nil
}

// --- treeStore ---

func (f *fakeDirectoryStore) CountChildren(path string) (int, int, error) {
	prefix := path + "/"
	files, dirs := 0, 0
	for p := range f.files {
		if strings.HasPrefix(p, prefix) {
			files++
		}
	}
	for p := range f.dirs {
		if strings.HasPrefix(p, prefix) {
			dirs++
		}
	}
	return files, dirs, nil
}

func (f *fakeDirectoryStore) ListChildFiles(path string, limit int) ([]database.PathEntry, error) {
	entries := make([]database.PathEntry, 0, len(f.files))
	for _, e := range f.files {
		entries = append(entries, e)
	}
	return listFakeChildren(entries, path, limit), nil
}

func (f *fakeDirectoryStore) ListChildDirectories(path string, limit int) ([]database.PathEntry, error) {
	entries := make([]database.PathEntry, 0, len(f.dirs))
	for p, meta := range f.dirs {
		entries = append(entries, database.PathEntry{ID: meta.ID, Path: p})
	}
	return listFakeChildren(entries, path, limit), nil
}

func (f *fakeDirectoryStore) SoftDeleteFiles(_ string, entries []database.PathEntry) error {
	f.softDeleteFileCalls = append(f.softDeleteFileCalls, len(entries))
	for _, e := range entries {
		delete(f.files, e.Path)
	}
	return nil
}

func (f *fakeDirectoryStore) SoftDeleteDirectories(_ string, entries []database.PathEntry) error {
	f.softDeleteDirCalls = append(f.softDeleteDirCalls, len(entries))
	for _, e := range entries {
		delete(f.dirs, e.Path)
	}
	return nil
}

func (f *fakeDirectoryStore) RenameTree(_ string, files, dirs []database.PathUpdate, target database.PathUpdate) error {
	f.renames = append(f.renames, fakeRename{files: files, dirs: dirs, target: target})
	f.moveFileUpdates(files)
	f.moveDirUpdates(append(append([]database.PathUpdate{}, dirs...), target))
	return nil
}

func (f *fakeDirectoryStore) moveFileUpdates(updates []database.PathUpdate) {
	for _, u := range updates {
		for path, e := range f.files {
			if e.ID != u.ID {
				continue
			}
			delete(f.files, path)
			e.Path = u.Path
			f.files[u.Path] = e
		}
	}
}

func (f *fakeDirectoryStore) moveDirUpdates(updates []database.PathUpdate) {
	for _, u := range updates {
		for path, meta := range f.dirs {
			if meta.ID != u.ID {
				continue
			}
			delete(f.dirs, path)
			meta.Path = u.Path
			meta.Name = u.Name
			f.dirs[u.Path] = meta
		}
	}
}

func listFakeChildren(entries []database.PathEntry, path string, limit int) []database.PathEntry {
	prefix := path + "/"
	matched := make([]database.PathEntry, 0, len(entries))
	for _, e := range entries {
		if strings.HasPrefix(e.Path, prefix) {
			matched = append(matched, e)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Path < matched[j].Path })
	if limit > 0 && len(matched) > limit {
		matched = matched[:limit]
	}
	return matched
}

func TestDeleteDirectoryNonRecursiveRefusesNonEmptyWithoutDatabase(t *testing.T) {
	store := newFakeDirectoryStore()
	store.putDir("foo")
	store.putFile("foo/a.txt")
	dm := NewDirectoryManagerWithStores(store, store, nil, nil)

	err := dm.DeleteDirectory("foo", false)
	if err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("DeleteDirectory error = %v, want non-empty rejection", err)
	}
	if store.removeCalls != 0 {
		t.Errorf("Remove called %d times, want 0 — a rejected delete must not touch metadata", store.removeCalls)
	}
	if _, ok := store.files["foo/a.txt"]; !ok {
		t.Error("child file row should survive a rejected non-recursive delete")
	}
}

func TestDeleteDirectoryNonRecursiveRemovesEmptyDirectoryWithoutDatabase(t *testing.T) {
	store := newFakeDirectoryStore()
	store.putDir("foo")
	dm := NewDirectoryManagerWithStores(store, store, nil, nil)

	if err := dm.DeleteDirectory("foo", false); err != nil {
		t.Fatalf("DeleteDirectory: %v", err)
	}
	if store.removeCalls != 1 {
		t.Errorf("Remove called %d times, want 1", store.removeCalls)
	}
	if exists, _ := store.Exists("foo"); exists {
		t.Error("directory row should be gone after delete")
	}
}

func TestDeleteDirectoryRecursiveBatchesAndCleansStorageWithoutDatabase(t *testing.T) {
	const fileCount = 1200

	store := newFakeDirectoryStore()
	store.putDir("foo")
	for i := 0; i < fileCount; i++ {
		store.putFile(fmt.Sprintf("foo/f%04d.txt", i))
	}
	store.putDir("foo/sub")
	store.putDir("foo/sub2")

	storageMock := newMockObjectStorage()
	storageMock.storageType = "local"
	dm := NewDirectoryManagerWithStores(store, store, storageMock, nil)

	if err := dm.DeleteDirectory("foo", true); err != nil {
		t.Fatalf("DeleteDirectory recursive: %v", err)
	}

	// 1200 files must go out in batches of at most 500, each batch in its own
	// store round-trip (the service must not load the whole subtree).
	wantBatches := "[500 500 200]"
	if got := fmt.Sprint(store.softDeleteFileCalls); got != wantBatches {
		t.Errorf("file batches = %s, want %s", got, wantBatches)
	}
	if len(store.files) != 0 {
		t.Errorf("%d file rows survived recursive delete", len(store.files))
	}
	if got := len(storageMock.removeCalls); got != fileCount {
		t.Errorf("storage Remove calls = %d, want %d", got, fileCount)
	}

	if got, want := fmt.Sprint(store.softDeleteDirCalls), "[2]"; got != want {
		t.Errorf("directory batches = %s, want %s", got, want)
	}
	if exists, _ := store.Exists("foo"); exists {
		t.Error("directory row should be gone after recursive delete")
	}
	if store.removeCalls != 1 {
		t.Errorf("Remove called %d times, want 1 for the directory row itself", store.removeCalls)
	}
}

func TestRenameDirectoryRewritesSubtreeWithoutDatabase(t *testing.T) {
	store := newFakeDirectoryStore()
	store.putDir("foo")
	store.putDir("foo/sub")
	store.putFile("foo/a.txt")
	store.putFile("foo/sub/b.txt")

	storageMock := newMockObjectStorage()
	storageMock.storageType = "local"
	dm := NewDirectoryManagerWithStores(store, store, storageMock, nil)

	if err := dm.RenameDirectory("foo", "bar"); err != nil {
		t.Fatalf("RenameDirectory: %v", err)
	}

	if len(store.renames) != 1 {
		t.Fatalf("RenameTree called %d times, want 1 (the whole subtree must move at once)", len(store.renames))
	}
	rename := store.renames[0]
	if rename.target.Path != "bar" || rename.target.Name != "bar" {
		t.Errorf("target update = %+v, want path/name bar", rename.target)
	}

	wantFileUpdates := map[string]string{
		"file-foo/a.txt":     "bar/a.txt",
		"file-foo/sub/b.txt": "bar/sub/b.txt",
	}
	if len(rename.files) != len(wantFileUpdates) {
		t.Fatalf("file updates = %+v, want %d entries", rename.files, len(wantFileUpdates))
	}
	for _, u := range rename.files {
		if want, ok := wantFileUpdates[u.ID]; !ok || want != u.Path {
			t.Errorf("file update = %+v, want %v", u, wantFileUpdates)
		}
	}
	if len(rename.dirs) != 1 || rename.dirs[0].ID != "dir-foo/sub" || rename.dirs[0].Path != "bar/sub" {
		t.Errorf("directory updates = %+v, want foo/sub -> bar/sub", rename.dirs)
	}

	// The in-memory rows must now live at the new paths.
	for _, p := range []string{"bar/a.txt", "bar/sub/b.txt"} {
		if _, ok := store.files[p]; !ok {
			t.Errorf("expected file row at %s after rename", p)
		}
	}
	if _, ok := store.dirs["bar/sub"]; !ok {
		t.Error("expected child directory at bar/sub after rename")
	}
	if exists, _ := store.Exists("foo"); exists {
		t.Error("old directory row should be gone after rename")
	}

	// Storage moves happen for the files plus the renamed directory itself
	// (renaming the local directory carries its children along).
	wantRenames := []renameCall{
		{oldPath: "foo/a.txt", newPath: "bar/a.txt"},
		{oldPath: "foo/sub/b.txt", newPath: "bar/sub/b.txt"},
		{oldPath: "foo", newPath: "bar"},
	}
	if fmt.Sprint(storageMock.renameCalls) != fmt.Sprint(wantRenames) {
		t.Errorf("storage renames = %+v, want %+v", storageMock.renameCalls, wantRenames)
	}
}

func TestRenameDirectoryRejectsObjectStorageWithoutDatabase(t *testing.T) {
	store := newFakeDirectoryStore()
	store.putDir("foo")
	store.putFile("foo/a.txt")

	storageMock := newMockObjectStorage() // StorageType() == "minio"
	dm := NewDirectoryManagerWithStores(store, store, storageMock, nil)

	err := dm.RenameDirectory("foo", "bar")
	if err == nil || !strings.Contains(err.Error(), "not supported for object storage") {
		t.Fatalf("RenameDirectory error = %v, want object-storage rejection", err)
	}
	if len(store.renames) != 0 {
		t.Errorf("RenameTree called %d times, want 0 when the rename is rejected up front", len(store.renames))
	}
	if len(storageMock.renameCalls) != 0 {
		t.Errorf("storage Rename called %d times, want 0", len(storageMock.renameCalls))
	}
}

func TestRenameDirectoryRefusesExistingTargetWithoutDatabase(t *testing.T) {
	store := newFakeDirectoryStore()
	store.putDir("foo")
	store.putDir("bar")
	dm := NewDirectoryManagerWithStores(store, store, nil, nil)

	err := dm.RenameDirectory("foo", "bar")
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("RenameDirectory error = %v, want target-exists rejection", err)
	}
	if len(store.renames) != 0 {
		t.Errorf("RenameTree called %d times, want 0", len(store.renames))
	}
}
