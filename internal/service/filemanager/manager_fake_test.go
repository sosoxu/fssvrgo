package filemanager

import (
	"database/sql"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// fakeMetadata is an in-memory metadataStore. These tests exercise FileManager
// without PostgreSQL — the point of the narrow interface introduced for #114.
type fakeMetadata struct {
	byPath  map[string]*database.FileMetadata
	created int
	updated int
	removed int
}

func newFakeMetadata() *fakeMetadata {
	return &fakeMetadata{byPath: map[string]*database.FileMetadata{}}
}

func (f *fakeMetadata) GetByPath(path string) (*database.FileMetadata, error) {
	if meta, ok := f.byPath[path]; ok {
		return meta, nil
	}
	return nil, sql.ErrNoRows
}

func (f *fakeMetadata) Create(meta *database.FileMetadata) error {
	f.created++
	f.byPath[meta.Path] = meta
	return nil
}

func (f *fakeMetadata) Update(meta *database.FileMetadata) error {
	f.updated++
	f.byPath[meta.Path] = meta
	return nil
}

func (f *fakeMetadata) Remove(id string) error {
	f.removed++
	for path, meta := range f.byPath {
		if meta.ID == id {
			delete(f.byPath, path)
		}
	}
	return nil
}

func (f *fakeMetadata) Exists(path string) (bool, error) {
	_, ok := f.byPath[path]
	return ok, nil
}

func newFakeBackedManager(t *testing.T) (*FileManager, *fakeMetadata, storage.StorageAdapter) {
	t.Helper()
	meta := newFakeMetadata()
	store := storage.NewLocalStorage(t.TempDir())
	fm := NewFileManagerWithStore(store, meta, distributed.NewLocalDistributedLock())
	return fm, meta, store
}

func TestUploadFileCreatesMetadataWithoutDatabase(t *testing.T) {
	fm, meta, store := newFakeBackedManager(t)
	payload := []byte("hello fake metadata")

	uploaded, err := fm.UploadFile("/docs/a.txt", payload)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if meta.created != 1 || meta.updated != 0 {
		t.Fatalf("created=%d updated=%d, want 1/0", meta.created, meta.updated)
	}
	if uploaded.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", uploaded.Size, len(payload))
	}
	stored, err := store.Read("/docs/a.txt")
	if err != nil {
		t.Fatalf("Read back: %v", err)
	}
	if string(stored) != string(payload) {
		t.Errorf("stored payload = %q, want %q", stored, payload)
	}
}

func TestUploadFileOverwritesExistingMetadataWithoutDatabase(t *testing.T) {
	fm, meta, _ := newFakeBackedManager(t)

	if _, err := fm.UploadFile("/docs/a.txt", []byte("first")); err != nil {
		t.Fatalf("first upload: %v", err)
	}
	second, err := fm.UploadFile("/docs/a.txt", []byte("second version"))
	if err != nil {
		t.Fatalf("second upload: %v", err)
	}
	if meta.created != 1 || meta.updated != 1 {
		t.Fatalf("created=%d updated=%d, want 1/1 (overwrite must update, not insert)", meta.created, meta.updated)
	}
	if second.Size != int64(len("second version")) {
		t.Errorf("size after overwrite = %d, want %d", second.Size, len("second version"))
	}
}

func TestDeleteFileRemovesMetadataWithoutDatabase(t *testing.T) {
	fm, meta, store := newFakeBackedManager(t)

	if _, err := fm.UploadFile("/docs/a.txt", []byte("payload")); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if err := fm.DeleteFile("/docs/a.txt"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if meta.removed != 1 {
		t.Errorf("removed = %d, want 1", meta.removed)
	}
	if exists, err := store.Exists("/docs/a.txt"); err != nil {
		t.Fatalf("Exists: %v", err)
	} else if exists {
		t.Error("storage object should be gone after DeleteFile")
	}
}
