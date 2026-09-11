package transfer

import (
	"database/sql"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// fakeMetadata is an in-memory metadataStore so the transfer flow can be
// exercised without PostgreSQL (issue #114).
type fakeMetadata struct {
	byPath  map[string]*database.FileMetadata
	created int
	updated int
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

func TestSequentialUploadCompletesWithoutDatabase(t *testing.T) {
	meta := newFakeMetadata()
	store := storage.NewLocalStorage(t.TempDir())
	svc := NewFileTransferServiceWithStore(store, meta)
	t.Cleanup(svc.StopCleanupThread)

	payload := []byte("chunk-one|chunk-two")
	sessionID, err := svc.CreateUploadSession("/fake/upload.bin", "upload.bin", int64(len(payload)), "client-1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}

	split := 10
	if err := svc.UploadChunk(sessionID, payload[:split], 0); err != nil {
		t.Fatalf("UploadChunk(first): %v", err)
	}
	if err := svc.UploadChunk(sessionID, payload[split:], int64(split)); err != nil {
		t.Fatalf("UploadChunk(second): %v", err)
	}

	result, err := svc.CompleteUpload(sessionID)
	if err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	if result == nil {
		t.Fatal("CompleteUpload returned nil result")
	}

	if meta.created != 1 {
		t.Errorf("metadata Create called %d times, want 1", meta.created)
	}
	stored, err := store.Read("/fake/upload.bin")
	if err != nil {
		t.Fatalf("stored object missing: %v", err)
	}
	if string(stored) != string(payload) {
		t.Errorf("stored payload = %q, want %q", stored, payload)
	}
}
