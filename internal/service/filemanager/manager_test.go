package filemanager

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

func setupFileManager(t *testing.T) (*FileManager, func()) {
	t.Helper()
	cfg := config.DatabaseConfig{Type: "sqlite", Path: ":memory:", PoolSize: 1}
	db := database.NewDatabase()
	if err := db.Connect(cfg); err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}
	queryDB := db.GetQueryDB()
	if err := database.InitTables(queryDB); err != nil {
		t.Fatalf("failed to init tables: %v", err)
	}
	store := storage.NewLocalStorage(t.TempDir())
	fm := NewFileManager(store, queryDB)
	return fm, func() { db.Close() }
}

func expectedHash(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func TestUploadFile(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "test.txt"
	data := []byte("hello world")
	meta, err := fm.UploadFile(path, data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if meta.Path != path {
		t.Errorf("meta.Path = %q, want %q", meta.Path, path)
	}
	if meta.Name != "test.txt" {
		t.Errorf("meta.Name = %q, want %q", meta.Name, "test.txt")
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("meta.Size = %d, want %d", meta.Size, len(data))
	}
	if meta.Hash != expectedHash(data) {
		t.Errorf("meta.Hash = %q, want %q", meta.Hash, expectedHash(data))
	}
	if meta.StorageType != "local" {
		t.Errorf("meta.StorageType = %q, want %q", meta.StorageType, "local")
	}
	if meta.IsDeleted {
		t.Errorf("meta.IsDeleted = true, want false")
	}
	if meta.CreatedAt == "" || meta.UpdatedAt == "" {
		t.Errorf("timestamps should not be empty")
	}
}

func TestUploadFile_Overwrite(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "overwrite.txt"
	original := []byte("original content")
	meta1, err := fm.UploadFile(path, original)
	if err != nil {
		t.Fatalf("first UploadFile failed: %v", err)
	}

	updated := []byte("updated content with more bytes")
	meta2, err := fm.UploadFile(path, updated)
	if err != nil {
		t.Fatalf("overwrite UploadFile failed: %v", err)
	}

	if meta2.ID != meta1.ID {
		t.Errorf("overwrite should keep same ID: got %q, want %q", meta2.ID, meta1.ID)
	}
	if meta2.Size != int64(len(updated)) {
		t.Errorf("meta2.Size = %d, want %d", meta2.Size, len(updated))
	}
	if meta2.Hash != expectedHash(updated) {
		t.Errorf("meta2.Hash = %q, want %q", meta2.Hash, expectedHash(updated))
	}

	got, err := fm.DownloadFile(path)
	if err != nil {
		t.Fatalf("DownloadFile after overwrite failed: %v", err)
	}
	if string(got) != string(updated) {
		t.Errorf("content after overwrite = %q, want %q", string(got), string(updated))
	}
}

func TestUploadFileFromReader_StagesAndCommits(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "streamed/upload.bin"
	data := strings.Repeat("streamed-data-", 4096)
	meta, err := fm.UploadFileFromReader(path, strings.NewReader(data))
	if err != nil {
		t.Fatalf("UploadFileFromReader failed: %v", err)
	}
	if meta.Size != int64(len(data)) || meta.Hash != expectedHash([]byte(data)) {
		t.Fatalf("streamed metadata size/hash mismatch: size=%d hash=%s", meta.Size, meta.Hash)
	}
	stored, err := fm.storage.Read(path)
	if err != nil {
		t.Fatalf("read streamed file: %v", err)
	}
	if string(stored) != data {
		t.Fatal("streamed file content mismatch")
	}
}

func TestUploadFileFromReader_OverwriteRollsBackWhenMetadataUpdateFails(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "streamed-rollback.bin"
	original := []byte("original streamed content")
	if _, err := fm.UploadFile(path, original); err != nil {
		t.Fatalf("initial UploadFile failed: %v", err)
	}
	if _, err := fm.db.Exec(`CREATE TRIGGER fail_streamed_update BEFORE UPDATE ON files BEGIN SELECT RAISE(FAIL, 'forced streamed update failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	if _, err := fm.UploadFileFromReader(path, strings.NewReader("replacement streamed content")); err == nil {
		t.Fatal("expected streamed metadata failure")
	}
	stored, err := fm.storage.Read(path)
	if err != nil {
		t.Fatalf("read restored streamed file: %v", err)
	}
	if string(stored) != string(original) {
		t.Fatalf("streamed rollback content = %q, want %q", stored, original)
	}
}

func TestUploadFile_OverwriteRollsBackWhenMetadataUpdateFails(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "rollback-overwrite.txt"
	original := []byte("original content")
	if _, err := fm.UploadFile(path, original); err != nil {
		t.Fatalf("initial UploadFile failed: %v", err)
	}
	if _, err := fm.db.Exec(`CREATE TRIGGER fail_file_update BEFORE UPDATE ON files BEGIN SELECT RAISE(FAIL, 'forced update failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	if _, err := fm.UploadFile(path, []byte("replacement content")); err == nil {
		t.Fatal("expected overwrite metadata failure")
	}
	data, err := fm.storage.Read(path)
	if err != nil {
		t.Fatalf("read rolled-back file: %v", err)
	}
	if string(data) != string(original) {
		t.Fatalf("content after rollback = %q, want %q", data, original)
	}
}

func TestDeleteFile_RollsBackWhenMetadataUpdateFails(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "rollback-delete.txt"
	original := []byte("keep me")
	if _, err := fm.UploadFile(path, original); err != nil {
		t.Fatalf("initial UploadFile failed: %v", err)
	}
	if _, err := fm.db.Exec(`CREATE TRIGGER fail_file_delete BEFORE UPDATE ON files BEGIN SELECT RAISE(FAIL, 'forced delete failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	if err := fm.DeleteFile(path); err == nil {
		t.Fatal("expected delete metadata failure")
	}
	data, err := fm.storage.Read(path)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != string(original) {
		t.Fatalf("content after delete rollback = %q, want %q", data, original)
	}
}

func TestDownloadFile(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "download.txt"
	data := []byte("download me please")
	if _, err := fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	got, err := fm.DownloadFile(path)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("DownloadFile content = %q, want %q", string(got), string(data))
	}
}

func TestDownloadFileAt(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "range.txt"
	data := []byte("Hello, World!")
	if _, err := fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	got, err := fm.DownloadFileAt(path, 5, 7)
	if err != nil {
		t.Fatalf("DownloadFileAt failed: %v", err)
	}
	if string(got) != "World" {
		t.Errorf("DownloadFileAt(5,7) = %q, want %q", string(got), "World")
	}

	// read from start
	gotStart, err := fm.DownloadFileAt(path, 5, 0)
	if err != nil {
		t.Fatalf("DownloadFileAt start failed: %v", err)
	}
	if string(gotStart) != "Hello" {
		t.Errorf("DownloadFileAt(5,0) = %q, want %q", string(gotStart), "Hello")
	}
}

func TestDownloadFileData(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "download-data.txt"
	data := []byte("download me please")
	if _, err := fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, err := fm.GetFileMetadata(path)
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}

	got, err := fm.DownloadFileData(meta)
	if err != nil {
		t.Fatalf("DownloadFileData failed: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("DownloadFileData content = %q, want %q", string(got), string(data))
	}
}

func TestDownloadFileDataAt(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "range-data.txt"
	data := []byte("Hello, World!")
	if _, err := fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, err := fm.GetFileMetadata(path)
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}

	got, err := fm.DownloadFileDataAt(meta, 5, 7)
	if err != nil {
		t.Fatalf("DownloadFileDataAt failed: %v", err)
	}
	if string(got) != "World" {
		t.Errorf("DownloadFileDataAt(5,7) = %q, want %q", string(got), "World")
	}

	gotStart, err := fm.DownloadFileDataAt(meta, 5, 0)
	if err != nil {
		t.Fatalf("DownloadFileDataAt start failed: %v", err)
	}
	if string(gotStart) != "Hello" {
		t.Errorf("DownloadFileDataAt(5,0) = %q, want %q", string(gotStart), "Hello")
	}
}

func TestDownloadFileData_NilMeta(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	if _, err := fm.DownloadFileData(nil); err == nil {
		t.Fatal("DownloadFileData(nil) expected error, got nil")
	}
	if _, err := fm.DownloadFileDataAt(nil, 5, 0); err == nil {
		t.Fatal("DownloadFileDataAt(nil,...) expected error, got nil")
	}
}

func TestDownloadFile_NotFound(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	_, err := fm.DownloadFile("does/not/exist.txt")
	if err == nil {
		t.Fatal("DownloadFile expected error for missing file, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

func TestDeleteFile(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "delete.txt"
	data := []byte("to be deleted")
	if _, err := fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if err := fm.DeleteFile(path); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	if _, err := fm.DownloadFile(path); err == nil {
		t.Error("DownloadFile should fail after delete, got nil")
	}
}

func TestDeleteFile_NotFound(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	if err := fm.DeleteFile("no/such/file.txt"); err == nil {
		t.Fatal("DeleteFile expected error for missing file, got nil")
	}
}

func TestRenameFile(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	oldPath := "rename_me.txt"
	data := []byte("rename content")
	if _, err := fm.UploadFile(oldPath, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if err := fm.RenameFile(oldPath, "renamed.txt"); err != nil {
		t.Fatalf("RenameFile failed: %v", err)
	}

	// old path no longer exists
	if _, err := fm.GetFileMetadata(oldPath); err == nil {
		t.Error("GetFileMetadata for old path should fail after rename")
	}

	// new path exists with correct content
	got, err := fm.DownloadFile("renamed.txt")
	if err != nil {
		t.Fatalf("DownloadFile for renamed path failed: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("content after rename = %q, want %q", string(got), string(data))
	}

	meta, err := fm.GetFileMetadata("renamed.txt")
	if err != nil {
		t.Fatalf("GetFileMetadata for renamed path failed: %v", err)
	}
	if meta.Name != "renamed.txt" {
		t.Errorf("meta.Name = %q, want %q", meta.Name, "renamed.txt")
	}
}

func TestRenameFileConcurrentSameTarget(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	if _, err := fm.UploadFile("a.txt", []byte("a")); err != nil {
		t.Fatalf("UploadFile a.txt: %v", err)
	}
	if _, err := fm.UploadFile("b.txt", []byte("b")); err != nil {
		t.Fatalf("UploadFile b.txt: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, source := range []string{"a.txt", "b.txt"} {
		go func(path string) {
			<-start
			errs <- fm.RenameFile(path, "target.txt")
		}(source)
	}
	close(start)
	err1, err2 := <-errs, <-errs
	if (err1 == nil) == (err2 == nil) {
		t.Fatalf("exactly one rename must succeed, got errors %v and %v", err1, err2)
	}
	data, err := fm.DownloadFile("target.txt")
	if err != nil {
		t.Fatalf("DownloadFile target.txt: %v", err)
	}
	if string(data) != "a" && string(data) != "b" {
		t.Fatalf("unexpected target content %q", data)
	}
	remaining := 0
	for _, source := range []string{"a.txt", "b.txt"} {
		if _, err := fm.GetFileMetadata(source); err == nil {
			remaining++
		}
	}
	if remaining != 1 {
		t.Fatalf("remaining source count = %d, want 1", remaining)
	}
}

func TestGetFileMetadata(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	path := "meta.txt"
	data := []byte("metadata test")
	if _, err := fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, err := fm.GetFileMetadata(path)
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}
	if meta.Path != path {
		t.Errorf("meta.Path = %q, want %q", meta.Path, path)
	}
	if meta.Name != "meta.txt" {
		t.Errorf("meta.Name = %q, want %q", meta.Name, "meta.txt")
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("meta.Size = %d, want %d", meta.Size, len(data))
	}
	if meta.Hash != expectedHash(data) {
		t.Errorf("meta.Hash = %q, want %q", meta.Hash, expectedHash(data))
	}
	if meta.StorageType != "local" {
		t.Errorf("meta.StorageType = %q, want %q", meta.StorageType, "local")
	}
}

func TestFileManager_ConcurrentUpload(t *testing.T) {
	fm, cleanup := setupFileManager(t)
	defer cleanup()

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path := fmt.Sprintf("concurrent/file%d.txt", idx)
			data := []byte(fmt.Sprintf("content-%d", idx))
			_, errs[idx] = fm.UploadFile(path, data)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d upload failed: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		path := fmt.Sprintf("concurrent/file%d.txt", i)
		want := fmt.Sprintf("content-%d", i)
		got, err := fm.DownloadFile(path)
		if err != nil {
			t.Fatalf("DownloadFile %s failed: %v", path, err)
		}
		if string(got) != want {
			t.Errorf("file %s content = %q, want %q", path, string(got), want)
		}
	}
}
