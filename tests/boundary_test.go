package tests

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// boundaryEnv holds the dependencies needed by boundary tests.
type boundaryEnv struct {
	storage     *storage.LocalStorage
	fm          *filemanager.FileManager
	transferSvc *transfer.FileTransferService
	db          *database.DB
	dbObj       *database.Database
	storageDir  string
}

func setupBoundaryEnv(t *testing.T) *boundaryEnv {
	t.Helper()

	storageDir := t.TempDir()

	dbPath := filepath.Join(storageDir, "test.db")
	dbCfg := config.DatabaseConfig{
		Type: "sqlite",
		Path: dbPath,
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	qdb := dbObj.GetQueryDB()

	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1,
		Name:    "initial_schema",
		Up:      func() error { return database.InitTables(qdb) },
	})
	if err := migrationMgr.RunMigrations(); err != nil {
		dbObj.Close()
		t.Fatalf("failed to run migrations: %v", err)
	}

	ls := storage.NewLocalStorage(storageDir)
	fm := filemanager.NewFileManager(ls, qdb)
	transferSvc := transfer.NewFileTransferService(ls, qdb)

	t.Cleanup(func() {
		transferSvc.StopCleanupThread()
		dbObj.Close()
	})

	return &boundaryEnv{
		storage:     ls,
		fm:          fm,
		transferSvc: transferSvc,
		db:          qdb,
		dbObj:       dbObj,
		storageDir:  storageDir,
	}
}

func sha256Hex(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// TestBoundary is the parent test that groups all boundary subtests so they
// can be selected with `go test -run Boundary`.
func TestBoundary(t *testing.T) {
	// --- Storage layer boundary tests ---
	t.Run("LocalStorage_EmptyFile", testLocalStorage_EmptyFile)
	t.Run("LocalStorage_LargeFile", testLocalStorage_LargeFile)
	t.Run("LocalStorage_SpecialCharacters", testLocalStorage_SpecialCharacters)
	t.Run("LocalStorage_DeepNestedPath", testLocalStorage_DeepNestedPath)
	t.Run("LocalStorage_PathTraversal", testLocalStorage_PathTraversal)
	t.Run("LocalStorage_OverwriteExisting", testLocalStorage_OverwriteExisting)
	t.Run("LocalStorage_WriteAt_Offset", testLocalStorage_WriteAt_Offset)
	t.Run("LocalStorage_Rename", testLocalStorage_Rename)
	t.Run("LocalStorage_RemoveNonExistent", testLocalStorage_RemoveNonExistent)
	t.Run("LocalStorage_GetSize", testLocalStorage_GetSize)

	// --- File manager boundary tests ---
	t.Run("FileManager_EmptyFileUpload", testFileManager_EmptyFileUpload)
	t.Run("FileManager_LargeFileUpload", testFileManager_LargeFileUpload)
	t.Run("FileManager_DeleteAndReupload", testFileManager_DeleteAndReupload)

	// --- Transfer service boundary tests ---
	t.Run("Transfer_EmptyFileUpload", testTransfer_EmptyFileUpload)
	t.Run("Transfer_SingleChunkUpload", testTransfer_SingleChunkUpload)
	t.Run("Transfer_HashMismatch", testTransfer_HashMismatch)
	t.Run("Transfer_OffsetBeyondSize", testTransfer_OffsetBeyondSize)

	// --- Path validation boundary tests ---
	t.Run("PathValidation_EmptyPath", testPathValidation_EmptyPath)
	t.Run("PathValidation_RootPath", testPathValidation_RootPath)
	t.Run("PathValidation_DotDot", testPathValidation_DotDot)
	t.Run("PathValidation_NormalPath", testPathValidation_NormalPath)
}

// ==================== Storage layer boundary tests ====================

func testLocalStorage_EmptyFile(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "empty.txt"

	if err := env.storage.Write(path, []byte{}); err != nil {
		t.Fatalf("Write empty file failed: %v", err)
	}

	data, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read empty file failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 bytes, got %d", len(data))
	}

	size, err := env.storage.GetSize(path)
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if size != 0 {
		t.Errorf("expected size 0, got %d", size)
	}
}

func testLocalStorage_LargeFile(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "large.bin"

	size := int64(10 * 1024 * 1024) // 10MB
	data := make([]byte, size)
	pattern := []byte("BoundaryLargeFileTest2026!")
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}

	if err := env.storage.Write(path, data); err != nil {
		t.Fatalf("Write large file failed: %v", err)
	}

	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read large file failed: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("expected %d bytes, got %d", len(data), len(got))
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch")
	}

	gotSize, err := env.storage.GetSize(path)
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if gotSize != size {
		t.Errorf("expected size %d, got %d", size, gotSize)
	}
}

func testLocalStorage_SpecialCharacters(t *testing.T) {
	env := setupBoundaryEnv(t)

	cases := []string{
		"测试文件.txt",
		"file with spaces.txt",
		"emoji_😀_file.txt",
		"中文/目录/文件.txt",
	}
	for _, p := range cases {
		content := []byte("content for " + p)
		if err := env.storage.Write(p, content); err != nil {
			t.Errorf("Write %q failed: %v", p, err)
			continue
		}
		if !env.storage.Exists(p) {
			t.Errorf("Exists %q returned false", p)
			continue
		}
		got, err := env.storage.Read(p)
		if err != nil {
			t.Errorf("Read %q failed: %v", p, err)
			continue
		}
		if !bytes.Equal(got, content) {
			t.Errorf("content mismatch for %q", p)
		}
	}
}

func testLocalStorage_DeepNestedPath(t *testing.T) {
	env := setupBoundaryEnv(t)

	// 10-level nested path
	parts := make([]string, 10)
	for i := range parts {
		parts[i] = fmt.Sprintf("level%d", i+1)
	}
	deepPath := strings.Join(parts, "/") + "/file.txt"

	content := []byte("deep nested content")
	if err := env.storage.Write(deepPath, content); err != nil {
		t.Fatalf("Write to deep path failed: %v", err)
	}

	got, err := env.storage.Read(deepPath)
	if err != nil {
		t.Fatalf("Read from deep path failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch for deep path")
	}
}

func testLocalStorage_PathTraversal(t *testing.T) {
	env := setupBoundaryEnv(t)

	// Path traversal attack: should be rejected by ValidatePath and Write.
	if err := env.storage.ValidatePath("../etc/passwd"); err == nil {
		t.Errorf("ValidatePath should reject ../etc/passwd")
	}

	if err := env.storage.Write("../etc/passwd", []byte("malicious")); err == nil {
		t.Errorf("expected error for path traversal write, got nil")
	}
}

func testLocalStorage_OverwriteExisting(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "overwrite.txt"

	// Write initial content
	if err := env.storage.Write(path, []byte("initial")); err != nil {
		t.Fatalf("initial Write failed: %v", err)
	}

	// Overwrite with different (longer) content
	newContent := []byte("overwritten content that is longer")
	if err := env.storage.Write(path, newContent); err != nil {
		t.Fatalf("overwrite Write failed: %v", err)
	}

	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(got, newContent) {
		t.Errorf("expected overwritten content, got %q", string(got))
	}
}

func testLocalStorage_WriteAt_Offset(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "writeat.bin"

	// Write initial 10 bytes at offset 0
	if err := env.storage.WriteAt(path, []byte("0123456789"), 0); err != nil {
		t.Fatalf("WriteAt at offset 0 failed: %v", err)
	}

	// Overwrite bytes 3..5 with "ABC"
	if err := env.storage.WriteAt(path, []byte("ABC"), 3); err != nil {
		t.Fatalf("WriteAt at offset 3 failed: %v", err)
	}

	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	expected := []byte("012ABC6789")
	if !bytes.Equal(got, expected) {
		t.Errorf("after overwrite: expected %q, got %q", string(expected), string(got))
	}

	// Write at offset beyond current size — gap is zero-filled.
	if err := env.storage.WriteAt(path, []byte("END"), 15); err != nil {
		t.Fatalf("WriteAt beyond size failed: %v", err)
	}
	got, err = env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read after extend failed: %v", err)
	}
	if len(got) != 18 {
		t.Errorf("expected length 18, got %d", len(got))
	}
	if !bytes.HasSuffix(got, []byte("END")) {
		t.Errorf("expected suffix END, got %q", string(got))
	}
}

func testLocalStorage_Rename(t *testing.T) {
	env := setupBoundaryEnv(t)
	oldPath := "oldname.txt"
	newPath := "newname.txt"

	content := []byte("rename me")
	if err := env.storage.Write(oldPath, content); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := env.storage.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if env.storage.Exists(oldPath) {
		t.Errorf("old path should not exist after rename")
	}
	if !env.storage.Exists(newPath) {
		t.Errorf("new path should exist after rename")
	}

	got, err := env.storage.Read(newPath)
	if err != nil {
		t.Fatalf("Read renamed file failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch after rename")
	}
}

func testLocalStorage_RemoveNonExistent(t *testing.T) {
	env := setupBoundaryEnv(t)

	err := env.storage.Remove("does_not_exist.txt")
	if err == nil {
		t.Errorf("expected error when removing non-existent file, got nil")
	}
}

func testLocalStorage_GetSize(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "sized.txt"

	content := []byte("exactly 13 bytes")
	if err := env.storage.Write(path, content); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	size, err := env.storage.GetSize(path)
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), size)
	}

	// Non-existent file should return an error.
	_, err = env.storage.GetSize("not_exist.txt")
	if err == nil {
		t.Errorf("expected error for GetSize on non-existent file")
	}
}

// ==================== File manager boundary tests ====================

func testFileManager_EmptyFileUpload(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "fm_empty.txt"

	meta, err := env.fm.UploadFile(path, []byte{})
	if err != nil {
		t.Fatalf("UploadFile empty failed: %v", err)
	}
	if meta.Size != 0 {
		t.Errorf("expected size 0, got %d", meta.Size)
	}

	data, err := env.fm.DownloadFile(path)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 bytes, got %d", len(data))
	}
}

func testFileManager_LargeFileUpload(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "fm_large.bin"

	size := int64(1024 * 1024) // 1MB
	data := make([]byte, size)
	pattern := []byte("FMLarge1")
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}

	meta, err := env.fm.UploadFile(path, data)
	if err != nil {
		t.Fatalf("UploadFile large failed: %v", err)
	}
	if meta.Size != size {
		t.Errorf("expected size %d, got %d", size, meta.Size)
	}
	expectedHash := sha256Hex(data)
	if meta.Hash != expectedHash {
		t.Errorf("expected hash %s, got %s", expectedHash, meta.Hash)
	}

	got, err := env.fm.DownloadFile(path)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("content mismatch")
	}
}

func testFileManager_DeleteAndReupload(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "delete_reupload.txt"

	content1 := []byte("first version")
	if _, err := env.fm.UploadFile(path, content1); err != nil {
		t.Fatalf("first UploadFile failed: %v", err)
	}
	if !env.fm.Exists(path) {
		t.Fatalf("file should exist after upload")
	}

	if err := env.fm.DeleteFile(path); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}
	if env.fm.Exists(path) {
		t.Errorf("file should not exist after delete")
	}

	// Re-upload with different content
	content2 := []byte("second version with different content")
	meta, err := env.fm.UploadFile(path, content2)
	if err != nil {
		t.Fatalf("re-upload failed: %v", err)
	}
	if meta.Size != int64(len(content2)) {
		t.Errorf("expected size %d, got %d", len(content2), meta.Size)
	}

	got, err := env.fm.DownloadFile(path)
	if err != nil {
		t.Fatalf("DownloadFile after reupload failed: %v", err)
	}
	if !bytes.Equal(got, content2) {
		t.Errorf("content mismatch after reupload")
	}
}

// ==================== Transfer service boundary tests ====================

func testTransfer_EmptyFileUpload(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "transfer_empty.txt"
	fileName := "transfer_empty.txt"

	totalSize := int64(0)
	hash := sha256Hex([]byte{})

	sessionID, err := env.transferSvc.CreateUploadSession(path, fileName, totalSize, "client1", hash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	// No chunks needed for an empty file (UploadedSize 0 == TotalSize 0).
	if err := env.transferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	// Verify the file exists in storage and is empty.
	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 bytes, got %d", len(got))
	}
}

func testTransfer_SingleChunkUpload(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "transfer_single.bin"
	fileName := "transfer_single.bin"

	data := []byte("single chunk upload content")
	totalSize := int64(len(data))
	hash := sha256Hex(data)

	sessionID, err := env.transferSvc.CreateUploadSession(path, fileName, totalSize, "client1", hash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := env.transferSvc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	if err := env.transferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("content mismatch")
	}
}

func testTransfer_HashMismatch(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "transfer_mismatch.bin"
	fileName := "transfer_mismatch.bin"

	data := []byte("actual content")
	totalSize := int64(len(data))
	// Provide a wrong hash so CompleteUpload must fail.
	wrongHash := sha256Hex([]byte("different content"))

	sessionID, err := env.transferSvc.CreateUploadSession(path, fileName, totalSize, "client1", wrongHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := env.transferSvc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	err = env.transferSvc.CompleteUpload(sessionID)
	if err == nil {
		t.Fatalf("expected hash mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Errorf("expected hash mismatch error, got: %v", err)
	}

	// File should not exist in storage after hash mismatch.
	if env.storage.Exists(path) {
		t.Errorf("file should not exist in storage after hash mismatch")
	}
}

func testTransfer_OffsetBeyondSize(t *testing.T) {
	env := setupBoundaryEnv(t)
	path := "transfer_offset.bin"
	fileName := "transfer_offset.bin"

	totalSize := int64(100)
	hash := "" // no hash check needed for this test

	sessionID, err := env.transferSvc.CreateUploadSession(path, fileName, totalSize, "client1", hash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	// offset + data length exceeds total size → must be rejected.
	data := make([]byte, 20)
	err = env.transferSvc.UploadChunk(sessionID, data, 90)
	if err == nil {
		t.Errorf("expected error for offset beyond size, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "beyond file size") {
		t.Errorf("expected 'beyond file size' error, got: %v", err)
	}
}

// ==================== Path validation boundary tests ====================

func testPathValidation_EmptyPath(t *testing.T) {
	if utils.IsValidFilePath("") {
		t.Errorf("expected empty path to be invalid")
	}
}

func testPathValidation_RootPath(t *testing.T) {
	if utils.IsValidFilePath("/") {
		t.Errorf("expected root path '/' to be invalid")
	}
}

func testPathValidation_DotDot(t *testing.T) {
	if utils.IsValidFilePath("..") {
		t.Errorf("expected '..' to be invalid")
	}
	// Path containing a ".." segment.
	if utils.IsValidFilePath("foo/../bar") {
		t.Errorf("expected 'foo/../bar' to be invalid")
	}
}

func testPathValidation_NormalPath(t *testing.T) {
	cases := []string{
		"foo.txt",
		"foo/bar.txt",
		"deep/nested/path/file.txt",
	}
	for _, p := range cases {
		if !utils.IsValidFilePath(p) {
			t.Errorf("expected %q to be valid", p)
		}
	}
}
