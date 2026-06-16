package transfer

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func setupTestEnv(t *testing.T) (*FileTransferService, *storage.LocalStorage, *database.DB) {
	t.Helper()

	storageDir, err := os.MkdirTemp("", "transfer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(storageDir, "test.db")
	dbCfg := config.DatabaseConfig{
		Type: "sqlite",
		Path: dbPath,
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		os.RemoveAll(storageDir)
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
		os.RemoveAll(storageDir)
		t.Fatalf("failed to run migrations: %v", err)
	}

	ls := storage.NewLocalStorage(storageDir)
	svc := NewFileTransferService(ls, qdb)

	t.Cleanup(func() {
		svc.StopCleanupThread()
		dbObj.Close()
		os.RemoveAll(storageDir)
	})

	return svc, ls, qdb
}

func TestCreateUploadSession(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("test.txt", "test.txt", 1024, "client1", "abc123")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if sessionID == "" {
		t.Errorf("expected non-empty session ID")
	}

	session, err := svc.GetUploadSession(sessionID)
	if err != nil {
		t.Fatalf("GetUploadSession failed: %v", err)
	}

	if session.FilePath != "test.txt" {
		t.Errorf("expected FilePath test.txt, got %s", session.FilePath)
	}
	if session.FileName != "test.txt" {
		t.Errorf("expected FileName test.txt, got %s", session.FileName)
	}
	if session.TotalSize != 1024 {
		t.Errorf("expected TotalSize 1024, got %d", session.TotalSize)
	}
	if session.Hash != "abc123" {
		t.Errorf("expected Hash abc123, got %s", session.Hash)
	}
	if session.ClientID != "client1" {
		t.Errorf("expected ClientID client1, got %s", session.ClientID)
	}
	if session.Status != "active" {
		t.Errorf("expected Status active, got %s", session.Status)
	}
	if session.UploadedSize != 0 {
		t.Errorf("expected UploadedSize 0, got %d", session.UploadedSize)
	}
}

func TestUploadChunk(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("chunk.txt", "chunk.txt", 100, "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	chunk := []byte("hello world")
	if err := svc.UploadChunk(sessionID, chunk, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	session, err := svc.GetUploadSession(sessionID)
	if err != nil {
		t.Fatalf("GetUploadSession failed: %v", err)
	}

	if session.UploadedSize != int64(len(chunk)) {
		t.Errorf("expected UploadedSize %d, got %d", len(chunk), session.UploadedSize)
	}
}

func TestCompleteUpload(t *testing.T) {
	svc, _, db := setupTestEnv(t)

	data := []byte("complete upload test")
	sessionID, err := svc.CreateUploadSession("complete.txt", "complete.txt", int64(len(data)), "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := svc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	if err := svc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	meta, err := database.NewFileMetadataService(db).GetByPath("complete.txt")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if meta == nil {
		t.Fatalf("file metadata not found")
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), meta.Size)
	}
}

func TestCompleteUploadSizeMismatch(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("mismatch.txt", "mismatch.txt", 1024, "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	smallData := []byte("too small")
	if err := svc.UploadChunk(sessionID, smallData, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	err = svc.CompleteUpload(sessionID)
	if err == nil {
		t.Errorf("expected size mismatch error, got nil")
	}
}

func TestCompleteUploadHashMismatch(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	data := []byte("hash test data")
	wrongHash := utils.SHA256("wrong")

	sessionID, err := svc.CreateUploadSession("hashmismatch.txt", "hashmismatch.txt", int64(len(data)), "client1", wrongHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := svc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	err = svc.CompleteUpload(sessionID)
	if err == nil {
		t.Errorf("expected hash mismatch error, got nil")
	}
}

func TestAbortUpload(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("abort.txt", "abort.txt", 1024, "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := svc.AbortUpload(sessionID); err != nil {
		t.Fatalf("AbortUpload failed: %v", err)
	}

	_, err = svc.GetUploadSession(sessionID)
	if err == nil {
		t.Errorf("expected error getting aborted session, got nil")
	}
}

func TestGetUploadProgress(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("progress.txt", "progress.txt", 1000, "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	progress := svc.GetUploadProgress(sessionID)
	if progress != 0 {
		t.Errorf("expected initial progress 0, got %d", progress)
	}

	chunk := []byte("partial data here")
	if err := svc.UploadChunk(sessionID, chunk, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	progress = svc.GetUploadProgress(sessionID)
	if progress != int64(len(chunk)) {
		t.Errorf("expected progress %d, got %d", len(chunk), progress)
	}
}

func TestCreateDownloadSession(t *testing.T) {
	svc, _, db := setupTestEnv(t)

	data := []byte("download session test")
	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            "dlsession.txt",
		Name:            "dlsession.txt",
		Size:            int64(len(data)),
		Hash:            fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := svc.CreateDownloadSession("dlsession.txt", "client1")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	if sessionID == "" {
		t.Errorf("expected non-empty session ID")
	}
}

func TestDownloadChunk(t *testing.T) {
	svc, ls, db := setupTestEnv(t)

	data := []byte("chunk download test data")
	if err := ls.Write("dlchunk.txt", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            "dlchunk.txt",
		Name:            "dlchunk.txt",
		Size:            int64(len(data)),
		Hash:            fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := svc.CreateDownloadSession("dlchunk.txt", "client1")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	chunk, err := svc.DownloadChunk(sessionID, 6, 0)
	if err != nil {
		t.Fatalf("DownloadChunk failed: %v", err)
	}

	if string(chunk) != "chunk " {
		t.Errorf("expected 'chunk ', got %q", string(chunk))
	}
}

func TestCompleteDownload(t *testing.T) {
	svc, ls, db := setupTestEnv(t)

	data := []byte("complete download cycle")
	if err := ls.Write("cycledl.txt", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            "cycledl.txt",
		Name:            "cycledl.txt",
		Size:            int64(len(data)),
		Hash:            fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := svc.CreateDownloadSession("cycledl.txt", "client1")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	var reassembled []byte
	var offset int64
	for offset < int64(len(data)) {
		sz := 5
		if int64(sz) > int64(len(data))-offset {
			sz = int(int64(len(data)) - offset)
		}
		chunk, err := svc.DownloadChunk(sessionID, sz, offset)
		if err != nil {
			t.Fatalf("DownloadChunk failed: %v", err)
		}
		reassembled = append(reassembled, chunk...)
		offset += int64(len(chunk))
	}

	if err := svc.CompleteDownload(sessionID); err != nil {
		t.Fatalf("CompleteDownload failed: %v", err)
	}

	if !bytes.Equal(reassembled, data) {
		t.Errorf("reassembled content does not match original")
	}
}

func TestAbortDownload(t *testing.T) {
	svc, ls, db := setupTestEnv(t)

	data := []byte("abort download")
	if err := ls.Write("abortdl.txt", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            "abortdl.txt",
		Name:            "abortdl.txt",
		Size:            int64(len(data)),
		Hash:            fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := svc.CreateDownloadSession("abortdl.txt", "client1")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	if err := svc.AbortDownload(sessionID); err != nil {
		t.Fatalf("AbortDownload failed: %v", err)
	}

	_, err = svc.DownloadChunk(sessionID, 5, 0)
	if err == nil {
		t.Errorf("expected error downloading from aborted session, got nil")
	}
}

func TestCleanupExpiredSessions(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("expired.txt", "expired.txt", 1024, "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	session, _ := svc.GetUploadSession(sessionID)
	session.CreatedAt = utils.FormatTimestamp(time.Now().Add(-2 * time.Hour))

	svc.CleanupExpiredSessions(3600)

	_, err = svc.GetUploadSession(sessionID)
	if err == nil {
		t.Errorf("expected expired session to be cleaned up, got nil")
	}
}

func TestLargeFileStreamingUpload(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	chunkSize := 1024 * 1024
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, err := svc.CreateUploadSession("large_stream.bin", "large_stream.bin", totalSize, "client1", expectedHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	var offset int64
	for offset < totalSize {
		end := offset + int64(chunkSize)
		if end > totalSize {
			end = totalSize
		}
		chunk := data[offset:end]
		if err := svc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("UploadChunk at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if err := svc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	_, err = svc.GetUploadSession(sessionID)
	if err == nil {
		t.Errorf("expected session to be deleted after completion")
	}

	result, err := svc.storage.Read("large_stream.bin")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("large file content mismatch")
	}
}

func TestLargeFileStreamingDownload(t *testing.T) {
	svc, ls, db := setupTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := ls.Write("large_dl.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            "large_dl.bin",
		Name:            "large_dl.bin",
		Size:            totalSize,
		Hash:            fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := svc.CreateDownloadSession("large_dl.bin", "client1")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	chunkSize := 1024 * 1024
	var reassembled []byte
	var offset int64

	for offset < totalSize {
		remaining := totalSize - offset
		sz := int64(chunkSize)
		if remaining < sz {
			sz = remaining
		}
		chunk, err := svc.DownloadChunk(sessionID, int(sz), offset)
		if err != nil {
			t.Fatalf("DownloadChunk at offset %d failed: %v", offset, err)
		}
		reassembled = append(reassembled, chunk...)
		offset += int64(len(chunk))
	}

	if !bytes.Equal(reassembled, data) {
		t.Errorf("large file download content mismatch")
	}
}
