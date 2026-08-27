package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
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

func TestUploadChunkRejectsNonSequentialOffsets(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	sessionID, err := svc.CreateUploadSession("sequential.txt", "sequential.txt", 8, "client1", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}
	if err := svc.UploadChunk(sessionID, []byte("abcd"), 0); err != nil {
		t.Fatalf("first UploadChunk failed: %v", err)
	}
	if err := svc.UploadChunk(sessionID, []byte("xx"), 2); err == nil {
		t.Fatal("expected overlapping chunk to be rejected")
	}
	if err := svc.UploadChunk(sessionID, []byte("yy"), 6); err == nil {
		t.Fatal("expected offset gap to be rejected")
	}
	if err := svc.UploadChunk(sessionID, []byte("efgh"), 4); err != nil {
		t.Fatalf("second sequential UploadChunk failed: %v", err)
	}
	if _, err := svc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}
}

func TestSequentialUploadResumesOnAnotherInstance(t *testing.T) {
	svc1, store, db := setupTestEnv(t)
	sharedSessions := distributed.NewMemorySessionStore()
	sharedLock := distributed.NewLocalDistributedLock()
	sharedTemp := t.TempDir()
	svc1.sessionStore = sharedSessions
	svc1.distLock = sharedLock
	if err := svc1.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc1: %v", err)
	}
	svc2 := NewFileTransferServiceWithRedis(store, db, sharedSessions, sharedLock)
	if err := svc2.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc2: %v", err)
	}

	sessionID, err := svc1.CreateUploadSession("resume-sequential.txt", "resume-sequential.txt", 8, "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	if err := svc1.UploadChunk(sessionID, []byte("abcd"), 0); err != nil {
		t.Fatalf("UploadChunk on first instance: %v", err)
	}
	if err := svc2.UploadChunk(sessionID, []byte("efgh"), 4); err != nil {
		t.Fatalf("UploadChunk on second instance: %v", err)
	}
	if _, err := svc2.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload on second instance: %v", err)
	}
	if active := atomic.LoadInt64(&svc2.sessionCount); active != 0 {
		t.Fatalf("second instance active session count = %d, want 0", active)
	}
	data, err := store.Read("resume-sequential.txt")
	if err != nil {
		t.Fatalf("Read completed upload: %v", err)
	}
	if string(data) != "abcdefgh" {
		t.Fatalf("completed data = %q, want %q", data, "abcdefgh")
	}
}

func TestEncryptedDownloadResumesOnAnotherInstance(t *testing.T) {
	svc1, store, db := setupTestEnv(t)
	sharedSessions := distributed.NewMemorySessionStore()
	sharedLock := distributed.NewLocalDistributedLock()
	svc1.sessionStore = sharedSessions
	svc1.distLock = sharedLock
	svc2 := NewFileTransferServiceWithRedis(store, db, sharedSessions, sharedLock)
	cryptoSvc := crypto.NewCryptoService()
	if err := cryptoSvc.Init("test-encryption-key"); err != nil {
		t.Fatalf("initialize crypto: %v", err)
	}
	svc1.SetCryptoService(cryptoSvc)
	svc2.SetCryptoService(cryptoSvc)

	plaintext := []byte("cross-instance encrypted download")
	sessionID, err := svc1.CreateUploadSession("encrypted.bin", "encrypted.bin", int64(len(plaintext)), "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	if err := svc1.UploadChunk(sessionID, plaintext, 0); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if _, err := svc1.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}

	downloadID, err := svc1.CreateDownloadSession("encrypted.bin", "client")
	if err != nil {
		t.Fatalf("CreateDownloadSession: %v", err)
	}
	data, err := svc2.DownloadChunk(downloadID, len(plaintext), 0)
	if err != nil {
		t.Fatalf("DownloadChunk on second instance: %v", err)
	}
	if !bytes.Equal(data, plaintext) {
		t.Fatalf("resumed plaintext = %q, want %q", data, plaintext)
	}
	if err := svc2.CompleteDownload(downloadID); err != nil {
		t.Fatalf("CompleteDownload on second instance: %v", err)
	}
}

func TestMultipartUploadResumesOnAnotherInstance(t *testing.T) {
	svc1, store, db := setupTestEnv(t)
	sharedSessions := distributed.NewMemorySessionStore()
	sharedLock := distributed.NewLocalDistributedLock()
	sharedTemp := t.TempDir()
	svc1.sessionStore = sharedSessions
	svc1.distLock = sharedLock
	if err := svc1.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc1: %v", err)
	}
	svc2 := NewFileTransferServiceWithRedis(store, db, sharedSessions, sharedLock)
	if err := svc2.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc2: %v", err)
	}

	sessionID, _, err := svc1.CreateMultipartUpload("resume-multipart.txt", "resume-multipart.txt", 8, "client", "")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := svc1.UploadPartData(sessionID, 1, 0, []byte("abcd")); err != nil {
		t.Fatalf("UploadPartData on first instance: %v", err)
	}
	if uploaded, total, parts := svc2.GetMultipartUploadProgress(sessionID); uploaded != 4 || total != 8 || parts != 1 {
		t.Fatalf("restored progress = (%d,%d,%d), want (4,8,1)", uploaded, total, parts)
	}
	if err := svc2.UploadPartData(sessionID, 2, 4, []byte("efgh")); err != nil {
		t.Fatalf("UploadPartData on second instance: %v", err)
	}
	if err := svc2.CompleteMultipartUpload(sessionID); err != nil {
		t.Fatalf("CompleteMultipartUpload on second instance: %v", err)
	}
	if active := atomic.LoadInt64(&svc2.sessionCount); active != 0 {
		t.Fatalf("second instance active session count = %d, want 0", active)
	}
	data, err := store.Read("resume-multipart.txt")
	if err != nil {
		t.Fatalf("Read completed upload: %v", err)
	}
	if string(data) != "abcdefgh" {
		t.Fatalf("completed data = %q, want %q", data, "abcdefgh")
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

	if _, err := svc.CompleteUpload(sessionID); err != nil {
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

func TestCompleteUploadIsIdempotentAfterSessionCacheLoss(t *testing.T) {
	svc, store, db := setupTestEnv(t)
	data := []byte("idempotent completion")
	sessionID, err := svc.CreateUploadSession("idempotent.txt", "idempotent.txt", int64(len(data)), "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	if err := svc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	first, err := svc.CompleteUpload(sessionID)
	if err != nil {
		t.Fatalf("first CompleteUpload: %v", err)
	}
	if err := svc.sessionStore.Delete(context.Background(), "upload", sessionID); err != nil {
		t.Fatalf("delete cached terminal state: %v", err)
	}
	svc2 := NewFileTransferService(store, db)
	second, err := svc2.CompleteUpload(sessionID)
	if err != nil {
		t.Fatalf("idempotent CompleteUpload: %v", err)
	}
	if *first != *second {
		t.Fatalf("completion result changed: first=%+v second=%+v", first, second)
	}
	if got, err := store.Read("idempotent.txt"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("committed file changed: data=%q err=%v", got, err)
	}
}

func TestCompleteUploadRollsBackWhenTerminalLedgerFails(t *testing.T) {
	svc, store, db := setupTestEnv(t)
	if _, err := db.Exec(`CREATE TRIGGER fail_upload_terminal BEFORE INSERT ON transfer_session_results
		WHEN NEW.session_type = 'upload' BEGIN SELECT RAISE(FAIL, 'forced terminal failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	data := []byte("must roll back")
	sessionID, err := svc.CreateUploadSession("ledger-failure.txt", "ledger-failure.txt", int64(len(data)), "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	if err := svc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if _, err := svc.CompleteUpload(sessionID); err == nil {
		t.Fatal("expected terminal ledger failure")
	}
	meta, err := database.NewFileMetadataService(db).GetByPath("ledger-failure.txt")
	if err != nil {
		t.Fatalf("GetByPath: %v", err)
	}
	if meta != nil || store.Exists("ledger-failure.txt") {
		t.Fatalf("failed completion left metadata=%v storageExists=%v", meta, store.Exists("ledger-failure.txt"))
	}
}

func TestCompleteMultipartUploadIsIdempotentAfterSessionCacheLoss(t *testing.T) {
	svc, store, db := setupTestEnv(t)
	data := []byte("multipart-idempotent")
	sessionID, _, err := svc.CreateMultipartUpload("multipart-idempotent.txt", "multipart-idempotent.txt", int64(len(data)), "client", "")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := svc.UploadPartData(sessionID, 1, 0, data); err != nil {
		t.Fatalf("UploadPartData: %v", err)
	}
	if err := svc.CompleteMultipartUpload(sessionID); err != nil {
		t.Fatalf("first CompleteMultipartUpload: %v", err)
	}
	if err := svc.sessionStore.Delete(context.Background(), "multipart_upload", sessionID); err != nil {
		t.Fatalf("delete cached terminal state: %v", err)
	}
	svc2 := NewFileTransferService(store, db)
	if err := svc2.CompleteMultipartUpload(sessionID); err != nil {
		t.Fatalf("idempotent CompleteMultipartUpload: %v", err)
	}
	if got, err := store.Read("multipart-idempotent.txt"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("committed multipart file changed: data=%q err=%v", got, err)
	}
}

func TestCompleteMultipartUploadRollsBackWhenTerminalLedgerFails(t *testing.T) {
	svc, store, db := setupTestEnv(t)
	if _, err := db.Exec(`CREATE TRIGGER fail_multipart_terminal BEFORE INSERT ON transfer_session_results
		WHEN NEW.session_type = 'multipart' BEGIN SELECT RAISE(FAIL, 'forced terminal failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	data := []byte("multipart rollback")
	sessionID, _, err := svc.CreateMultipartUpload("multipart-ledger-failure.txt", "multipart-ledger-failure.txt", int64(len(data)), "client", "")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := svc.UploadPartData(sessionID, 1, 0, data); err != nil {
		t.Fatalf("UploadPartData: %v", err)
	}
	if err := svc.CompleteMultipartUpload(sessionID); err == nil {
		t.Fatal("expected terminal ledger failure")
	}
	meta, err := database.NewFileMetadataService(db).GetByPath("multipart-ledger-failure.txt")
	if err != nil {
		t.Fatalf("GetByPath: %v", err)
	}
	if meta != nil || store.Exists("multipart-ledger-failure.txt") {
		t.Fatalf("failed multipart completion left metadata=%v storageExists=%v", meta, store.Exists("multipart-ledger-failure.txt"))
	}
}

func TestAbortMultipartUploadIsIdempotentAfterSessionCacheLoss(t *testing.T) {
	svc, store, db := setupTestEnv(t)
	sessionID, _, err := svc.CreateMultipartUpload("multipart-abort.txt", "multipart-abort.txt", 8, "client", "")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := svc.AbortMultipartUpload(sessionID); err != nil {
		t.Fatalf("first AbortMultipartUpload: %v", err)
	}
	if err := svc.sessionStore.Delete(context.Background(), "multipart_upload", sessionID); err != nil {
		t.Fatalf("delete cached terminal state: %v", err)
	}
	svc2 := NewFileTransferService(store, db)
	if err := svc2.AbortMultipartUpload(sessionID); err != nil {
		t.Fatalf("idempotent AbortMultipartUpload: %v", err)
	}
	if err := svc2.UploadPartData(sessionID, 1, 0, []byte("12345678")); err == nil {
		t.Fatal("aborted multipart session accepted a part after cache loss")
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

	_, err = svc.CompleteUpload(sessionID)
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

	_, err = svc.CompleteUpload(sessionID)
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

func TestAbortUploadIsIdempotentAfterSessionCacheLoss(t *testing.T) {
	svc, store, db := setupTestEnv(t)
	sessionID, err := svc.CreateUploadSession("abort-idempotent.txt", "abort-idempotent.txt", 8, "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	if err := svc.AbortUpload(sessionID); err != nil {
		t.Fatalf("first AbortUpload: %v", err)
	}
	if err := svc.sessionStore.Delete(context.Background(), "upload", sessionID); err != nil {
		t.Fatalf("delete cached terminal state: %v", err)
	}
	svc2 := NewFileTransferService(store, db)
	if err := svc2.AbortUpload(sessionID); err != nil {
		t.Fatalf("idempotent AbortUpload: %v", err)
	}
	if err := svc2.UploadChunk(sessionID, []byte("12345678"), 0); err == nil {
		t.Fatal("aborted session accepted a chunk after cache loss")
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

func TestGetUploadProgressFromSharedSessionStore(t *testing.T) {
	svc1, store, db := setupTestEnv(t)
	sharedSessions := distributed.NewMemorySessionStore()
	sharedLock := distributed.NewLocalDistributedLock()
	sharedTemp := t.TempDir()
	svc1.sessionStore = sharedSessions
	svc1.distLock = sharedLock
	if err := svc1.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc1: %v", err)
	}
	svc2 := NewFileTransferServiceWithRedis(store, db, sharedSessions, sharedLock)
	if err := svc2.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc2: %v", err)
	}

	sessionID, err := svc1.CreateUploadSession("shared-progress.txt", "shared-progress.txt", 8, "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	if err := svc1.UploadChunk(sessionID, []byte("abcd"), 0); err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if got := svc2.GetUploadProgress(sessionID); got != 4 {
		t.Fatalf("cross-instance progress = %d, want 4", got)
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
	old := utils.FormatTimestamp(time.Now().Add(-2 * time.Hour))
	session.CreatedAt = old
	session.UpdatedAt = old
	if err := svc.sessionStore.Set(context.Background(), "upload", sessionID, session, 2*time.Hour); err != nil {
		t.Fatalf("persist expired session: %v", err)
	}

	svc.CleanupExpiredSessions(3600)

	_, err = svc.GetUploadSession(sessionID)
	if err == nil {
		t.Errorf("expected expired session to be cleaned up, got nil")
	}
}

func TestCleanupExpiredSessionsUsesSharedLastActivity(t *testing.T) {
	svc1, store, db := setupTestEnv(t)
	sharedSessions := distributed.NewMemorySessionStore()
	sharedLock := distributed.NewLocalDistributedLock()
	sharedTemp := t.TempDir()
	svc1.sessionStore = sharedSessions
	svc1.distLock = sharedLock
	if err := svc1.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc1: %v", err)
	}
	svc2 := NewFileTransferServiceWithRedis(store, db, sharedSessions, sharedLock)
	if err := svc2.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("SetTempDir svc2: %v", err)
	}

	sessionID, err := svc1.CreateUploadSession("active.txt", "active.txt", 8, "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}
	local, err := svc1.GetUploadSession(sessionID)
	if err != nil {
		t.Fatalf("GetUploadSession: %v", err)
	}
	local.CreatedAt = utils.FormatTimestamp(time.Now().Add(-2 * time.Hour))
	local.UpdatedAt = local.CreatedAt

	if err := svc2.UploadChunk(sessionID, []byte("abcd"), 0); err != nil {
		t.Fatalf("UploadChunk on second instance: %v", err)
	}
	svc1.CleanupExpiredSessions(3600)

	if got := svc2.GetUploadProgress(sessionID); got != 4 {
		t.Fatalf("active shared session was cleaned, progress = %d, want 4", got)
	}
	if _, err := os.Stat(filepath.Join(sharedTemp, sessionID+".tmp")); err != nil {
		t.Fatalf("active shared temp file was removed: %v", err)
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

	if _, err := svc.CompleteUpload(sessionID); err != nil {
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
