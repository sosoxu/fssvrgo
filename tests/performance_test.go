package tests

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// setupPerfEnv creates a test environment for performance and stress tests.
// It mirrors the setupBoundaryEnv pattern but accepts testing.TB so it can
// serve both *testing.T (stress tests) and *testing.B (benchmarks).
//
// The environment uses LocalStorage backed by a t.TempDir() directory, a
// file-based SQLite database stored in the same temp directory, and the
// default in-process distributed lock that NewFileManager / NewFileTransferService
// install via distributed.NewLocalDistributedLock(). No Redis or MinIO
// dependencies are required.
func setupPerfEnv(tb testing.TB) *boundaryEnv {
	tb.Helper()

	storageDir := tb.TempDir()

	dbPath := filepath.Join(storageDir, "perf.db")
	dbCfg := config.DatabaseConfig{
		Type: "sqlite",
		Path: dbPath,
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		tb.Fatalf("failed to connect database: %v", err)
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
		tb.Fatalf("failed to run migrations: %v", err)
	}

	// SQLite allows only one writer at a time, and the per-connection
	// busy_timeout PRAGMA set in connectSQLite does not propagate to every
	// connection in the pool. Pin the pool to a single connection so the
	// Go database/sql layer serializes access and we avoid SQLITE_BUSY
	// errors under concurrent stress workloads.
	dbObj.GetDB().SetMaxOpenConns(1)
	dbObj.GetDB().SetMaxIdleConns(1)

	ls := storage.NewLocalStorage(storageDir)
	fm := filemanager.NewFileManager(ls, qdb)
	transferSvc := transfer.NewFileTransferService(ls, qdb)

	tb.Cleanup(func() {
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

// ==================== Benchmarks ====================

// BenchmarkLocalStorage_Write measures raw write performance for 1KB payloads.
func BenchmarkLocalStorage_Write(b *testing.B) {
	env := setupPerfEnv(b)
	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := env.storage.Write("bench_write.bin", data); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

// BenchmarkLocalStorage_Read measures raw read performance on a pre-written file.
func BenchmarkLocalStorage_Read(b *testing.B) {
	env := setupPerfEnv(b)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := env.storage.Write("bench_read.bin", data); err != nil {
		b.Fatalf("Write failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := env.storage.Read("bench_read.bin"); err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

// BenchmarkLocalStorage_WriteAt measures offset-based write performance.
func BenchmarkLocalStorage_WriteAt(b *testing.B) {
	env := setupPerfEnv(b)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	// Pre-create the file so WriteAt does not pay directory creation cost each call.
	if err := env.storage.Write("bench_writeat.bin", data); err != nil {
		b.Fatalf("Write failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := env.storage.WriteAt("bench_writeat.bin", data, 0); err != nil {
			b.Fatalf("WriteAt failed: %v", err)
		}
	}
}

// BenchmarkFileManager_Upload measures FileManager.UploadFile performance,
// including storage write and metadata persistence.
func BenchmarkFileManager_Upload(b *testing.B) {
	env := setupPerfEnv(b)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := env.fm.UploadFile("bench_upload.bin", data); err != nil {
			b.Fatalf("UploadFile failed: %v", err)
		}
	}
}

// BenchmarkFileManager_Download measures FileManager.DownloadFile performance,
// including metadata lookup and storage read.
func BenchmarkFileManager_Download(b *testing.B) {
	env := setupPerfEnv(b)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := env.fm.UploadFile("bench_download.bin", data); err != nil {
		b.Fatalf("UploadFile failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := env.fm.DownloadFile("bench_download.bin"); err != nil {
			b.Fatalf("DownloadFile failed: %v", err)
		}
	}
}

// BenchmarkTransfer_UploadChunk measures the streaming chunk upload path,
// including session creation, chunk write, and abort (cleanup).
func BenchmarkTransfer_UploadChunk(b *testing.B) {
	env := setupPerfEnv(b)
	chunkSize := int64(64 * 1024) // 64KB
	data := make([]byte, chunkSize)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("bench_chunk_%d.bin", i)
		sessionID, err := env.transferSvc.CreateUploadSession(path, path, chunkSize, "bench", "")
		if err != nil {
			b.Fatalf("CreateUploadSession failed: %v", err)
		}
		if err := env.transferSvc.UploadChunk(sessionID, data, 0); err != nil {
			b.Fatalf("UploadChunk failed: %v", err)
		}
		if err := env.transferSvc.AbortUpload(sessionID); err != nil {
			b.Fatalf("AbortUpload failed: %v", err)
		}
	}
}

// BenchmarkTransfer_ConcurrentUpload measures concurrent streaming chunk upload
// performance with 4 goroutines writing to non-overlapping offsets.
func BenchmarkTransfer_ConcurrentUpload(b *testing.B) {
	env := setupPerfEnv(b)
	concurrency := 4
	chunkSize := int64(64 * 1024) // 64KB
	data := make([]byte, chunkSize)
	totalSize := chunkSize * int64(concurrency)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("bench_conc_%d.bin", i)
		sessionID, err := env.transferSvc.CreateUploadSession(path, path, totalSize, "bench", "")
		if err != nil {
			b.Fatalf("CreateUploadSession failed: %v", err)
		}

		var wg sync.WaitGroup
		errCh := make(chan error, concurrency)
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				offset := int64(idx) * chunkSize
				if err := env.transferSvc.UploadChunk(sessionID, data, offset); err != nil {
					errCh <- fmt.Errorf("chunk %d: %w", idx, err)
				}
			}(j)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			b.Errorf("concurrent upload error: %v", err)
		}

		if err := env.transferSvc.AbortUpload(sessionID); err != nil {
			b.Fatalf("AbortUpload failed: %v", err)
		}
	}
}

// BenchmarkCrypto_EncryptDecrypt measures AES-GCM encrypt+decrypt throughput
// using the project's CryptoService.
func BenchmarkCrypto_EncryptDecrypt(b *testing.B) {
	cs := crypto.NewCryptoService()
	if err := cs.Init("test-key-32-bytes-long-1234567890"); err != nil {
		b.Fatalf("Init failed: %v", err)
	}
	plaintext := "benchmark test data for encryption and decryption performance measurement"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		encrypted, err := cs.Encrypt(plaintext)
		if err != nil {
			b.Fatalf("Encrypt failed: %v", err)
		}
		if _, err := cs.Decrypt(encrypted); err != nil {
			b.Fatalf("Decrypt failed: %v", err)
		}
	}
}

// ==================== Stress Tests ====================

// TestStress_ConcurrentUploads exercises 100 goroutines uploading distinct
// files concurrently through FileManager.
func TestStress_ConcurrentUploads(t *testing.T) {
	t.Parallel()
	env := setupPerfEnv(t)

	const n = 100
	var wg sync.WaitGroup
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path := fmt.Sprintf("stress_upload_%d.bin", idx)
			data := []byte(fmt.Sprintf("content for file %d", idx))
			if _, err := env.fm.UploadFile(path, data); err != nil {
				errCh <- fmt.Errorf("upload %d: %w", idx, err)
				return
			}
			if !env.fm.Exists(path) {
				errCh <- fmt.Errorf("upload %d: file does not exist after upload", idx)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent upload error: %v", err)
	}
}

// TestStress_ConcurrentReads exercises 100 goroutines reading the same file.
func TestStress_ConcurrentReads(t *testing.T) {
	t.Parallel()
	env := setupPerfEnv(t)

	path := "stress_read_target.bin"
	data := []byte("shared file content for concurrent reads")
	if _, err := env.fm.UploadFile(path, data); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	const n = 100
	var wg sync.WaitGroup
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := env.fm.DownloadFile(path)
			if err != nil {
				errCh <- fmt.Errorf("download: %w", err)
				return
			}
			if !bytes.Equal(got, data) {
				errCh <- fmt.Errorf("content mismatch: expected %q, got %q", string(data), string(got))
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent read error: %v", err)
	}
}

// TestStress_MixedWorkload runs a mixed read/write/delete workload
// (50 reads + 30 writes + 20 deletes) concurrently. Each operation targets
// a disjoint file set so that no two operations collide on the same path.
func TestStress_MixedWorkload(t *testing.T) {
	t.Parallel()
	env := setupPerfEnv(t)

	const (
		numReads   = 50
		numWrites  = 30
		numDeletes = 20
	)
	totalOps := numReads + numWrites + numDeletes

	// Pre-create files for reads and deletes so the operations have targets.
	for i := 0; i < numReads; i++ {
		path := fmt.Sprintf("mixed_read_%d.bin", i)
		if _, err := env.fm.UploadFile(path, []byte("read target")); err != nil {
			t.Fatalf("pre-upload read %d failed: %v", i, err)
		}
	}
	for i := 0; i < numDeletes; i++ {
		path := fmt.Sprintf("mixed_del_%d.bin", i)
		if _, err := env.fm.UploadFile(path, []byte("delete target")); err != nil {
			t.Fatalf("pre-upload del %d failed: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, totalOps)

	// 50 reads on stable read-only files.
	for i := 0; i < numReads; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path := fmt.Sprintf("mixed_read_%d.bin", idx)
			if _, err := env.fm.DownloadFile(path); err != nil {
				errCh <- fmt.Errorf("read %d: %w", idx, err)
			}
		}(i)
	}

	// 30 writes creating brand-new files.
	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path := fmt.Sprintf("mixed_new_%d.bin", idx)
			data := []byte(fmt.Sprintf("new content %d", idx))
			if _, err := env.fm.UploadFile(path, data); err != nil {
				errCh <- fmt.Errorf("write %d: %w", idx, err)
			}
		}(i)
	}

	// 20 deletes on files dedicated to deletion.
	for i := 0; i < numDeletes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path := fmt.Sprintf("mixed_del_%d.bin", idx)
			if err := env.fm.DeleteFile(path); err != nil {
				errCh <- fmt.Errorf("delete %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("mixed workload error: %v", err)
	}
}

// TestStress_LargeFileStreaming performs a 10MB streaming upload via the
// transfer service and verifies the resulting file's integrity.
func TestStress_LargeFileStreaming(t *testing.T) {
	t.Parallel()
	env := setupPerfEnv(t)

	totalSize := int64(10 * 1024 * 1024) // 10MB
	data := generateTestData(totalSize)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	path := "stress_large_streaming.bin"
	fileName := "stress_large_streaming.bin"

	sessionID, err := env.transferSvc.CreateUploadSession(path, fileName, totalSize, "stress_client", expectedHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	// Stream the file in 1MB chunks.
	chunkSize := int64(1024 * 1024)
	var offset int64
	for offset < totalSize {
		end := offset + chunkSize
		if end > totalSize {
			end = totalSize
		}
		chunk := data[offset:end]
		if err := env.transferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("UploadChunk at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if _, err := env.transferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	// Verify integrity.
	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(got) != int(totalSize) {
		t.Fatalf("expected size %d, got %d", totalSize, len(got))
	}
	gotHash := fmt.Sprintf("%x", sha256.Sum256(got))
	if gotHash != expectedHash {
		t.Fatalf("hash mismatch: expected %s, got %s", expectedHash, gotHash)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("content mismatch after streaming upload")
	}
}

// TestStress_MultipartUpload uploads a 5MB file split into 5 parts uploaded
// concurrently, then verifies the assembled file's integrity.
func TestStress_MultipartUpload(t *testing.T) {
	t.Parallel()
	env := setupPerfEnv(t)

	totalSize := int64(5 * 1024 * 1024) // 5MB
	data := generateTestData(totalSize)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	path := "stress_multipart.bin"
	fileName := "stress_multipart.bin"

	sessionID, _, err := env.transferSvc.CreateMultipartUpload(path, fileName, totalSize, "stress_client", expectedHash)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	const numParts = 5
	partSize := totalSize / int64(numParts)

	var wg sync.WaitGroup
	errCh := make(chan error, numParts)

	for i := 0; i < numParts; i++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			offset := int64(partNum) * partSize
			end := offset + partSize
			if partNum == numParts-1 {
				end = totalSize
			}
			chunk := data[offset:end]
			if err := env.transferSvc.UploadPartData(sessionID, partNum+1, offset, chunk); err != nil {
				errCh <- fmt.Errorf("part %d: %w", partNum+1, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("multipart upload error: %v", err)
	}

	if err := env.transferSvc.CompleteMultipartUpload(sessionID); err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify integrity.
	got, err := env.storage.Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(got) != int(totalSize) {
		t.Fatalf("expected size %d, got %d", totalSize, len(got))
	}
	gotHash := fmt.Sprintf("%x", sha256.Sum256(got))
	if gotHash != expectedHash {
		t.Fatalf("hash mismatch: expected %s, got %s", expectedHash, gotHash)
	}
}

// TestStress_SessionCleanup verifies that CleanupExpiredSessions removes
// sessions older than the max age while preserving recent sessions.
func TestStress_SessionCleanup(t *testing.T) {
	t.Parallel()
	env := setupPerfEnv(t)

	const totalSessions = 5
	const expiredCount = 3

	sessionIDs := make([]string, totalSessions)
	for i := 0; i < totalSessions; i++ {
		path := fmt.Sprintf("cleanup_%d.bin", i)
		sessionID, err := env.transferSvc.CreateUploadSession(path, path, 1024, "cleanup_client", "")
		if err != nil {
			t.Fatalf("CreateUploadSession %d failed: %v", i, err)
		}
		sessionIDs[i] = sessionID
	}

	// Mark the first `expiredCount` sessions as 2 hours old.
	oldTimestamp := time.Now().Add(-2 * time.Hour).UTC().Format("2006-01-02T15:04:05Z")
	for i := 0; i < expiredCount; i++ {
		sess, err := env.transferSvc.GetUploadSession(sessionIDs[i])
		if err != nil {
			t.Fatalf("GetUploadSession %d failed: %v", i, err)
		}
		sess.CreatedAt = oldTimestamp
	}

	// Run cleanup with a 1-hour max age window.
	env.transferSvc.CleanupExpiredSessions(3600)

	// Expired sessions should have been removed.
	for i := 0; i < expiredCount; i++ {
		if _, err := env.transferSvc.GetUploadSession(sessionIDs[i]); err == nil {
			t.Errorf("expired session %d should have been cleaned up", i)
		}
	}

	// Recent sessions should still be present.
	for i := expiredCount; i < totalSessions; i++ {
		if _, err := env.transferSvc.GetUploadSession(sessionIDs[i]); err != nil {
			t.Errorf("recent session %d should still exist: %v", i, err)
		}
	}
}
