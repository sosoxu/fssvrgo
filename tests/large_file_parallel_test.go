package tests

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type ParallelTestEnv struct {
	TransferSvc *transfer.FileTransferService
	Storage     *storage.LocalStorage
	DB          *database.DB
	StorageDir  string
	dbObj       *database.Database
}

func setupParallelTestEnv(t *testing.T) *ParallelTestEnv {
	t.Helper()

	storageDir, err := os.MkdirTemp("", "parallel-test-*")
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
	svc := transfer.NewFileTransferService(ls, qdb)

	t.Cleanup(func() {
		svc.StopCleanupThread()
		dbObj.Close()
		os.RemoveAll(storageDir)
	})

	return &ParallelTestEnv{
		TransferSvc: svc,
		Storage:     ls,
		DB:          qdb,
		StorageDir:  storageDir,
		dbObj:       dbObj,
	}
}

func generateTestData(size int64) []byte {
	data := make([]byte, size)
	pattern := []byte("ParallelReadWriteTest2026!")
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}
	return data
}

func TestMultipartUpload_Basic(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	data := generateTestData(totalSize)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, partSize, err := env.TransferSvc.CreateMultipartUpload("multipart_basic.bin", "multipart_basic.bin", totalSize, "test_client", expectedHash)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	if partSize <= 0 {
		t.Errorf("expected positive part size, got %d", partSize)
	}

	partCount := int(totalSize / partSize)
	if totalSize%partSize != 0 {
		partCount++
	}

	for i := 0; i < partCount; i++ {
		offset := int64(i) * partSize
		end := offset + partSize
		if end > totalSize {
			end = totalSize
		}
		chunk := data[offset:end]
		if err := env.TransferSvc.UploadPartData(sessionID, i+1, offset, chunk); err != nil {
			t.Fatalf("UploadPartData part %d failed: %v", i+1, err)
		}
	}

	if err := env.TransferSvc.CompleteMultipartUpload(sessionID); err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	result, err := env.Storage.Read("multipart_basic.bin")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(result) != int(totalSize) {
		t.Errorf("expected size %d, got %d", totalSize, len(result))
	}
}

func TestMultipartUpload_ConcurrentParts(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(50 * 1024 * 1024)
	data := generateTestData(totalSize)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, partSize, err := env.TransferSvc.CreateMultipartUpload("multipart_concurrent.bin", "multipart_concurrent.bin", totalSize, "test_client", expectedHash)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	partCount := int(totalSize / partSize)
	if totalSize%partSize != 0 {
		partCount++
	}

	var wg sync.WaitGroup
	errCh := make(chan error, partCount)

	for i := 0; i < partCount; i++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			offset := int64(partNum) * partSize
			end := offset + partSize
			if end > totalSize {
				end = totalSize
			}
			chunk := data[offset:end]
			if err := env.TransferSvc.UploadPartData(sessionID, partNum+1, offset, chunk); err != nil {
				errCh <- fmt.Errorf("part %d: %w", partNum+1, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent upload error: %v", err)
	}

	if err := env.TransferSvc.CompleteMultipartUpload(sessionID); err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	result, err := env.Storage.Read("multipart_concurrent.bin")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(result) != int(totalSize) {
		t.Errorf("expected size %d, got %d", totalSize, len(result))
	}

	for i := range data {
		if result[i] != data[i] {
			t.Errorf("data mismatch at byte %d", i)
			break
		}
	}
}

func TestMultipartUpload_Abort(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	sessionID, _, err := env.TransferSvc.CreateMultipartUpload("multipart_abort.bin", "multipart_abort.bin", totalSize, "test_client", "")
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	if err := env.TransferSvc.AbortMultipartUpload(sessionID); err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	_, err = env.TransferSvc.GetMultipartUploadSession(sessionID)
	if err == nil {
		t.Errorf("expected error getting aborted session, got nil")
	}
}

func TestMultipartUpload_Progress(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	data := generateTestData(totalSize)

	sessionID, partSize, err := env.TransferSvc.CreateMultipartUpload("multipart_progress.bin", "multipart_progress.bin", totalSize, "test_client", "")
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploaded, total, completedParts := env.TransferSvc.GetMultipartUploadProgress(sessionID)
	if uploaded != 0 || total != totalSize || completedParts != 0 {
		t.Errorf("expected initial progress (0, %d, 0), got (%d, %d, %d)", totalSize, uploaded, total, completedParts)
	}

	chunk := data[:partSize]
	if err := env.TransferSvc.UploadPartData(sessionID, 1, 0, chunk); err != nil {
		t.Fatalf("UploadPartData failed: %v", err)
	}

	uploaded, total, completedParts = env.TransferSvc.GetMultipartUploadProgress(sessionID)
	if uploaded != int64(len(chunk)) {
		t.Errorf("expected uploaded %d, got %d", len(chunk), uploaded)
	}
	if completedParts != 1 {
		t.Errorf("expected 1 completed part, got %d", completedParts)
	}
}

func TestParallelDownload_Basic(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	data := generateTestData(totalSize)

	if err := env.Storage.Write("parallel_dl.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:          utils.GenerateUUID(),
		Path:        "parallel_dl.bin",
		Name:        "parallel_dl.bin",
		Size:        totalSize,
		Hash:        fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType: "local",
		CreatedAt:   now,
		UpdatedAt:   now,
		IsDeleted:   false,
	}
	if err := database.NewFileMetadataService(env.DB).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := env.TransferSvc.CreateDownloadSession("parallel_dl.bin", "test_client")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	concurrency := 4
	segmentSize := totalSize / int64(concurrency)
	segments := make([]transfer.DownloadSegment, concurrency)
	for i := 0; i < concurrency; i++ {
		offset := int64(i) * segmentSize
		size := int(segmentSize)
		if i == concurrency-1 {
			size = int(totalSize - offset)
		}
		segments[i] = transfer.DownloadSegment{Offset: offset, Size: size}
	}

	results := env.TransferSvc.ParallelDownloadChunks(sessionID, segments)

	var totalDownloaded int64
	for i, result := range results {
		if result.Error != nil {
			t.Errorf("segment %d error: %v", i, result.Error)
			continue
		}
		totalDownloaded += int64(len(result.Data))

		expectedOffset := int64(i) * segmentSize
		for j, b := range result.Data {
			if b != data[expectedOffset+int64(j)] {
				t.Errorf("data mismatch at segment %d byte %d", i, j)
				break
			}
		}
	}

	if totalDownloaded != totalSize {
		t.Errorf("expected total downloaded %d, got %d", totalSize, totalDownloaded)
	}
}

func TestParallelDownload_HighConcurrency(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(50 * 1024 * 1024)
	data := generateTestData(totalSize)

	if err := env.Storage.Write("parallel_dl_high.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:          utils.GenerateUUID(),
		Path:        "parallel_dl_high.bin",
		Name:        "parallel_dl_high.bin",
		Size:        totalSize,
		Hash:        fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType: "local",
		CreatedAt:   now,
		UpdatedAt:   now,
		IsDeleted:   false,
	}
	if err := database.NewFileMetadataService(env.DB).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	sessionID, err := env.TransferSvc.CreateDownloadSession("parallel_dl_high.bin", "test_client")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	concurrency := 8
	segmentSize := totalSize / int64(concurrency)
	segments := make([]transfer.DownloadSegment, concurrency)
	for i := 0; i < concurrency; i++ {
		offset := int64(i) * segmentSize
		size := int(segmentSize)
		if i == concurrency-1 {
			size = int(totalSize - offset)
		}
		segments[i] = transfer.DownloadSegment{Offset: offset, Size: size}
	}

	results := env.TransferSvc.ParallelDownloadChunks(sessionID, segments)

	var totalDownloaded int64
	for i, result := range results {
		if result.Error != nil {
			t.Errorf("segment %d error: %v", i, result.Error)
			continue
		}
		totalDownloaded += int64(len(result.Data))
	}

	if totalDownloaded != totalSize {
		t.Errorf("expected total downloaded %d, got %d", totalSize, totalDownloaded)
	}
}

func TestIncrementalHash_SequentialUpload(t *testing.T) {
	env := setupParallelTestEnv(t)

	totalSize := int64(10 * 1024 * 1024)
	data := generateTestData(totalSize)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, err := env.TransferSvc.CreateUploadSession("incr_hash.bin", "incr_hash.bin", totalSize, "test_client", expectedHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	chunkSize := int64(1024 * 1024)
	var offset int64
	for offset < totalSize {
		end := offset + chunkSize
		if end > totalSize {
			end = totalSize
		}
		chunk := data[offset:end]
		if err := env.TransferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("UploadChunk at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	start := time.Now()
	if _, err := env.TransferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}
	completeDuration := time.Since(start)

	t.Logf("CompleteUpload with incremental hash took %v (no re-read needed)", completeDuration)

	if completeDuration > 100*time.Millisecond {
		t.Logf("Warning: CompleteUpload took longer than expected for incremental hash (%v), possibly fell back to full file hash", completeDuration)
	}
}

func TestLargeFileParallelPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file performance test in short mode")
	}

	env := setupParallelTestEnv(t)

	fileSizes := []int64{
		100 * 1024 * 1024,
		256 * 1024 * 1024,
	}
	concurrencyLevels := []int{1, 2, 4, 8}

	for _, fileSize := range fileSizes {
		data := generateTestData(fileSize)
		expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

		if err := env.Storage.Write(fmt.Sprintf("perf_%d.bin", fileSize), data); err != nil {
			t.Fatalf("Write failed for size %d: %v", fileSize, err)
		}

		now := utils.GetCurrentTimestamp()
		meta := &database.FileMetadata{
			ID:          utils.GenerateUUID(),
			Path:        fmt.Sprintf("perf_%d.bin", fileSize),
			Name:        fmt.Sprintf("perf_%d.bin", fileSize),
			Size:        fileSize,
			Hash:        expectedHash,
			StorageType: "local",
			CreatedAt:   now,
			UpdatedAt:   now,
			IsDeleted:   false,
		}
		if err := database.NewFileMetadataService(env.DB).Create(meta); err != nil {
			t.Fatalf("Create metadata failed: %v", err)
		}

		for _, concurrency := range concurrencyLevels {
			t.Run(fmt.Sprintf("Upload_%dMB_conc%d", fileSize/(1024*1024), concurrency), func(t *testing.T) {
				fileName := fmt.Sprintf("perf_upload_%d_c%d.bin", fileSize, concurrency)
				sessionID, partSize, err := env.TransferSvc.CreateMultipartUpload(fileName, fileName, fileSize, "perf_client", expectedHash)
				if err != nil {
					t.Fatalf("CreateMultipartUpload failed: %v", err)
				}

				partCount := int(fileSize / partSize)
				if fileSize%partSize != 0 {
					partCount++
				}

				start := time.Now()

				sem := make(chan struct{}, concurrency)
				var wg sync.WaitGroup
				errCh := make(chan error, partCount)

				for i := 0; i < partCount; i++ {
					wg.Add(1)
					sem <- struct{}{}
					go func(partNum int) {
						defer wg.Done()
						defer func() { <-sem }()

						offset := int64(partNum) * partSize
						end := offset + partSize
						if end > fileSize {
							end = fileSize
						}
						chunk := data[offset:end]
						if err := env.TransferSvc.UploadPartData(sessionID, partNum+1, offset, chunk); err != nil {
							select {
							case errCh <- fmt.Errorf("part %d: %w", partNum+1, err):
							default:
							}
						}
					}(i)
				}

				wg.Wait()
				close(errCh)

				uploadDuration := time.Since(start)

				for err := range errCh {
					t.Errorf("upload error: %v", err)
				}

				if err := env.TransferSvc.CompleteMultipartUpload(sessionID); err != nil {
					t.Fatalf("CompleteMultipartUpload failed: %v", err)
				}

				throughput := float64(fileSize) / uploadDuration.Seconds() / (1024 * 1024)
				t.Logf("Upload: %dMB, concurrency=%d, duration=%v, throughput=%.2f MB/s",
					fileSize/(1024*1024), concurrency, uploadDuration, throughput)
			})

			t.Run(fmt.Sprintf("Download_%dMB_conc%d", fileSize/(1024*1024), concurrency), func(t *testing.T) {
				dlFileName := fmt.Sprintf("perf_%d.bin", fileSize)
				sessionID, err := env.TransferSvc.CreateDownloadSession(dlFileName, "perf_client")
				if err != nil {
					t.Fatalf("CreateDownloadSession failed: %v", err)
				}

				segmentSize := fileSize / int64(concurrency)
				segments := make([]transfer.DownloadSegment, concurrency)
				for i := 0; i < concurrency; i++ {
					offset := int64(i) * segmentSize
					size := int(segmentSize)
					if i == concurrency-1 {
						size = int(fileSize - offset)
					}
					segments[i] = transfer.DownloadSegment{Offset: offset, Size: size}
				}

				start := time.Now()
				results := env.TransferSvc.ParallelDownloadChunks(sessionID, segments)
				downloadDuration := time.Since(start)

				var totalDownloaded int64
				for i, result := range results {
					if result.Error != nil {
						t.Errorf("segment %d error: %v", i, result.Error)
						continue
					}
					totalDownloaded += int64(len(result.Data))
				}

				if totalDownloaded != fileSize {
					t.Errorf("expected total downloaded %d, got %d", fileSize, totalDownloaded)
				}

				throughput := float64(fileSize) / downloadDuration.Seconds() / (1024 * 1024)
				t.Logf("Download: %dMB, concurrency=%d, duration=%v, throughput=%.2f MB/s",
					fileSize/(1024*1024), concurrency, downloadDuration, throughput)
			})
		}

		env.Storage.Remove(fmt.Sprintf("perf_%d.bin", fileSize))
	}
}

func TestSequentialVsParallel_Upload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comparison test in short mode")
	}

	env := setupParallelTestEnv(t)

	totalSize := int64(100 * 1024 * 1024)
	data := generateTestData(totalSize)
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	t.Run("Sequential", func(t *testing.T) {
		sessionID, err := env.TransferSvc.CreateUploadSession("seq_vs_par_seq.bin", "seq_vs_par_seq.bin", totalSize, "perf_client", expectedHash)
		if err != nil {
			t.Fatalf("CreateUploadSession failed: %v", err)
		}

		chunkSize := int64(4 * 1024 * 1024)
		start := time.Now()
		var offset int64
		for offset < totalSize {
			end := offset + chunkSize
			if end > totalSize {
				end = totalSize
			}
			chunk := data[offset:end]
			if err := env.TransferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
				t.Fatalf("UploadChunk failed: %v", err)
			}
			offset = end
		}
		uploadDuration := time.Since(start)

		if _, err := env.TransferSvc.CompleteUpload(sessionID); err != nil {
			t.Fatalf("CompleteUpload failed: %v", err)
		}

		throughput := float64(totalSize) / uploadDuration.Seconds() / (1024 * 1024)
		t.Logf("Sequential upload: %dMB, duration=%v, throughput=%.2f MB/s",
			totalSize/(1024*1024), uploadDuration, throughput)
	})

	t.Run("Parallel_4Parts", func(t *testing.T) {
		sessionID, partSize, err := env.TransferSvc.CreateMultipartUpload("seq_vs_par_par.bin", "seq_vs_par_par.bin", totalSize, "perf_client", expectedHash)
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		partCount := int(totalSize / partSize)
		if totalSize%partSize != 0 {
			partCount++
		}

		start := time.Now()

		sem := make(chan struct{}, 4)
		var wg sync.WaitGroup
		errCh := make(chan error, partCount)

		for i := 0; i < partCount; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(partNum int) {
				defer wg.Done()
				defer func() { <-sem }()

				offset := int64(partNum) * partSize
				end := offset + partSize
				if end > totalSize {
					end = totalSize
				}
				chunk := data[offset:end]
				if err := env.TransferSvc.UploadPartData(sessionID, partNum+1, offset, chunk); err != nil {
					select {
					case errCh <- fmt.Errorf("part %d: %w", partNum+1, err):
					default:
					}
				}
			}(i)
		}

		wg.Wait()
		close(errCh)
		uploadDuration := time.Since(start)

		for err := range errCh {
			t.Errorf("upload error: %v", err)
		}

		if err := env.TransferSvc.CompleteMultipartUpload(sessionID); err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}

		throughput := float64(totalSize) / uploadDuration.Seconds() / (1024 * 1024)
		t.Logf("Parallel upload (4 parts): %dMB, duration=%v, throughput=%.2f MB/s",
			totalSize/(1024*1024), uploadDuration, throughput)
	})
}

func TestSequentialVsParallel_Download(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comparison test in short mode")
	}

	env := setupParallelTestEnv(t)

	totalSize := int64(100 * 1024 * 1024)
	data := generateTestData(totalSize)

	if err := env.Storage.Write("seq_vs_par_dl.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:          utils.GenerateUUID(),
		Path:        "seq_vs_par_dl.bin",
		Name:        "seq_vs_par_dl.bin",
		Size:        totalSize,
		Hash:        fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType: "local",
		CreatedAt:   now,
		UpdatedAt:   now,
		IsDeleted:   false,
	}
	if err := database.NewFileMetadataService(env.DB).Create(meta); err != nil {
		t.Fatalf("Create metadata failed: %v", err)
	}

	t.Run("Sequential", func(t *testing.T) {
		sessionID, err := env.TransferSvc.CreateDownloadSession("seq_vs_par_dl.bin", "perf_client")
		if err != nil {
			t.Fatalf("CreateDownloadSession failed: %v", err)
		}

		chunkSize := int64(4 * 1024 * 1024)
		start := time.Now()
		var offset int64
		var totalDownloaded int64
		for offset < totalSize {
			remaining := totalSize - offset
			sz := chunkSize
			if remaining < sz {
				sz = remaining
			}
			chunk, err := env.TransferSvc.DownloadChunk(sessionID, int(sz), offset)
			if err != nil {
				t.Fatalf("DownloadChunk failed: %v", err)
			}
			totalDownloaded += int64(len(chunk))
			offset += int64(len(chunk))
		}
		downloadDuration := time.Since(start)

		if totalDownloaded != totalSize {
			t.Errorf("expected total downloaded %d, got %d", totalSize, totalDownloaded)
		}

		throughput := float64(totalSize) / downloadDuration.Seconds() / (1024 * 1024)
		t.Logf("Sequential download: %dMB, duration=%v, throughput=%.2f MB/s",
			totalSize/(1024*1024), downloadDuration, throughput)
	})

	t.Run("Parallel_4Segments", func(t *testing.T) {
		sessionID, err := env.TransferSvc.CreateDownloadSession("seq_vs_par_dl.bin", "perf_client")
		if err != nil {
			t.Fatalf("CreateDownloadSession failed: %v", err)
		}

		concurrency := 4
		segmentSize := totalSize / int64(concurrency)
		segments := make([]transfer.DownloadSegment, concurrency)
		for i := 0; i < concurrency; i++ {
			offset := int64(i) * segmentSize
			size := int(segmentSize)
			if i == concurrency-1 {
				size = int(totalSize - offset)
			}
			segments[i] = transfer.DownloadSegment{Offset: offset, Size: size}
		}

		start := time.Now()
		results := env.TransferSvc.ParallelDownloadChunks(sessionID, segments)
		downloadDuration := time.Since(start)

		var totalDownloaded int64
		for _, result := range results {
			if result.Error != nil {
				t.Errorf("segment error: %v", result.Error)
				continue
			}
			totalDownloaded += int64(len(result.Data))
		}

		if totalDownloaded != totalSize {
			t.Errorf("expected total downloaded %d, got %d", totalSize, totalDownloaded)
		}

		throughput := float64(totalSize) / downloadDuration.Seconds() / (1024 * 1024)
		t.Logf("Parallel download (4 segments): %dMB, duration=%v, throughput=%.2f MB/s",
			totalSize/(1024*1024), downloadDuration, throughput)
	})
}

func BenchmarkMultipartUpload_100MB_Conc1(b *testing.B) {
	benchmarkMultipartUpload(b, 100*1024*1024, 1)
}

func BenchmarkMultipartUpload_100MB_Conc4(b *testing.B) {
	benchmarkMultipartUpload(b, 100*1024*1024, 4)
}

func BenchmarkMultipartUpload_100MB_Conc8(b *testing.B) {
	benchmarkMultipartUpload(b, 100*1024*1024, 8)
}

func BenchmarkMultipartUpload_256MB_Conc1(b *testing.B) {
	benchmarkMultipartUpload(b, 256*1024*1024, 1)
}

func BenchmarkMultipartUpload_256MB_Conc4(b *testing.B) {
	benchmarkMultipartUpload(b, 256*1024*1024, 4)
}

func BenchmarkMultipartUpload_256MB_Conc8(b *testing.B) {
	benchmarkMultipartUpload(b, 256*1024*1024, 8)
}

func BenchmarkParallelDownload_100MB_Conc1(b *testing.B) {
	benchmarkParallelDownload(b, 100*1024*1024, 1)
}

func BenchmarkParallelDownload_100MB_Conc4(b *testing.B) {
	benchmarkParallelDownload(b, 100*1024*1024, 4)
}

func BenchmarkParallelDownload_100MB_Conc8(b *testing.B) {
	benchmarkParallelDownload(b, 100*1024*1024, 8)
}

func BenchmarkParallelDownload_256MB_Conc1(b *testing.B) {
	benchmarkParallelDownload(b, 256*1024*1024, 1)
}

func BenchmarkParallelDownload_256MB_Conc4(b *testing.B) {
	benchmarkParallelDownload(b, 256*1024*1024, 4)
}

func BenchmarkParallelDownload_256MB_Conc8(b *testing.B) {
	benchmarkParallelDownload(b, 256*1024*1024, 8)
}

func benchmarkMultipartUpload(b *testing.B, fileSize int64, concurrency int) {
	storageDir, err := os.MkdirTemp("", "bench-upload-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(storageDir)

	dbPath := filepath.Join(storageDir, "bench.db")
	dbCfg := config.DatabaseConfig{Type: "sqlite", Path: dbPath}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		b.Fatalf("failed to connect database: %v", err)
	}
	defer dbObj.Close()

	qdb := dbObj.GetQueryDB()
	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1,
		Name:    "initial_schema",
		Up:      func() error { return database.InitTables(qdb) },
	})
	if err := migrationMgr.RunMigrations(); err != nil {
		b.Fatalf("failed to run migrations: %v", err)
	}

	ls := storage.NewLocalStorage(storageDir)
	svc := transfer.NewFileTransferService(ls, qdb)
	defer svc.StopCleanupThread()

	data := generateTestData(fileSize)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fileName := fmt.Sprintf("bench_upload_%d.bin", i)
		sessionID, partSize, err := svc.CreateMultipartUpload(fileName, fileName, fileSize, "bench", "")
		if err != nil {
			b.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		partCount := int(fileSize / partSize)
		if fileSize%partSize != 0 {
			partCount++
		}

		sem := make(chan struct{}, concurrency)
		var wg sync.WaitGroup

		for p := 0; p < partCount; p++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(partNum int) {
				defer wg.Done()
				defer func() { <-sem }()

				offset := int64(partNum) * partSize
				end := offset + partSize
				if end > fileSize {
					end = fileSize
				}
				chunk := data[offset:end]
				svc.UploadPartData(sessionID, partNum+1, offset, chunk)
			}(p)
		}

		wg.Wait()

		if err := svc.CompleteMultipartUpload(sessionID); err != nil {
			b.Fatalf("CompleteMultipartUpload failed: %v", err)
		}
	}
}

func benchmarkParallelDownload(b *testing.B, fileSize int64, concurrency int) {
	storageDir, err := os.MkdirTemp("", "bench-download-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(storageDir)

	dbPath := filepath.Join(storageDir, "bench.db")
	dbCfg := config.DatabaseConfig{Type: "sqlite", Path: dbPath}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		b.Fatalf("failed to connect database: %v", err)
	}
	defer dbObj.Close()

	qdb := dbObj.GetQueryDB()
	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1,
		Name:    "initial_schema",
		Up:      func() error { return database.InitTables(qdb) },
	})
	if err := migrationMgr.RunMigrations(); err != nil {
		b.Fatalf("failed to run migrations: %v", err)
	}

	ls := storage.NewLocalStorage(storageDir)
	svc := transfer.NewFileTransferService(ls, qdb)
	defer svc.StopCleanupThread()

	data := generateTestData(fileSize)
	fileName := "bench_download.bin"
	if err := ls.Write(fileName, data); err != nil {
		b.Fatalf("Write failed: %v", err)
	}

	now := utils.GetCurrentTimestamp()
	meta := &database.FileMetadata{
		ID:          utils.GenerateUUID(),
		Path:        fileName,
		Name:        fileName,
		Size:        fileSize,
		Hash:        fmt.Sprintf("%x", sha256.Sum256(data)),
		StorageType: "local",
		CreatedAt:   now,
		UpdatedAt:   now,
		IsDeleted:   false,
	}
	if err := database.NewFileMetadataService(qdb).Create(meta); err != nil {
		b.Fatalf("Create metadata failed: %v", err)
	}

	segmentSize := fileSize / int64(concurrency)
	segments := make([]transfer.DownloadSegment, concurrency)
	for i := 0; i < concurrency; i++ {
		offset := int64(i) * segmentSize
		size := int(segmentSize)
		if i == concurrency-1 {
			size = int(fileSize - offset)
		}
		segments[i] = transfer.DownloadSegment{Offset: offset, Size: size}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sessionID, _ := svc.CreateDownloadSession(fileName, "bench")
		svc.ParallelDownloadChunks(sessionID, segments)
		svc.CompleteDownload(sessionID)
	}
}
