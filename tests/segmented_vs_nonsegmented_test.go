package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	httpserver "github.com/sosoxu/fssvrgo/internal/api/http"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type CompareTestEnv struct {
	BaseURL     string
	StorageDir  string
	TempDir     string
	DB          *database.DB
	Store       storage.StorageAdapter
	FM          *filemanager.FileManager
	TransferSvc *transfer.FileTransferService
	dbObj       *database.Database
	server      *httpserver.Server
}

type CompareResult struct {
	Mode       string
	Protocol   string
	Operation  string
	FileSize   string
	FileSizeB  int64
	Concurrency int
	Duration   time.Duration
	Throughput float64
	Error      string
}

func setupCompareEnv(t *testing.T) *CompareTestEnv {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "fsserver-compare-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	storageDir := filepath.Join(tempDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create storage dir: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")
	dbCfg := config.DatabaseConfig{Type: "sqlite", Path: dbPath}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		os.RemoveAll(tempDir)
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
		os.RemoveAll(tempDir)
		t.Fatalf("failed to run migrations: %v", err)
	}

	store := storage.NewLocalStorage(storageDir)
	fm := filemanager.NewFileManager(store, qdb)
	dirSvc := directory.NewDirectoryManager(qdb)
	flSvc := filelist.NewFileListService(qdb)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferService(store, qdb)

	serverCfg := config.ServerConfig{
		HTTPPort:           0,
		MaxUploadSizeMB:    4096,
		MaxChunkSizeMB:     256,
		MaxPageSize:        1000,
		CORSAllowedOrigins: "*",
	}

	srv := httpserver.NewServer(serverCfg, config.TLSConfig{}, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, store, cacheSvc, nil, qdb)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("failed to listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	go func() {
		srv.Serve(ln)
	}()

	baseURL := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(baseURL + "/api/v1/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		transferSvc.StopCleanupThread()
		dbObj.Close()
		os.RemoveAll(tempDir)
	})

	return &CompareTestEnv{
		BaseURL:     baseURL,
		StorageDir:  storageDir,
		TempDir:     tempDir,
		DB:          qdb,
		Store:       store,
		FM:          fm,
		TransferSvc: transferSvc,
		dbObj:       dbObj,
		server:      srv,
	}
}

func generateCompareData(size int64) []byte {
	data := make([]byte, size)
	pattern := []byte("CompareSegmentTest2026!")
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}
	return data
}

func httpSequentialUpload(baseURL, filePath, fileName string, data []byte, chunkSize int) error {
	totalSize := int64(len(data))
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessReq := map[string]interface{}{
		"file_path":  filePath,
		"file_name":  fileName,
		"total_size": totalSize,
		"hash":       hash,
	}
	sessBody, _ := json.Marshal(sessReq)
	resp, err := http.Post(baseURL+"/api/v1/uploads", "application/json", bytes.NewReader(sessBody))
	if err != nil {
		return err
	}
	var sessResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&sessResp)
	resp.Body.Close()

	sessionID, ok := sessResp["session_id"].(string)
	if !ok || sessionID == "" {
		return fmt.Errorf("failed to create upload session: %v", sessResp)
	}

	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]

		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)
		part, _ := writer.CreateFormFile("data", "chunk")
		part.Write(chunk)
		writer.WriteField("offset", fmt.Sprintf("%d", offset))
		writer.Close()

		req, _ := http.NewRequest("PUT",
			fmt.Sprintf("%s/api/v1/uploads/%s/chunk", baseURL, sessionID),
			&buf)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		chunkResp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		chunkResp.Body.Close()
		if chunkResp.StatusCode != http.StatusOK {
			return fmt.Errorf("chunk upload failed with status %d", chunkResp.StatusCode)
		}
	}

	completeResp, err := http.Post(baseURL+fmt.Sprintf("/api/v1/uploads/%s/complete", sessionID), "application/json", nil)
	if err != nil {
		return err
	}
	completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		return fmt.Errorf("complete upload failed with status %d", completeResp.StatusCode)
	}
	return nil
}

func httpMultipartUpload(baseURL, filePath, fileName string, data []byte, partSize int64, concurrency int) error {
	totalSize := int64(len(data))
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessReq := map[string]interface{}{
		"file_path":  filePath,
		"file_name":  fileName,
		"total_size": totalSize,
		"hash":       hash,
	}
	sessBody, _ := json.Marshal(sessReq)
	resp, err := http.Post(baseURL+"/api/v1/multipart-uploads", "application/json", bytes.NewReader(sessBody))
	if err != nil {
		return err
	}
	var sessResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&sessResp)
	resp.Body.Close()

	sessionID, ok := sessResp["session_id"].(string)
	if !ok || sessionID == "" {
		return fmt.Errorf("failed to create multipart upload session: %v", sessResp)
	}

	partCount := int(totalSize / partSize)
	if totalSize%partSize != 0 {
		partCount++
	}

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
			if end > totalSize {
				end = totalSize
			}
			chunk := data[offset:end]

			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)
			part, _ := writer.CreateFormFile("data", "part")
			part.Write(chunk)
			writer.WriteField("offset", fmt.Sprintf("%d", offset))
			writer.Close()

			url := fmt.Sprintf("%s/api/v1/multipart-uploads/%s/parts/%d", baseURL, sessionID, partNum+1)
			req, _ := http.NewRequest("PUT", url, &buf)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			partResp, err := http.DefaultClient.Do(req)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			partResp.Body.Close()
			if partResp.StatusCode != http.StatusOK {
				select {
				case errCh <- fmt.Errorf("part %d upload failed with status %d", partNum+1, partResp.StatusCode):
				default:
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}

	completeResp, err := http.Post(baseURL+fmt.Sprintf("/api/v1/multipart-uploads/%s/complete", sessionID), "application/json", nil)
	if err != nil {
		return err
	}
	completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		return fmt.Errorf("complete multipart upload failed with status %d", completeResp.StatusCode)
	}
	return nil
}

func httpSequentialDownload(baseURL, filePath string, chunkSize int) ([]byte, error) {
	var result []byte
	offset := 0

	for {
		end := offset + chunkSize - 1
		req, _ := http.NewRequest("GET", baseURL+"/api/v1/files"+filePath, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		chunk, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable || len(chunk) == 0 {
			break
		}

		result = append(result, chunk...)
		offset += len(chunk)

		if resp.StatusCode == http.StatusOK {
			break
		}
	}

	return result, nil
}

func httpParallelDownload(baseURL, filePath string, totalSize int64, concurrency int) ([]byte, error) {
	segmentSize := totalSize / int64(concurrency)
	type segResult struct {
		index int
		data  []byte
		err   error
	}

	results := make([]segResult, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			offset := int64(idx) * segmentSize
			end := offset + segmentSize - 1
			if idx == concurrency-1 {
				end = totalSize - 1
			}

			req, _ := http.NewRequest("GET", baseURL+"/api/v1/files"+filePath, nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results[idx] = segResult{index: idx, err: err}
				return
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			results[idx] = segResult{index: idx, data: data}
		}(i)
	}

	wg.Wait()

	var totalData []byte
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		totalData = append(totalData, r.data...)
	}
	return totalData, nil
}

func grpcSequentialUpload(svc *transfer.FileTransferService, filePath string, data []byte, chunkSize int) error {
	totalSize := int64(len(data))
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, err := svc.CreateUploadSession(filePath, filepath.Base(filePath), totalSize, "perf", hash)
	if err != nil {
		return err
	}

	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := svc.UploadChunk(sessionID, data[offset:end], int64(offset)); err != nil {
			return err
		}
	}

	return svc.CompleteUpload(sessionID)
}

func grpcMultipartUpload(svc *transfer.FileTransferService, filePath string, data []byte, concurrency int) error {
	totalSize := int64(len(data))
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, partSize, err := svc.CreateMultipartUpload(filePath, filepath.Base(filePath), totalSize, "perf", hash)
	if err != nil {
		return err
	}

	partCount := int(totalSize / partSize)
	if totalSize%partSize != 0 {
		partCount++
	}

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
			if end > totalSize {
				end = totalSize
			}
			chunk := data[offset:end]
			if err := svc.UploadPartData(sessionID, partNum+1, offset, chunk); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}

	return svc.CompleteMultipartUpload(sessionID)
}

func grpcSequentialDownload(svc *transfer.FileTransferService, filePath string, totalSize int64, chunkSize int) ([]byte, error) {
	sessionID, err := svc.CreateDownloadSession(filePath, "perf")
	if err != nil {
		return nil, err
	}

	var result []byte
	var offset int64
	for offset < totalSize {
		sz := int64(chunkSize)
		if totalSize-offset < sz {
			sz = totalSize - offset
		}
		chunk, err := svc.DownloadChunk(sessionID, int(sz), offset)
		if err != nil {
			return nil, err
		}
		result = append(result, chunk...)
		offset += int64(len(chunk))
	}

	svc.CompleteDownload(sessionID)
	return result, nil
}

func grpcParallelDownload(svc *transfer.FileTransferService, filePath string, totalSize int64, concurrency int) ([]byte, error) {
	sessionID, err := svc.CreateDownloadSession(filePath, "perf")
	if err != nil {
		return nil, err
	}

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

	results := svc.ParallelDownloadChunks(sessionID, segments)

	var totalData []byte
	for _, r := range results {
		if r.Error != nil {
			return nil, r.Error
		}
		totalData = append(totalData, r.Data...)
	}

	svc.CompleteDownload(sessionID)
	return totalData, nil
}

func cleanCompareFiles(db *database.DB) {
	db.Exec("DELETE FROM transfer_tasks")
	db.Exec("DELETE FROM audit_log")
	db.Exec("DELETE FROM files")
	db.Exec("DELETE FROM directories")
}

func TestSegmentedVsNonSegmented_Comparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comparison test in short mode")
	}

	env := setupCompareEnv(t)

	fileSizes := []int64{
		10 * 1024 * 1024,
		50 * 1024 * 1024,
		100 * 1024 * 1024,
	}
	concurrencyLevels := []int{1, 4, 8}
	chunkSize := 4 * 1024 * 1024
	iterations := 2

	var results []CompareResult

	for _, fileSize := range fileSizes {
		sizeLabel := formatSize(fileSize)
		data := generateCompareData(fileSize)
		t.Logf("\n========== File Size: %s ==========", sizeLabel)

		for _, concurrency := range concurrencyLevels {
			t.Logf("\n--- Concurrency: %d ---", concurrency)

			var durations []time.Duration
			for i := 0; i < iterations; i++ {
				path := fmt.Sprintf("/compare/http/seq_upload/%s/c%d/%d", sizeLabel, concurrency, i)
				start := time.Now()
				err := httpSequentialUpload(env.BaseURL, path, fmt.Sprintf("test_%s.bin", sizeLabel), data, chunkSize)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Sequential", Protocol: "HTTP", Operation: "Upload",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1, Error: err.Error(),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Sequential", Protocol: "HTTP", Operation: "Upload",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  HTTP Sequential Upload:     %v (%.2f MB/s)", avgDur, tp)
			}
			durations = nil
			cleanCompareFiles(env.DB)

			for i := 0; i < iterations; i++ {
				path := fmt.Sprintf("/compare/http/mp_upload/%s/c%d/%d", sizeLabel, concurrency, i)
				start := time.Now()
				err := httpMultipartUpload(env.BaseURL, path, fmt.Sprintf("test_%s.bin", sizeLabel), data, int64(chunkSize), concurrency)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Segmented", Protocol: "HTTP", Operation: "Upload",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency, Error: err.Error(),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Segmented", Protocol: "HTTP", Operation: "Upload",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  HTTP Segmented Upload (c=%d): %v (%.2f MB/s)", concurrency, avgDur, tp)
			}
			durations = nil
			cleanCompareFiles(env.DB)

			for i := 0; i < iterations; i++ {
				path := fmt.Sprintf("/compare/grpc/seq_upload/%s/c%d/%d", sizeLabel, concurrency, i)
				start := time.Now()
				err := grpcSequentialUpload(env.TransferSvc, path, data, chunkSize)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Sequential", Protocol: "gRPC", Operation: "Upload",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1, Error: err.Error(),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Sequential", Protocol: "gRPC", Operation: "Upload",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  gRPC Sequential Upload:     %v (%.2f MB/s)", avgDur, tp)
			}
			durations = nil
			cleanCompareFiles(env.DB)

			for i := 0; i < iterations; i++ {
				path := fmt.Sprintf("/compare/grpc/mp_upload/%s/c%d/%d", sizeLabel, concurrency, i)
				start := time.Now()
				err := grpcMultipartUpload(env.TransferSvc, path, data, concurrency)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Segmented", Protocol: "gRPC", Operation: "Upload",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency, Error: err.Error(),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Segmented", Protocol: "gRPC", Operation: "Upload",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  gRPC Segmented Upload (c=%d): %v (%.2f MB/s)", concurrency, avgDur, tp)
			}
			durations = nil
			cleanCompareFiles(env.DB)
		}

		uploadPath := fmt.Sprintf("/compare/dl_setup/%s/file.bin", sizeLabel)
		sessionID, _ := env.TransferSvc.CreateUploadSession(uploadPath, fmt.Sprintf("test_%s.bin", sizeLabel), fileSize, "perf", "")
		for offset := 0; offset < len(data); offset += chunkSize {
			end := offset + chunkSize
			if end > len(data) {
				end = len(data)
			}
			env.TransferSvc.UploadChunk(sessionID, data[offset:end], int64(offset))
		}
		env.TransferSvc.CompleteUpload(sessionID)

		for _, concurrency := range concurrencyLevels {
			var durations []time.Duration

			for i := 0; i < iterations; i++ {
				start := time.Now()
				downloaded, err := httpSequentialDownload(env.BaseURL, uploadPath, chunkSize)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Sequential", Protocol: "HTTP", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1, Error: err.Error(),
					})
					continue
				}
				if len(downloaded) != int(fileSize) {
					results = append(results, CompareResult{
						Mode: "Sequential", Protocol: "HTTP", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1,
						Error: fmt.Sprintf("size mismatch: got %d", len(downloaded)),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Sequential", Protocol: "HTTP", Operation: "Download",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  HTTP Sequential Download:     %v (%.2f MB/s)", avgDur, tp)
			}
			durations = nil

			for i := 0; i < iterations; i++ {
				start := time.Now()
				downloaded, err := httpParallelDownload(env.BaseURL, uploadPath, fileSize, concurrency)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Segmented", Protocol: "HTTP", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency, Error: err.Error(),
					})
					continue
				}
				if len(downloaded) != int(fileSize) {
					results = append(results, CompareResult{
						Mode: "Segmented", Protocol: "HTTP", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency,
						Error: fmt.Sprintf("size mismatch: got %d", len(downloaded)),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Segmented", Protocol: "HTTP", Operation: "Download",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  HTTP Segmented Download (c=%d): %v (%.2f MB/s)", concurrency, avgDur, tp)
			}
			durations = nil

			for i := 0; i < iterations; i++ {
				start := time.Now()
				downloaded, err := grpcSequentialDownload(env.TransferSvc, uploadPath, fileSize, chunkSize)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Sequential", Protocol: "gRPC", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1, Error: err.Error(),
					})
					continue
				}
				if len(downloaded) != int(fileSize) {
					results = append(results, CompareResult{
						Mode: "Sequential", Protocol: "gRPC", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1,
						Error: fmt.Sprintf("size mismatch: got %d", len(downloaded)),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Sequential", Protocol: "gRPC", Operation: "Download",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: 1,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  gRPC Sequential Download:     %v (%.2f MB/s)", avgDur, tp)
			}
			durations = nil

			for i := 0; i < iterations; i++ {
				start := time.Now()
				downloaded, err := grpcParallelDownload(env.TransferSvc, uploadPath, fileSize, concurrency)
				dur := time.Since(start)
				if err != nil {
					results = append(results, CompareResult{
						Mode: "Segmented", Protocol: "gRPC", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency, Error: err.Error(),
					})
					continue
				}
				if len(downloaded) != int(fileSize) {
					results = append(results, CompareResult{
						Mode: "Segmented", Protocol: "gRPC", Operation: "Download",
						FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency,
						Error: fmt.Sprintf("size mismatch: got %d", len(downloaded)),
					})
					continue
				}
				durations = append(durations, dur)
			}
			if len(durations) > 0 {
				avgDur := avgDuration(durations)
				tp := float64(fileSize) / avgDur.Seconds() / (1024 * 1024)
				results = append(results, CompareResult{
					Mode: "Segmented", Protocol: "gRPC", Operation: "Download",
					FileSize: sizeLabel, FileSizeB: fileSize, Concurrency: concurrency,
					Duration: avgDur, Throughput: tp,
				})
				t.Logf("  gRPC Segmented Download (c=%d): %v (%.2f MB/s)", concurrency, avgDur, tp)
			}
			durations = nil
		}

		cleanCompareFiles(env.DB)
	}

	t.Log("\n\n" + strings.Repeat("=", 120))
	t.Log("  SEGMENTED vs NON-SEGMENTED TRANSFER COMPARISON (HTTP & gRPC)")
	t.Log(strings.Repeat("=", 120))

	sort.Slice(results, func(i, j int) bool {
		if results[i].FileSizeB != results[j].FileSizeB {
			return results[i].FileSizeB < results[j].FileSizeB
		}
		if results[i].Operation != results[j].Operation {
			return results[i].Operation < results[j].Operation
		}
		if results[i].Protocol != results[j].Protocol {
			return results[i].Protocol < results[j].Protocol
		}
		return results[i].Mode < results[j].Mode
	})

	t.Logf("\n%-12s %-8s %-10s %-12s %-4s %-12s %-14s %s",
		"FileSize", "Protocol", "Operation", "Mode", "Conc", "Avg Time", "Throughput", "Error")
	t.Log(strings.Repeat("-", 120))

	for _, r := range results {
		errStr := ""
		if r.Error != "" {
			errStr = r.Error
		}
		t.Logf("%-12s %-8s %-10s %-12s %-4d %-12v %-14.2f MB/s %s",
			r.FileSize, r.Protocol, r.Operation, r.Mode, r.Concurrency,
			r.Duration.Round(time.Microsecond), r.Throughput, errStr)
	}

	t.Log(strings.Repeat("-", 120))

	t.Log("\n\n  SEGMENTED vs SEQUENTIAL SPEEDUP ANALYSIS")
	t.Log(strings.Repeat("=", 100))

	for _, fileSize := range fileSizes {
		sizeLabel := formatSize(fileSize)
		t.Logf("\n--- %s ---", sizeLabel)

		for _, protocol := range []string{"HTTP", "gRPC"} {
			for _, operation := range []string{"Upload", "Download"} {
				for _, concurrency := range concurrencyLevels {
					var seqResult, segResult *CompareResult
					for _, r := range results {
						if r.FileSizeB == fileSize && r.Protocol == protocol &&
							r.Operation == operation && r.Concurrency == concurrency && r.Error == "" {
							if r.Mode == "Sequential" && seqResult == nil {
								seqResult = &r
							}
							if r.Mode == "Segmented" && segResult == nil {
								segResult = &r
							}
						}
					}

					if seqResult != nil && segResult != nil {
						speedup := segResult.Throughput / seqResult.Throughput
						faster := "FASTER"
						if speedup < 1.0 {
							faster = "SLOWER"
						}
						t.Logf("  %s %s (c=%d): Seq=%.2f MB/s, Seg=%.2f MB/s, Seg %.2fx %s",
							protocol, operation, concurrency,
							seqResult.Throughput, segResult.Throughput,
							speedup, faster)
					}
				}
			}
		}
	}

	t.Log("\n\n  HTTP vs gRPC COMPARISON (by mode)")
	t.Log(strings.Repeat("=", 100))

	for _, fileSize := range fileSizes {
		sizeLabel := formatSize(fileSize)
		t.Logf("\n--- %s ---", sizeLabel)

		for _, operation := range []string{"Upload", "Download"} {
			for _, mode := range []string{"Sequential", "Segmented"} {
				for _, concurrency := range concurrencyLevels {
					var httpResult, grpcResult *CompareResult
					for _, r := range results {
						if r.FileSizeB == fileSize && r.Operation == operation &&
							r.Mode == mode && r.Concurrency == concurrency && r.Error == "" {
							if r.Protocol == "HTTP" && httpResult == nil {
								httpResult = &r
							}
							if r.Protocol == "gRPC" && grpcResult == nil {
								grpcResult = &r
							}
						}
					}

					if httpResult != nil && grpcResult != nil {
						ratio := grpcResult.Throughput / httpResult.Throughput
						faster := "gRPC"
						if httpResult.Throughput > grpcResult.Throughput {
							ratio = httpResult.Throughput / grpcResult.Throughput
							faster = "HTTP"
						}
						t.Logf("  %s %s (c=%d): HTTP=%.2f MB/s, gRPC=%.2f MB/s, %s %.2fx faster",
							mode, operation, concurrency,
							httpResult.Throughput, grpcResult.Throughput,
							faster, ratio)
					}
				}
			}
		}
	}
}

func BenchmarkHTTP_SequentialUpload_100MB(b *testing.B) {
	benchmarkCompareHTTPUpload(b, 100*1024*1024, false, 1)
}

func BenchmarkHTTP_SegmentedUpload_100MB_Conc4(b *testing.B) {
	benchmarkCompareHTTPUpload(b, 100*1024*1024, true, 4)
}

func BenchmarkHTTP_SegmentedUpload_100MB_Conc8(b *testing.B) {
	benchmarkCompareHTTPUpload(b, 100*1024*1024, true, 8)
}

func BenchmarkHTTP_SequentialDownload_100MB(b *testing.B) {
	benchmarkCompareHTTPDownload(b, 100*1024*1024, false, 1)
}

func BenchmarkHTTP_SegmentedDownload_100MB_Conc4(b *testing.B) {
	benchmarkCompareHTTPDownload(b, 100*1024*1024, true, 4)
}

func BenchmarkHTTP_SegmentedDownload_100MB_Conc8(b *testing.B) {
	benchmarkCompareHTTPDownload(b, 100*1024*1024, true, 8)
}

func BenchmarkGRPC_SequentialUpload_100MB(b *testing.B) {
	benchmarkCompareGRPCUpload(b, 100*1024*1024, false, 1)
}

func BenchmarkGRPC_SegmentedUpload_100MB_Conc4(b *testing.B) {
	benchmarkCompareGRPCUpload(b, 100*1024*1024, true, 4)
}

func BenchmarkGRPC_SegmentedUpload_100MB_Conc8(b *testing.B) {
	benchmarkCompareGRPCUpload(b, 100*1024*1024, true, 8)
}

func BenchmarkGRPC_SequentialDownload_100MB(b *testing.B) {
	benchmarkCompareGRPCDownload(b, 100*1024*1024, false, 1)
}

func BenchmarkGRPC_SegmentedDownload_100MB_Conc4(b *testing.B) {
	benchmarkCompareGRPCDownload(b, 100*1024*1024, true, 4)
}

func BenchmarkGRPC_SegmentedDownload_100MB_Conc8(b *testing.B) {
	benchmarkCompareGRPCDownload(b, 100*1024*1024, true, 8)
}

func BenchmarkHTTP_SequentialUpload_256MB(b *testing.B) {
	benchmarkCompareHTTPUpload(b, 256*1024*1024, false, 1)
}

func BenchmarkHTTP_SegmentedUpload_256MB_Conc4(b *testing.B) {
	benchmarkCompareHTTPUpload(b, 256*1024*1024, true, 4)
}

func BenchmarkHTTP_SegmentedUpload_256MB_Conc8(b *testing.B) {
	benchmarkCompareHTTPUpload(b, 256*1024*1024, true, 8)
}

func BenchmarkHTTP_SequentialDownload_256MB(b *testing.B) {
	benchmarkCompareHTTPDownload(b, 256*1024*1024, false, 1)
}

func BenchmarkHTTP_SegmentedDownload_256MB_Conc4(b *testing.B) {
	benchmarkCompareHTTPDownload(b, 256*1024*1024, true, 4)
}

func BenchmarkHTTP_SegmentedDownload_256MB_Conc8(b *testing.B) {
	benchmarkCompareHTTPDownload(b, 256*1024*1024, true, 8)
}

func BenchmarkGRPC_SequentialUpload_256MB(b *testing.B) {
	benchmarkCompareGRPCUpload(b, 256*1024*1024, false, 1)
}

func BenchmarkGRPC_SegmentedUpload_256MB_Conc4(b *testing.B) {
	benchmarkCompareGRPCUpload(b, 256*1024*1024, true, 4)
}

func BenchmarkGRPC_SegmentedUpload_256MB_Conc8(b *testing.B) {
	benchmarkCompareGRPCUpload(b, 256*1024*1024, true, 8)
}

func BenchmarkGRPC_SequentialDownload_256MB(b *testing.B) {
	benchmarkCompareGRPCDownload(b, 256*1024*1024, false, 1)
}

func BenchmarkGRPC_SegmentedDownload_256MB_Conc4(b *testing.B) {
	benchmarkCompareGRPCDownload(b, 256*1024*1024, true, 4)
}

func BenchmarkGRPC_SegmentedDownload_256MB_Conc8(b *testing.B) {
	benchmarkCompareGRPCDownload(b, 256*1024*1024, true, 8)
}

func createCompareBenchEnv(b *testing.B) *CompareTestEnv {
	tempDir, _ := os.MkdirTemp("", "fsserver-bench-*")
	storageDir := filepath.Join(tempDir, "storage")
	os.MkdirAll(storageDir, 0755)

	dbPath := filepath.Join(tempDir, "bench.db")
	dbCfg := config.DatabaseConfig{Type: "sqlite", Path: dbPath}
	dbObj := database.NewDatabase()
	dbObj.Connect(dbCfg)
	qdb := dbObj.GetQueryDB()
	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1, Name: "initial_schema",
		Up: func() error { return database.InitTables(qdb) },
	})
	migrationMgr.RunMigrations()

	store := storage.NewLocalStorage(storageDir)
	fm := filemanager.NewFileManager(store, qdb)
	dirSvc := directory.NewDirectoryManager(qdb)
	flSvc := filelist.NewFileListService(qdb)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferService(store, qdb)

	serverCfg := config.ServerConfig{
		HTTPPort: 0, MaxUploadSizeMB: 4096, MaxChunkSizeMB: 256,
		MaxPageSize: 1000, CORSAllowedOrigins: "*",
	}
	srv := httpserver.NewServer(serverCfg, config.TLSConfig{}, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, store, cacheSvc, nil, qdb)
	ln, _ := net.Listen("tcp", ":0")
	port := ln.Addr().(*net.TCPAddr).Port

	go func() { srv.Serve(ln) }()

	baseURL := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(baseURL + "/api/v1/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		transferSvc.StopCleanupThread()
		dbObj.Close()
		os.RemoveAll(tempDir)
	})

	return &CompareTestEnv{
		BaseURL: baseURL, StorageDir: storageDir, TempDir: tempDir,
		DB: qdb, Store: store, FM: fm, TransferSvc: transferSvc,
		dbObj: dbObj, server: srv,
	}
}

func benchmarkCompareHTTPUpload(b *testing.B, fileSize int64, segmented bool, concurrency int) {
	env := createCompareBenchEnv(b)
	data := generateCompareData(fileSize)
	chunkSize := 4 * 1024 * 1024

	b.ResetTimer()
	b.SetBytes(fileSize)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/bench/http/upload/%d/%d", fileSize, i)
		var err error
		if segmented {
			err = httpMultipartUpload(env.BaseURL, path, "bench.bin", data, int64(chunkSize), concurrency)
		} else {
			err = httpSequentialUpload(env.BaseURL, path, "bench.bin", data, chunkSize)
		}
		if err != nil {
			b.Fatalf("upload failed: %v", err)
		}
	}
	b.StopTimer()
	cleanCompareFiles(env.DB)
}

func benchmarkCompareHTTPDownload(b *testing.B, fileSize int64, segmented bool, concurrency int) {
	env := createCompareBenchEnv(b)
	data := generateCompareData(fileSize)
	chunkSize := 4 * 1024 * 1024

	uploadPath := fmt.Sprintf("/bench/http/dl_setup/%d/file.bin", fileSize)
	sessionID, _ := env.TransferSvc.CreateUploadSession(uploadPath, "bench.bin", fileSize, "bench", "")
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		env.TransferSvc.UploadChunk(sessionID, data[offset:end], int64(offset))
	}
	env.TransferSvc.CompleteUpload(sessionID)

	b.ResetTimer()
	b.SetBytes(fileSize)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var downloaded []byte
		var err error
		if segmented {
			downloaded, err = httpParallelDownload(env.BaseURL, uploadPath, fileSize, concurrency)
		} else {
			downloaded, err = httpSequentialDownload(env.BaseURL, uploadPath, chunkSize)
		}
		if err != nil {
			b.Fatalf("download failed: %v", err)
		}
		if len(downloaded) != int(fileSize) {
			b.Fatalf("size mismatch: expected %d, got %d", fileSize, len(downloaded))
		}
	}
	b.StopTimer()
}

func benchmarkCompareGRPCUpload(b *testing.B, fileSize int64, segmented bool, concurrency int) {
	env := createCompareBenchEnv(b)
	data := generateCompareData(fileSize)
	chunkSize := 4 * 1024 * 1024

	b.ResetTimer()
	b.SetBytes(fileSize)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/bench/grpc/upload/%d/%d", fileSize, i)
		var err error
		if segmented {
			err = grpcMultipartUpload(env.TransferSvc, path, data, concurrency)
		} else {
			err = grpcSequentialUpload(env.TransferSvc, path, data, chunkSize)
		}
		if err != nil {
			b.Fatalf("upload failed: %v", err)
		}
	}
	b.StopTimer()
	cleanCompareFiles(env.DB)
}

func benchmarkCompareGRPCDownload(b *testing.B, fileSize int64, segmented bool, concurrency int) {
	env := createCompareBenchEnv(b)
	data := generateCompareData(fileSize)
	chunkSize := 4 * 1024 * 1024

	uploadPath := fmt.Sprintf("/bench/grpc/dl_setup/%d/file.bin", fileSize)
	sessionID, _ := env.TransferSvc.CreateUploadSession(uploadPath, "bench.bin", fileSize, "bench", "")
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		env.TransferSvc.UploadChunk(sessionID, data[offset:end], int64(offset))
	}
	env.TransferSvc.CompleteUpload(sessionID)

	b.ResetTimer()
	b.SetBytes(fileSize)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var downloaded []byte
		var err error
		if segmented {
			downloaded, err = grpcParallelDownload(env.TransferSvc, uploadPath, fileSize, concurrency)
		} else {
			downloaded, err = grpcSequentialDownload(env.TransferSvc, uploadPath, fileSize, chunkSize)
		}
		if err != nil {
			b.Fatalf("download failed: %v", err)
		}
		if len(downloaded) != int(fileSize) {
			b.Fatalf("size mismatch: expected %d, got %d", fileSize, len(downloaded))
		}
	}
	b.StopTimer()
}
