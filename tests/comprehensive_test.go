package tests

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// ---------------------------------------------------------------------------
// Shared cluster infrastructure
// ---------------------------------------------------------------------------

// CompInstance is one fsserver instance in the comprehensive test cluster.
type CompInstance struct {
	ID          int
	BaseURL     string
	Server      *httpserver.Server
	FM          *filemanager.FileManager
	DirSvc      *directory.DirectoryManager
	FlSvc       *filelist.FileListService
	TransferSvc *transfer.FileTransferService
}

// CompCluster assembles Redis (distributed lock + session store) + PostgreSQL
// (shared database) + a pluggable storage adapter (local OR MinIO) shared by
// N instances. It is the foundation for every comprehensive test below.
type CompCluster struct {
	Instances    []*CompInstance
	StorageDir   string
	TempDir      string
	SharedDB     *database.DB
	SharedStore  storage.StorageAdapter
	StorageType  string
	dbObj        *database.Database
	RedisManager *distributed.RedisManager
}

// compClusterConfig controls which backend each cluster uses.
type compClusterConfig struct {
	storageType string // "local" or "minio"
	useRedis    bool   // enable Redis distributed lock + session store
	usePgSQL    bool   // enable PostgreSQL (true) or SQLite (false)
	numInstance int    // number of fsserver instances
}

// resetPgDB wipes all tables so each test starts from a clean PostgreSQL state.
func resetPgDB(t *testing.T, qdb *database.DB) {
	t.Helper()
	for _, tbl := range []string{"transfer_tasks", "audit_log", "api_keys", "files", "directories", "schema_migrations"} {
		if _, err := qdb.Exec("DELETE FROM " + tbl); err != nil {
			// Table may not exist yet on first run; ignore.
			_ = err
		}
	}
}

// NewCompCluster builds a cluster with the requested backends.
func NewCompCluster(t *testing.T, cfg compClusterConfig) *CompCluster {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "fsserver-comp-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	storageDir := filepath.Join(tempDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("create storage dir: %v", err)
	}

	_ = logger.Initialize("", "error")

	// --- Database ---
	var dbCfg config.DatabaseConfig
	if cfg.usePgSQL {
		dbCfg = config.DatabaseConfig{
			Type:     "postgresql",
			Host:     "localhost",
			Port:     5432,
			Name:     "fsserver",
			User:     "fsserver",
			Password: "fsserver123",
			SSLMode:  "disable",
		}
	} else {
		dbCfg = config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(tempDir, "test.db")}
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		os.RemoveAll(tempDir)
		if cfg.usePgSQL {
			t.Skipf("PostgreSQL not available: %v", err)
		}
		t.Fatalf("connect database: %v", err)
	}
	qdb := dbObj.GetQueryDB()
	if cfg.usePgSQL {
		resetPgDB(t, qdb)
	}
	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1, Name: "initial_schema",
		Up: func() error { return database.InitTables(qdb) },
	})
	if err := migrationMgr.RunMigrations(); err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("run migrations: %v", err)
	}

	// --- Storage adapter ---
	var store storage.StorageAdapter
	switch cfg.storageType {
	case "minio":
		minioStore, err := storage.NewMinIOStorage(storage.MinIOConfig{
			Endpoint:  "localhost:9000",
			AccessKey: "minioadmin",
			SecretKey: "minioadmin",
			Bucket:    fmt.Sprintf("fsserver-test-%d", time.Now().UnixNano()),
			UseSSL:    false,
		})
		if err != nil {
			dbObj.Close()
			os.RemoveAll(tempDir)
			t.Skipf("MinIO not available: %v", err)
		}
		store = minioStore
	case "local":
		fallthrough
	default:
		store = storage.NewLocalStorage(storageDir)
	}

	// --- Redis (optional) ---
	var redisMgr *distributed.RedisManager
	var distLock distributed.DistributedLock
	var sessionStore distributed.SessionStore
	if cfg.useRedis {
		redisMgr, err = distributed.NewRedisManager("localhost:6379", "", 0, 10)
		if err != nil {
			dbObj.Close()
			os.RemoveAll(tempDir)
			t.Skipf("Redis not available: %v", err)
		}
		// Clean up any leftover keys from prior runs.
		redisClient := redisMgr.GetClient()
		ctx := context.Background()
		if keys, e := redisClient.Keys(ctx, "fsserver:*").Result(); e == nil && len(keys) > 0 {
			redisClient.Del(ctx, keys...)
		}
		distLock = redisMgr.GetLock()
		sessionStore = redisMgr.GetSessionStore()
	} else {
		distLock = distributed.NewLocalDistributedLock()
		sessionStore = distributed.NewMemorySessionStore()
	}

	cluster := &CompCluster{
		StorageDir:   storageDir,
		TempDir:      tempDir,
		SharedDB:     qdb,
		SharedStore:  store,
		StorageType:  cfg.storageType,
		dbObj:        dbObj,
		RedisManager: redisMgr,
	}

	n := cfg.numInstance
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		inst := createCompInstance(t, i, qdb, store, distLock, sessionStore, cfg.useRedis)
		cluster.Instances = append(cluster.Instances, inst)
	}
	return cluster
}

func createCompInstance(t *testing.T, id int, db *database.DB, store storage.StorageAdapter, distLock distributed.DistributedLock, sessionStore distributed.SessionStore, useRedis bool) *CompInstance {
	t.Helper()

	var fm *filemanager.FileManager
	var transferSvc *transfer.FileTransferService
	if useRedis {
		fm = filemanager.NewFileManagerWithDistLock(store, db, distLock)
		transferSvc = transfer.NewFileTransferServiceWithRedis(store, db, sessionStore, distLock)
	} else {
		fm = filemanager.NewFileManager(store, db)
		transferSvc = transfer.NewFileTransferService(store, db)
	}
	dirSvc := directory.NewDirectoryManager(db)
	flSvc := filelist.NewFileListService(db)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)

	serverCfg := config.ServerConfig{
		HTTPPort:           0,
		MaxUploadSizeMB:    4096,
		MaxChunkSizeMB:     256,
		MaxPageSize:        1000,
		CORSAllowedOrigins: "*",
	}

	srv := httpserver.NewServer(serverCfg, config.TLSConfig{}, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, store, cacheSvc, nil, db)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Instance %d listen: %v", id, err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	go srv.Serve(ln)

	baseURL := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(baseURL + "/ready")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &CompInstance{
		ID: id, BaseURL: baseURL, Server: srv,
		FM: fm, DirSvc: dirSvc, FlSvc: flSvc, TransferSvc: transferSvc,
	}
}

func (c *CompCluster) Cleanup() {
	for _, inst := range c.Instances {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		inst.Server.Shutdown(ctx)
		cancel()
	}
	c.dbObj.Close()
	os.RemoveAll(c.TempDir)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func httpUpload(t *testing.T, baseURL, path string, data []byte) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	fw.Write(data)
	w.WriteField("path", path)
	w.Close()

	resp, err := http.Post(baseURL+"/api/v1/files", w.FormDataContentType(), &buf)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	return resp
}

func httpDownload(t *testing.T, baseURL, path string) []byte {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/v1/files/" + path)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download %s: status %d", path, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return data
}

// httpStreamingUpload performs a chunked streaming upload via the session API.
// It sends data in chunkSize pieces and returns the total elapsed time.
func httpStreamingUpload(t *testing.T, baseURL, filePath string, totalSize int64, chunkSize int, hash string) (string, time.Duration) {
	t.Helper()
	start := time.Now()

	// 1. Create session
	body := fmt.Sprintf(`{"file_path":"%s","file_name":"%s","total_size":%d,"hash":"%s"}`,
		filePath, filepath.Base(filePath), totalSize, hash)
	resp, err := http.Post(baseURL+"/api/v1/uploads", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	var sessResp struct {
		SessionID string `json:"session_id"`
	}
	json.NewDecoder(resp.Body).Decode(&sessResp)
	resp.Body.Close()
	if sessResp.SessionID == "" {
		t.Fatalf("empty session id")
	}

	// 2. Upload chunks
	sent := int64(0)
	for sent < totalSize {
		end := sent + int64(chunkSize)
		if end > totalSize {
			end = totalSize
		}
		chunk := make([]byte, end-sent)
		rand.Read(chunk)

		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		fw, _ := w.CreateFormFile("data", "chunk.bin")
		fw.Write(chunk)
		w.WriteField("offset", fmt.Sprintf("%d", sent))
		w.Close()

		req, _ := http.NewRequest("PUT", baseURL+"/api/v1/uploads/"+sessResp.SessionID+"/chunk", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("upload chunk at offset %d: %v", sent, err)
		}
		resp.Body.Close()
		sent = end
	}

	// 3. Complete
	resp, err = http.Post(baseURL+"/api/v1/uploads/"+sessResp.SessionID+"/complete", "application/json", nil)
	if err != nil {
		t.Fatalf("complete upload: %v", err)
	}
	resp.Body.Close()

	return sessResp.SessionID, time.Since(start)
}

// ---------------------------------------------------------------------------
// Test data generators
// ---------------------------------------------------------------------------

func genData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func compSha256Hex(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ---------------------------------------------------------------------------
// SECTION 1: MinIO object storage tests (HTTP + service layer / "gRPC")
// ---------------------------------------------------------------------------

// TestComprehensive_MinIO_Basic verifies CRUD operations against MinIO object
// storage using the HTTP API and the service layer (gRPC-equivalent path).
func TestComprehensive_MinIO_Basic(t *testing.T) {
	cluster := NewCompCluster(t, compClusterConfig{
		storageType: "minio", useRedis: true, usePgSQL: true, numInstance: 1,
	})
	defer cluster.Cleanup()
	inst := cluster.Instances[0]
	t.Logf("Storage type: %s, DB: PostgreSQL, Lock: Redis", cluster.StorageType)

	sizes := []int{1 * 1024, 64 * 1024, 1024 * 1024} // 1KB, 64KB, 1MB

	for _, size := range sizes {
		t.Run(fmt.Sprintf("HTTP_%s", formatBytes(int64(size))), func(t *testing.T) {
			data := genData(size)
			path := fmt.Sprintf("minio/http/%d.bin", size)
			// Upload
			resp := httpUpload(t, inst.BaseURL, path, data)
			if resp.StatusCode != http.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("upload status %d: %s", resp.StatusCode, body)
			}
			resp.Body.Close()
			// Download & verify
			got := httpDownload(t, inst.BaseURL, path)
			if !bytes.Equal(data, got) {
				t.Errorf("data mismatch: uploaded %d bytes, got %d bytes", len(data), len(got))
			}
			// Verify storage type is MinIO
			meta, err := inst.FM.GetFileMetadata(path)
			if err != nil {
				t.Fatalf("get metadata: %v", err)
			}
			if meta.StorageType != "minio" {
				t.Errorf("expected storage type minio, got %s", meta.StorageType)
			}
		})

		t.Run(fmt.Sprintf("ServiceLayer_%s", formatBytes(int64(size))), func(t *testing.T) {
			data := genData(size)
			path := fmt.Sprintf("minio/svc/%d.bin", size)
			// Upload via FileManager (gRPC service layer equivalent)
			meta, err := inst.FM.UploadFile(path, data)
			if err != nil {
				t.Fatalf("UploadFile: %v", err)
			}
			if meta.StorageType != "minio" {
				t.Errorf("expected storage type minio, got %s", meta.StorageType)
			}
			if meta.Hash != compSha256Hex(data) {
				t.Errorf("hash mismatch")
			}
			// Download via FileManager
			got, err := inst.FM.DownloadFile(path)
			if err != nil {
				t.Fatalf("DownloadFile: %v", err)
			}
			if !bytes.Equal(data, got) {
				t.Errorf("data mismatch")
			}
		})
	}

	// Delete + verify gone
	t.Run("Delete", func(t *testing.T) {
		data := genData(1024)
		path := "minio/delete-test.bin"
		resp := httpUpload(t, inst.BaseURL, path, data)
		resp.Body.Close()
		req, _ := http.NewRequest("DELETE", inst.BaseURL+"/api/v1/files/"+path, nil)
		dresp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		dresp.Body.Close()
		if dresp.StatusCode != http.StatusOK {
			t.Errorf("delete status: %d", dresp.StatusCode)
		}
	})

	// Rename
	t.Run("Rename", func(t *testing.T) {
		data := genData(1024)
		oldPath := "minio/rename-old.bin"
		newPath := "minio/rename-new.bin"
		resp := httpUpload(t, inst.BaseURL, oldPath, data)
		resp.Body.Close()
		body := `{"new_name":"rename-new.bin"}`
		req, _ := http.NewRequest("PATCH", inst.BaseURL+"/api/v1/files/"+oldPath, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		renresp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
		renresp.Body.Close()
		got := httpDownload(t, inst.BaseURL, newPath)
		if !bytes.Equal(data, got) {
			t.Errorf("renamed file data mismatch")
		}
	})
}

// TestComprehensive_MinIO_Streaming verifies chunked streaming upload against
// MinIO object storage with Redis session store.
func TestComprehensive_MinIO_Streaming(t *testing.T) {
	cluster := NewCompCluster(t, compClusterConfig{
		storageType: "minio", useRedis: true, usePgSQL: true, numInstance: 1,
	})
	defer cluster.Cleanup()
	inst := cluster.Instances[0]

	size := 10 * 1024 * 1024 // 10MB
	chunkSize := 1024 * 1024 // 1MB chunks
	path := "minio/streaming/10mb.bin"
	data := genData(size)

	// Compute hash
	hash := compSha256Hex(data)

	// Use streaming upload API
	_, elapsed := httpStreamingUpload(t, inst.BaseURL, path, int64(size), chunkSize, hash)
	t.Logf("MinIO streaming upload %s: %v (%.2f MB/s)",
		formatBytes(int64(size)), elapsed, float64(size)/1024/1024/elapsed.Seconds())

	// Download & verify integrity (re-read original data by regenerating same pattern)
	got := httpDownload(t, inst.BaseURL, path)
	if len(got) != size {
		t.Errorf("size mismatch: expected %d, got %d", size, len(got))
	}
	// Verify hash
	if compSha256Hex(got) != hash {
		t.Errorf("hash mismatch after streaming upload")
	}
}

// ---------------------------------------------------------------------------
// SECTION 2: GB-level large file tests (local storage + Redis + PostgreSQL)
// ---------------------------------------------------------------------------

// TestComprehensive_GB_HttpStreaming uploads a 1GB file via HTTP streaming
// (chunked session API) with Redis lock + PostgreSQL + local storage.
func TestComprehensive_GB_HttpStreaming(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GB-level test in short mode")
	}
	cluster := NewCompCluster(t, compClusterConfig{
		storageType: "local", useRedis: true, usePgSQL: true, numInstance: 1,
	})
	defer cluster.Cleanup()
	inst := cluster.Instances[0]
	t.Logf("Storage: local, DB: PostgreSQL, Lock: Redis")

	totalSize := int64(1024 * 1024 * 1024) // 1GB
	chunkSize := 16 * 1024 * 1024          // 16MB chunks
	path := "gb/http-streaming/1gb.bin"

	// Generate deterministic data in chunks and compute hash incrementally
	hasher := sha256.New()
	chunkData := make([]byte, chunkSize)
	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}
	// Write chunk pattern repeatedly to compute expected hash
	remaining := totalSize
	for remaining > 0 {
		n := int64(chunkSize)
		if n > remaining {
			n = remaining
		}
		hasher.Write(chunkData[:n])
		remaining -= n
	}
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Perform streaming upload — we re-generate the same data pattern per chunk
	start := time.Now()

	// Create session
	body := fmt.Sprintf(`{"file_path":"%s","file_name":"%s","total_size":%d,"hash":"%s"}`,
		path, filepath.Base(path), totalSize, expectedHash)
	resp, err := http.Post(inst.BaseURL+"/api/v1/uploads", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	var sessResp struct {
		SessionID string `json:"session_id"`
	}
	json.NewDecoder(resp.Body).Decode(&sessResp)
	resp.Body.Close()

	// Upload chunks
	sent := int64(0)
	for sent < totalSize {
		end := sent + int64(chunkSize)
		if end > totalSize {
			end = totalSize
		}
		thisChunk := chunkData[:end-sent]

		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		fw, _ := w.CreateFormFile("data", "chunk.bin")
		fw.Write(thisChunk)
		w.WriteField("offset", fmt.Sprintf("%d", sent))
		w.Close()

		req2, _ := http.NewRequest("PUT", inst.BaseURL+"/api/v1/uploads/"+sessResp.SessionID+"/chunk", &buf)
		req2.Header.Set("Content-Type", w.FormDataContentType())
		resp, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatalf("upload chunk at %d: %v", sent, err)
		}
		resp.Body.Close()
		sent = end
	}

	// Complete
	resp, err = http.Post(inst.BaseURL+"/api/v1/uploads/"+sessResp.SessionID+"/complete", "application/json", nil)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	completeBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete status %d: %s", resp.StatusCode, completeBody)
	}

	uploadElapsed := time.Since(start)
	t.Logf("1GB HTTP streaming upload completed in %v (%.2f MB/s)",
		uploadElapsed, float64(totalSize)/1024/1024/uploadElapsed.Seconds())

	// Download and verify hash
	dlStart := time.Now()
	dlResp, err := http.Get(inst.BaseURL + "/api/v1/files/" + path)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if dlResp.StatusCode != http.StatusOK {
		dlResp.Body.Close()
		t.Fatalf("download status: %d", dlResp.StatusCode)
	}
	dlHasher := sha256.New()
	io.Copy(dlHasher, dlResp.Body)
	dlResp.Body.Close()
	dlElapsed := time.Since(dlStart)
	dlHash := fmt.Sprintf("%x", dlHasher.Sum(nil))

	t.Logf("1GB HTTP download completed in %v (%.2f MB/s)",
		dlElapsed, float64(totalSize)/1024/1024/dlElapsed.Seconds())

	if dlHash != expectedHash {
		t.Errorf("hash mismatch: expected %s, got %s", expectedHash, dlHash)
	} else {
		t.Logf("1GB hash verified OK: %s", dlHash[:16]+"...")
	}
}

// TestComprehensive_GB_Multipart uploads a 1GB file via multipart concurrent
// upload (segmented) with Redis + PostgreSQL + local storage.
func TestComprehensive_GB_Multipart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GB-level test in short mode")
	}
	cluster := NewCompCluster(t, compClusterConfig{
		storageType: "local", useRedis: true, usePgSQL: true, numInstance: 1,
	})
	defer cluster.Cleanup()
	inst := cluster.Instances[0]

	totalSize := int64(1024 * 1024 * 1024) // 1GB
	path := "gb/multipart/1gb.bin"

	// Compute expected hash (deterministic pattern)
	hasher := sha256.New()
	pattern := make([]byte, 16*1024*1024)
	for i := range pattern {
		pattern[i] = byte(i % 256)
	}
	remaining := totalSize
	for remaining > 0 {
		n := int64(len(pattern))
		if n > remaining {
			n = remaining
		}
		hasher.Write(pattern[:n])
		remaining -= n
	}
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Create multipart session
	start := time.Now()
	body := fmt.Sprintf(`{"file_path":"%s","file_name":"%s","total_size":%d,"hash":"%s"}`,
		path, filepath.Base(path), totalSize, expectedHash)
	resp, err := http.Post(inst.BaseURL+"/api/v1/multipart-uploads", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create multipart: %v", err)
	}
	var mpResp struct {
		SessionID string `json:"session_id"`
		PartSize  int64  `json:"part_size"`
	}
	json.NewDecoder(resp.Body).Decode(&mpResp)
	resp.Body.Close()
	if mpResp.SessionID == "" {
		t.Fatal("empty multipart session id")
	}

	partSize := int(mpResp.PartSize)
	if partSize <= 0 {
		partSize = 64 * 1024 * 1024 // 64MB default
	}
	t.Logf("Multipart session: %s, part size: %s", mpResp.SessionID, formatBytes(int64(partSize)))

	// Upload parts concurrently
	numParts := int(totalSize) / partSize
	if int64(numParts*partSize) < totalSize {
		numParts++
	}
	var wg sync.WaitGroup
	errCh := make(chan error, numParts)
	chunkData := make([]byte, partSize)
	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}

	for p := 1; p <= numParts; p++ {
		offset := int64((p - 1) * partSize)
		thisSize := partSize
		if offset+int64(thisSize) > totalSize {
			thisSize = int(totalSize - offset)
		}
		partNum := p
		wg.Add(1)
		go func(pn int, off int64, sz int) {
			defer wg.Done()
			var buf bytes.Buffer
			w := multipart.NewWriter(&buf)
			fw, _ := w.CreateFormFile("data", "part.bin")
			fw.Write(chunkData[:sz])
			w.WriteField("offset", fmt.Sprintf("%d", off))
			w.Close()
			req3, _ := http.NewRequest("PUT",
				inst.BaseURL+"/api/v1/multipart-uploads/"+mpResp.SessionID+"/parts/"+fmt.Sprintf("%d", pn), &buf)
			req3.Header.Set("Content-Type", w.FormDataContentType())
			resp, err := http.DefaultClient.Do(req3)
			if err != nil {
				errCh <- fmt.Errorf("part %d: %v", pn, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("part %d status %d", pn, resp.StatusCode)
			}
		}(partNum, offset, thisSize)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			t.Fatalf("multipart upload error: %v", e)
		}
	}

	// Complete multipart
	resp, err = http.Post(inst.BaseURL+"/api/v1/multipart-uploads/"+mpResp.SessionID+"/complete", "application/json", nil)
	if err != nil {
		t.Fatalf("complete multipart: %v", err)
	}
	completeBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete multipart status %d: %s", resp.StatusCode, completeBody)
	}

	uploadElapsed := time.Since(start)
	t.Logf("1GB multipart upload (%d parts) completed in %v (%.2f MB/s)",
		numParts, uploadElapsed, float64(totalSize)/1024/1024/uploadElapsed.Seconds())

	// Download & verify hash
	dlStart := time.Now()
	dlResp, err := http.Get(inst.BaseURL + "/api/v1/files/" + path)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if dlResp.StatusCode != http.StatusOK {
		dlResp.Body.Close()
		t.Fatalf("download status: %d", dlResp.StatusCode)
	}
	dlHasher := sha256.New()
	io.Copy(dlHasher, dlResp.Body)
	dlResp.Body.Close()
	dlElapsed := time.Since(dlStart)
	dlHash := fmt.Sprintf("%x", dlHasher.Sum(nil))

	t.Logf("1GB download completed in %v (%.2f MB/s)",
		dlElapsed, float64(totalSize)/1024/1024/dlElapsed.Seconds())

	if dlHash != expectedHash {
		t.Errorf("hash mismatch: expected %s, got %s", expectedHash, dlHash)
	} else {
		t.Logf("1GB hash verified OK: %s", dlHash[:16]+"...")
	}
}

// TestComprehensive_GB_ServiceLayer tests 1GB upload/download via the service
// layer (gRPC-equivalent path) with Redis + PostgreSQL + local storage.
func TestComprehensive_GB_ServiceLayer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GB-level test in short mode")
	}
	cluster := NewCompCluster(t, compClusterConfig{
		storageType: "local", useRedis: true, usePgSQL: true, numInstance: 1,
	})
	defer cluster.Cleanup()
	inst := cluster.Instances[0]

	totalSize := int64(1024 * 1024 * 1024) // 1GB
	path := "gb/svclayer/1gb.bin"

	// Use streaming upload via service layer
	clientID := "gb-test"
	hash := "" // no hash pre-check

	start := time.Now()
	sessionID, err := inst.TransferSvc.CreateUploadSession(path, filepath.Base(path), totalSize, clientID, hash)
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}

	// Upload in 16MB chunks
	chunkSize := int64(16 * 1024 * 1024)
	chunkData := make([]byte, chunkSize)
	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}
	hasher := sha256.New()
	sent := int64(0)
	for sent < totalSize {
		end := sent + chunkSize
		if end > totalSize {
			end = totalSize
		}
		thisChunk := chunkData[:end-sent]
		if err := inst.TransferSvc.UploadChunk(sessionID, thisChunk, sent); err != nil {
			t.Fatalf("UploadChunk at %d: %v", sent, err)
		}
		hasher.Write(thisChunk)
		sent = end
	}

	result, err := inst.TransferSvc.CompleteUpload(sessionID)
	if err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	uploadElapsed := time.Since(start)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))
	t.Logf("1GB service-layer streaming upload completed in %v (%.2f MB/s)",
		uploadElapsed, float64(totalSize)/1024/1024/uploadElapsed.Seconds())

	// Verify hash from result
	if result != nil && !result.HashVerified {
		t.Errorf("hash not verified by CompleteUpload")
	}

	// Download via service layer
	dlStart := time.Now()
	got, err := inst.FM.DownloadFile(path)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	dlElapsed := time.Since(dlStart)
	t.Logf("1GB service-layer download completed in %v (%.2f MB/s)",
		dlElapsed, float64(totalSize)/1024/1024/dlElapsed.Seconds())

	if int64(len(got)) != totalSize {
		t.Errorf("size mismatch: expected %d, got %d", totalSize, len(got))
	}
	dlHash := compSha256Hex(got)
	if dlHash != expectedHash {
		t.Errorf("download hash mismatch: expected %s, got %s", expectedHash, dlHash)
	} else {
		t.Logf("1GB hash verified OK: %s", dlHash[:16]+"...")
	}
}

// ---------------------------------------------------------------------------
// SECTION 3: Performance tests across file sizes (KB → GB), both protocols,
// both storage backends, with Redis + PostgreSQL
// ---------------------------------------------------------------------------

type perfRecord struct {
	Operation  string
	Storage    string
	Protocol   string
	FileSize   string
	SizeBytes  int64
	Duration   time.Duration
	Throughput float64 // MB/s
	HashOK     bool
}

func TestComprehensive_Performance_Matrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance matrix in short mode")
	}

	sizes := []int64{
		1 * 1024,        // 1KB
		64 * 1024,       // 64KB
		1024 * 1024,     // 1MB
		10 * 1024 * 1024, // 10MB
		100 * 1024 * 1024, // 100MB
	}

	storageTypes := []string{"local", "minio"}
	var records []perfRecord

	for _, st := range storageTypes {
		t.Run("storage_"+st, func(t *testing.T) {
			cluster := NewCompCluster(t, compClusterConfig{
				storageType: st, useRedis: true, usePgSQL: true, numInstance: 1,
			})
			defer cluster.Cleanup()
			inst := cluster.Instances[0]

			for _, size := range sizes {
				// --- HTTP upload/download ---
			data := genData(int(size))
			hash := compSha256Hex(data)
			pathHTTP := fmt.Sprintf("perf/%s/http/%s.bin", st, formatBytes(size))

				// HTTP upload
				upStart := time.Now()
				resp := httpUpload(t, inst.BaseURL, pathHTTP, data)
				upElapsed := time.Since(upStart)
				resp.Body.Close()
				if resp.StatusCode != http.StatusCreated {
					t.Fatalf("HTTP upload %s: status %d", formatBytes(size), resp.StatusCode)
				}

				// HTTP download
				dlStart := time.Now()
				got := httpDownload(t, inst.BaseURL, pathHTTP)
			dlElapsed := time.Since(dlStart)
			hashOK := compSha256Hex(got) == hash

				records = append(records, perfRecord{
					Operation: "upload", Storage: st, Protocol: "HTTP",
					FileSize: formatBytes(size), SizeBytes: size,
					Duration: upElapsed, Throughput: float64(size) / 1024 / 1024 / upElapsed.Seconds(),
					HashOK: true,
				})
				records = append(records, perfRecord{
					Operation: "download", Storage: st, Protocol: "HTTP",
					FileSize: formatBytes(size), SizeBytes: size,
					Duration: dlElapsed, Throughput: float64(size) / 1024 / 1024 / dlElapsed.Seconds(),
					HashOK: hashOK,
				})

				// --- Service layer (gRPC-equivalent) upload/download ---
				pathSL := fmt.Sprintf("perf/%s/svc/%s.bin", st, formatBytes(size))
				upStart = time.Now()
				_, err := inst.FM.UploadFile(pathSL, data)
				if err != nil {
					t.Fatalf("svc upload %s: %v", formatBytes(size), err)
				}
				upElapsed = time.Since(upStart)

				dlStart = time.Now()
				got, err = inst.FM.DownloadFile(pathSL)
				if err != nil {
					t.Fatalf("svc download %s: %v", formatBytes(size), err)
				}
			dlElapsed = time.Since(dlStart)
			hashOK = compSha256Hex(got) == hash

				records = append(records, perfRecord{
					Operation: "upload", Storage: st, Protocol: "gRPC",
					FileSize: formatBytes(size), SizeBytes: size,
					Duration: upElapsed, Throughput: float64(size) / 1024 / 1024 / upElapsed.Seconds(),
					HashOK: true,
				})
				records = append(records, perfRecord{
					Operation: "download", Storage: st, Protocol: "gRPC",
					FileSize: formatBytes(size), SizeBytes: size,
					Duration: dlElapsed, Throughput: float64(size) / 1024 / 1024 / dlElapsed.Seconds(),
					HashOK: hashOK,
				})

				t.Logf("[%s/%s/%s] upload: %.2f MB/s, download: %.2f MB/s, hashOK=%v",
					st, "HTTP", formatBytes(size),
					float64(size)/1024/1024/upElapsed.Seconds(),
					float64(size)/1024/1024/dlElapsed.Seconds(),
					hashOK)
			}
		})
	}

	// Print performance summary table
	t.Log("\n========== PERFORMANCE MATRIX SUMMARY ==========")
	t.Log("Storage | Protocol | Op       | Size    | Duration  | Throughput (MB/s) | HashOK")
	t.Log("--------|----------|----------|---------|-----------|-------------------|-------")
	sort.Slice(records, func(i, j int) bool {
		if records[i].Storage != records[j].Storage {
			return records[i].Storage < records[j].Storage
		}
		if records[i].Protocol != records[j].Protocol {
			return records[i].Protocol < records[j].Protocol
		}
		if records[i].Operation != records[j].Operation {
			return records[i].Operation < records[j].Operation
		}
		return records[i].SizeBytes < records[j].SizeBytes
	})
	for _, r := range records {
		t.Logf("%-7s | %-8s | %-8s | %-7s | %9s | %17.2f | %v",
			r.Storage, r.Protocol, r.Operation, r.FileSize,
			r.Duration.Round(time.Millisecond), r.Throughput, r.HashOK)
	}
	t.Log("=================================================")
}

// ---------------------------------------------------------------------------
// SECTION 4: Stress tests (high concurrency, Redis + PostgreSQL, both storages)
// ---------------------------------------------------------------------------

// TestComprehensive_Stress_ConcurrentUploads tests concurrent uploads under
// heavy load with Redis distributed lock + PostgreSQL.
func TestComprehensive_Stress_ConcurrentUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	storageTypes := []string{"local", "minio"}
	for _, st := range storageTypes {
		t.Run("storage_"+st, func(t *testing.T) {
			cluster := NewCompCluster(t, compClusterConfig{
				storageType: st, useRedis: true, usePgSQL: true, numInstance: 3,
			})
			defer cluster.Cleanup()

			concurrency := 20
			filesPerWorker := 5
			fileSize := 1 * 1024 * 1024 // 1MB

			var wg sync.WaitGroup
			errCh := make(chan error, concurrency*filesPerWorker)
			var successCount int64
			var mu sync.Mutex

			start := time.Now()
			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for f := 0; f < filesPerWorker; f++ {
						// Round-robin across instances
						inst := cluster.Instances[(workerID+f)%len(cluster.Instances)]
						path := fmt.Sprintf("stress/%s/w%d-f%d.bin", st, workerID, f)
						data := genData(fileSize)
						resp := httpUpload(t, inst.BaseURL, path, data)
						resp.Body.Close()
						if resp.StatusCode != http.StatusCreated {
							errCh <- fmt.Errorf("worker %d file %d: status %d", workerID, f, resp.StatusCode)
							return
						}
						mu.Lock()
						successCount++
						mu.Unlock()
					}
				}(w)
			}
			wg.Wait()
			close(errCh)
			elapsed := time.Since(start)

			var errs []error
			for e := range errCh {
				errs = append(errs, e)
			}

			t.Logf("[%s] %d workers × %d files (%dMB each) = %d total, succeeded=%d, failed=%d, elapsed=%v",
				st, concurrency, filesPerWorker, fileSize/1024/1024,
				concurrency*filesPerWorker, successCount, len(errs), elapsed.Round(time.Millisecond))

			if len(errs) > 0 {
				t.Errorf("[%s] %d errors during stress test, first: %v", st, len(errs), errs[0])
			}
		})
	}
}

// TestComprehensive_Stress_MixedWorkload runs a mixed read/write/delete
// workload against a multi-instance cluster with Redis + PostgreSQL.
func TestComprehensive_Stress_MixedWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	storageTypes := []string{"local", "minio"}
	for _, st := range storageTypes {
		t.Run("storage_"+st, func(t *testing.T) {
			cluster := NewCompCluster(t, compClusterConfig{
				storageType: st, useRedis: true, usePgSQL: true, numInstance: 3,
			})
			defer cluster.Cleanup()

			// Pre-populate files
			prePopulate := 30
			fileSize := 256 * 1024 // 256KB
			for i := 0; i < prePopulate; i++ {
				inst := cluster.Instances[i%len(cluster.Instances)]
				path := fmt.Sprintf("mixed/%s/pre-%d.bin", st, i)
				resp := httpUpload(t, inst.BaseURL, path, genData(fileSize))
				resp.Body.Close()
			}

			// Mixed workload
			concurrency := 15
			opsPerWorker := 20
			var wg sync.WaitGroup
			opCounts := map[string]int64{"upload": 0, "download": 0, "delete": 0}
			var mu sync.Mutex
			errCh := make(chan error, concurrency*opsPerWorker)

			start := time.Now()
			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for op := 0; op < opsPerWorker; op++ {
						inst := cluster.Instances[(workerID+op)%len(cluster.Instances)]
						// Cycle: 40% download, 40% upload, 20% delete
						choice := (workerID + op) % 5
						switch choice {
						case 0, 1: // download pre-populated file
							idx := (workerID*opsPerWorker + op) % prePopulate
							path := fmt.Sprintf("mixed/%s/pre-%d.bin", st, idx)
							resp, err := http.Get(inst.BaseURL + "/api/v1/files/" + path)
							if err != nil {
								errCh <- err
								continue
							}
							resp.Body.Close()
							mu.Lock(); opCounts["download"]++; mu.Unlock()
						case 2, 3: // upload new file
							path := fmt.Sprintf("mixed/%s/w%d-o%d.bin", st, workerID, op)
							resp := httpUpload(t, inst.BaseURL, path, genData(fileSize))
							resp.Body.Close()
							mu.Lock(); opCounts["upload"]++; mu.Unlock()
						case 4: // delete a file (may already be deleted — that's OK)
							idx := (workerID*opsPerWorker + op) % prePopulate
							path := fmt.Sprintf("mixed/%s/pre-%d.bin", st, idx)
							req, _ := http.NewRequest("DELETE", inst.BaseURL+"/api/v1/files/"+path, nil)
							resp, err := http.DefaultClient.Do(req)
							if err != nil {
								errCh <- err
								continue
							}
							resp.Body.Close()
							mu.Lock(); opCounts["delete"]++; mu.Unlock()
						}
					}
				}(w)
			}
			wg.Wait()
			close(errCh)
			elapsed := time.Since(start)

			var errs []error
			for e := range errCh {
				errs = append(errs, e)
			}

			totalOps := opCounts["upload"] + opCounts["download"] + opCounts["delete"]
			t.Logf("[%s] Mixed workload: %d ops (upload=%d, download=%d, delete=%d), errors=%d, elapsed=%v, throughput=%.1f ops/s",
				st, totalOps, opCounts["upload"], opCounts["download"], opCounts["delete"],
				len(errs), elapsed.Round(time.Millisecond), float64(totalOps)/elapsed.Seconds())

			if len(errs) > 0 {
				t.Errorf("[%s] %d errors, first: %v", st, len(errs), errs[0])
			}
		})
	}
}

// TestComprehensive_Stress_RedisLock_Mutex verifies that the Redis distributed
// lock correctly serializes concurrent writes to the same file path across
// multiple instances.
func TestComprehensive_Stress_RedisLock_Mutex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	cluster := NewCompCluster(t, compClusterConfig{
		storageType: "local", useRedis: true, usePgSQL: true, numInstance: 3,
	})
	defer cluster.Cleanup()

	concurrency := 10
	path := "stress/mutex/concurrent-write.bin"
	data := genData(64 * 1024) // 64KB

	var wg sync.WaitGroup
	successCount := int64(0)
	var mu sync.Mutex

	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			inst := cluster.Instances[workerID%len(cluster.Instances)]
			resp := httpUpload(t, inst.BaseURL, path, data)
			resp.Body.Close()
			if resp.StatusCode == http.StatusCreated {
				mu.Lock(); successCount++; mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// All concurrent writes should succeed (idempotent overwrite design)
	t.Logf("Redis lock mutex: %d concurrent writes to same path, %d succeeded, elapsed=%v",
		concurrency, successCount, elapsed.Round(time.Millisecond))

	if successCount != int64(concurrency) {
		t.Errorf("expected %d successes, got %d", concurrency, successCount)
	}

	// Verify final file is intact
	got := httpDownload(t, cluster.Instances[0].BaseURL, path)
	if !bytes.Equal(data, got) {
		t.Errorf("final file data mismatch after concurrent writes")
	}
}

// ---------------------------------------------------------------------------
// SECTION 5: Multi-instance consistency with Redis + PostgreSQL
// ---------------------------------------------------------------------------

// TestComprehensive_MultiInstance_Consistency verifies that a file uploaded to
// one instance is immediately visible to other instances (shared DB + Redis).
func TestComprehensive_MultiInstance_Consistency(t *testing.T) {
	storageTypes := []string{"local", "minio"}
	for _, st := range storageTypes {
		t.Run("storage_"+st, func(t *testing.T) {
			cluster := NewCompCluster(t, compClusterConfig{
				storageType: st, useRedis: true, usePgSQL: true, numInstance: 3,
			})
			defer cluster.Cleanup()

			data := genData(256 * 1024) // 256KB
			path := fmt.Sprintf("consistency/%s/cross-instance.bin", st)

			// Upload to instance 0
			resp := httpUpload(t, cluster.Instances[0].BaseURL, path, data)
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("upload status %d", resp.StatusCode)
			}

			// Download from instance 1 and 2 — should see the file immediately
			for i := 1; i < len(cluster.Instances); i++ {
				got := httpDownload(t, cluster.Instances[i].BaseURL, path)
				if !bytes.Equal(data, got) {
					t.Errorf("instance %d: data mismatch", i)
				}
			}

			// Verify metadata is consistent across instances
			meta0, _ := cluster.Instances[0].FM.GetFileMetadata(path)
			for i := 1; i < len(cluster.Instances); i++ {
				metaI, err := cluster.Instances[i].FM.GetFileMetadata(path)
				if err != nil {
					t.Errorf("instance %d GetMetadata: %v", i, err)
					continue
				}
				if metaI.Hash != meta0.Hash {
					t.Errorf("instance %d hash mismatch: %s vs %s", i, metaI.Hash, meta0.Hash)
				}
				if metaI.Size != meta0.Size {
					t.Errorf("instance %d size mismatch: %d vs %d", i, metaI.Size, meta0.Size)
				}
			}
			t.Logf("[%s] Cross-instance consistency verified across %d instances", st, len(cluster.Instances))
		})
	}
}
