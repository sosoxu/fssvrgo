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
	"testing"
	"time"

	httpserver "github.com/sosoxu/fssvrgo/internal/api/http"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type PerfInstance struct {
	ID          int
	BaseURL     string
	Server      *httpserver.Server
	FM          *filemanager.FileManager
	DirSvc      *directory.DirectoryManager
	FlSvc       *filelist.FileListService
	TransferSvc *transfer.FileTransferService
}

type PerfCluster struct {
	Instances    []*PerfInstance
	StorageDir   string
	TempDir      string
	SharedDB     *database.DB
	SharedStore  storage.StorageAdapter
	dbObj        *database.Database
	RedisManager *distributed.RedisManager
}

type PerfResult struct {
	Operation   string
	FileSize    string
	FileSizeInt int64
	Protocol    string
	Duration    time.Duration
	Throughput  float64
	Error       string
}

func NewPerfCluster(t *testing.T) *PerfCluster {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "fsserver-perf-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	storageDir := filepath.Join(tempDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create storage dir: %v", err)
	}

	dbCfg := config.DatabaseConfig{
		Type:     "postgresql",
		Host:     "localhost",
		Port:     5432,
		Name:     "fsserver",
		User:     "fsserver",
		Password: "fsserver123",
		SSLMode:  "disable",
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		os.RemoveAll(tempDir)
		t.Skipf("PostgreSQL not available: %v", err)
	}

	qdb := dbObj.GetQueryDB()
	_, _ = qdb.Exec("DELETE FROM transfer_tasks")
	_, _ = qdb.Exec("DELETE FROM audit_log")
	_, _ = qdb.Exec("DELETE FROM api_keys")
	_, _ = qdb.Exec("DELETE FROM files")
	_, _ = qdb.Exec("DELETE FROM directories")
	_, _ = qdb.Exec("DELETE FROM schema_migrations")

	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1,
		Name:    "initial_schema",
		Up:      func() error { return database.InitTables(qdb) },
	})
	if err := migrationMgr.RunMigrations(); err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to run migrations: %v", err)
	}

	store := storage.NewLocalStorage(storageDir)

	redisMgr, err := distributed.NewRedisManager("localhost:6379", "", 0, 10)
	if err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Skipf("Redis not available: %v", err)
	}

	redisClient := redisMgr.GetClient()
	ctx := context.Background()
	keys, _ := redisClient.Keys(ctx, "fsserver:*").Result()
	if len(keys) > 0 {
		redisClient.Del(ctx, keys...)
	}

	cluster := &PerfCluster{
		StorageDir:   storageDir,
		TempDir:      tempDir,
		SharedDB:     qdb,
		SharedStore:  store,
		dbObj:        dbObj,
		RedisManager: redisMgr,
	}

	distLock := redisMgr.GetLock()
	sessionStore := redisMgr.GetSessionStore()

	for i := 0; i < 3; i++ {
		inst := createPerfInstance(t, i, qdb, store, distLock, sessionStore)
		cluster.Instances = append(cluster.Instances, inst)
	}

	return cluster
}

func createPerfInstance(t *testing.T, id int, db *database.DB, store storage.StorageAdapter, distLock distributed.DistributedLock, sessionStore distributed.SessionStore) *PerfInstance {
	t.Helper()

	fm := filemanager.NewFileManagerWithDistLock(store, db, distLock)
	dirSvc := directory.NewDirectoryManager(db)
	flSvc := filelist.NewFileListService(db)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferServiceWithRedis(store, db, sessionStore, distLock)

	serverCfg := config.ServerConfig{
		HTTPPort:           0,
		MaxUploadSizeMB:    2048,
		MaxChunkSizeMB:     128,
		MaxPageSize:        1000,
		CORSAllowedOrigins: "*",
	}

	srv := httpserver.NewServer(serverCfg, config.TLSConfig{}, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, store, cacheSvc, nil, db)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Instance %d: Failed to listen: %v", id, err)
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

	return &PerfInstance{
		ID:          id,
		BaseURL:     baseURL,
		Server:      srv,
		FM:          fm,
		DirSvc:      dirSvc,
		FlSvc:       flSvc,
		TransferSvc: transferSvc,
	}
}

func (c *PerfCluster) Cleanup() {
	for _, inst := range c.Instances {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		inst.Server.Shutdown(ctx)
		cancel()
	}

	if c.RedisManager != nil {
		ctx := context.Background()
		client := c.RedisManager.GetClient()
		keys, _ := client.Keys(ctx, "fsserver:*").Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		c.RedisManager.Close()
	}

	_, _ = c.SharedDB.Exec("DELETE FROM transfer_tasks")
	_, _ = c.SharedDB.Exec("DELETE FROM audit_log")
	_, _ = c.SharedDB.Exec("DELETE FROM api_keys")
	_, _ = c.SharedDB.Exec("DELETE FROM files")
	_, _ = c.SharedDB.Exec("DELETE FROM directories")
	_, _ = c.SharedDB.Exec("DELETE FROM schema_migrations")

	c.dbObj.Close()
	os.RemoveAll(c.TempDir)
}

func (c *PerfCluster) CleanFiles() {
	_, _ = c.SharedDB.Exec("DELETE FROM transfer_tasks")
	_, _ = c.SharedDB.Exec("DELETE FROM files")
	_, _ = c.SharedDB.Exec("DELETE FROM directories")

	ctx := context.Background()
	client := c.RedisManager.GetClient()
	keys, _ := client.Keys(ctx, "fsserver:*").Result()
	if len(keys) > 0 {
		client.Del(ctx, keys...)
	}
}

func generatePerfData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func formatSize(size int64) string {
	switch {
	case size >= 1024*1024*1024:
		return fmt.Sprintf("%dGB", size/(1024*1024*1024))
	case size >= 1024*1024:
		return fmt.Sprintf("%dMB", size/(1024*1024))
	case size >= 1024:
		return fmt.Sprintf("%dKB", size/1024)
	default:
		return fmt.Sprintf("%dB", size)
	}
}

func uploadPerfHTTP(baseURL, filePath, fileName string, data []byte) (*http.Response, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		return nil, err
	}
	part.Write(data)
	writer.WriteField("path", filePath)
	writer.Close()
	return http.Post(baseURL+"/api/v1/files", writer.FormDataContentType(), &buf)
}

func downloadPerfHTTP(baseURL, filePath string) ([]byte, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/files" + filePath)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return data, resp.StatusCode, nil
}

func streamingUploadPerfHTTP(baseURL, filePath, fileName string, data []byte, chunkSize int) error {
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
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("chunk upload failed with status %d", resp.StatusCode)
		}
	}

	resp, err = http.Post(baseURL+fmt.Sprintf("/api/v1/uploads/%s/complete", sessionID), "application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func streamingDownloadPerfHTTP(baseURL, filePath string, chunkSize int) ([]byte, error) {
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

func runPerfBenchmark(b *testing.B, operation func() error) {
	for i := 0; i < b.N; i++ {
		if err := operation(); err != nil {
			b.Fatalf("operation failed: %v", err)
		}
	}
}

func BenchmarkPostgreSQLRedis_HTTP_Upload_1KB(b *testing.B) {
	benchmarkHTTPUpload(b, 1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Upload_64KB(b *testing.B) {
	benchmarkHTTPUpload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Upload_256KB(b *testing.B) {
	benchmarkHTTPUpload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Upload_1MB(b *testing.B) {
	benchmarkHTTPUpload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Upload_10MB(b *testing.B) {
	benchmarkHTTPUpload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Upload_50MB(b *testing.B) {
	benchmarkHTTPUpload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_HTTP_Download_1KB(b *testing.B) {
	benchmarkHTTPDownload(b, 1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Download_64KB(b *testing.B) {
	benchmarkHTTPDownload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Download_256KB(b *testing.B) {
	benchmarkHTTPDownload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Download_1MB(b *testing.B) {
	benchmarkHTTPDownload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Download_10MB(b *testing.B) {
	benchmarkHTTPDownload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_Download_50MB(b *testing.B) {
	benchmarkHTTPDownload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_HTTP_StreamingUpload_1KB(b *testing.B) {
	benchmarkHTTPStreamingUpload(b, 1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingUpload_64KB(b *testing.B) {
	benchmarkHTTPStreamingUpload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingUpload_256KB(b *testing.B) {
	benchmarkHTTPStreamingUpload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingUpload_1MB(b *testing.B) {
	benchmarkHTTPStreamingUpload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingUpload_10MB(b *testing.B) {
	benchmarkHTTPStreamingUpload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingUpload_50MB(b *testing.B) {
	benchmarkHTTPStreamingUpload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_HTTP_StreamingDownload_1KB(b *testing.B) {
	benchmarkHTTPStreamingDownload(b, 1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingDownload_64KB(b *testing.B) {
	benchmarkHTTPStreamingDownload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingDownload_256KB(b *testing.B) {
	benchmarkHTTPStreamingDownload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingDownload_1MB(b *testing.B) {
	benchmarkHTTPStreamingDownload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingDownload_10MB(b *testing.B) {
	benchmarkHTTPStreamingDownload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_HTTP_StreamingDownload_50MB(b *testing.B) {
	benchmarkHTTPStreamingDownload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_GRPC_Upload_1KB(b *testing.B) {
	benchmarkGRPCUpload(b, 1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Upload_64KB(b *testing.B) {
	benchmarkGRPCUpload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Upload_256KB(b *testing.B) {
	benchmarkGRPCUpload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Upload_1MB(b *testing.B) {
	benchmarkGRPCUpload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Upload_10MB(b *testing.B) {
	benchmarkGRPCUpload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Upload_50MB(b *testing.B) {
	benchmarkGRPCUpload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_GRPC_Download_1KB(b *testing.B) {
	benchmarkGRPCDownload(b, 1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Download_64KB(b *testing.B) {
	benchmarkGRPCDownload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Download_256KB(b *testing.B) {
	benchmarkGRPCDownload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Download_1MB(b *testing.B) {
	benchmarkGRPCDownload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Download_10MB(b *testing.B) {
	benchmarkGRPCDownload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_Download_50MB(b *testing.B) {
	benchmarkGRPCDownload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_GRPC_StreamingUpload_1KB(b *testing.B) {
	benchmarkGRPCStreamingUpload(b, 1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingUpload_64KB(b *testing.B) {
	benchmarkGRPCStreamingUpload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingUpload_256KB(b *testing.B) {
	benchmarkGRPCStreamingUpload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingUpload_1MB(b *testing.B) {
	benchmarkGRPCStreamingUpload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingUpload_10MB(b *testing.B) {
	benchmarkGRPCStreamingUpload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingUpload_50MB(b *testing.B) {
	benchmarkGRPCStreamingUpload(b, 50*1024*1024)
}

func BenchmarkPostgreSQLRedis_GRPC_StreamingDownload_1KB(b *testing.B) {
	benchmarkGRPCStreamingDownload(b, 1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingDownload_64KB(b *testing.B) {
	benchmarkGRPCStreamingDownload(b, 64*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingDownload_256KB(b *testing.B) {
	benchmarkGRPCStreamingDownload(b, 256*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingDownload_1MB(b *testing.B) {
	benchmarkGRPCStreamingDownload(b, 1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingDownload_10MB(b *testing.B) {
	benchmarkGRPCStreamingDownload(b, 10*1024*1024)
}
func BenchmarkPostgreSQLRedis_GRPC_StreamingDownload_50MB(b *testing.B) {
	benchmarkGRPCStreamingDownload(b, 50*1024*1024)
}

var perfCluster *PerfCluster

func initPerfCluster(b *testing.B) *PerfInstance {
	if perfCluster == nil {
		t := &testing.T{}
		perfCluster = NewPerfCluster(t)
	}
	return perfCluster.Instances[0]
}

func cleanupPerfCluster() {
	if perfCluster != nil {
		perfCluster.Cleanup()
		perfCluster = nil
	}
}

func benchmarkHTTPUpload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	fileName := fmt.Sprintf("perf_upload_%s.dat", formatSize(int64(size)))

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/perf/http/upload/%s/%d", formatSize(int64(size)), i)
		resp, err := uploadPerfHTTP(inst.BaseURL, path, fileName, data)
		if err != nil {
			b.Fatalf("upload failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected status: %d", resp.StatusCode)
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkHTTPDownload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	fileName := fmt.Sprintf("perf_dl_%s.dat", formatSize(int64(size)))
	path := fmt.Sprintf("/perf/http/download/%s/file.dat", formatSize(int64(size)))

	resp, err := uploadPerfHTTP(inst.BaseURL, path, fileName, data)
	if err != nil {
		b.Fatalf("setup upload failed: %v", err)
	}
	resp.Body.Close()

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		downloaded, status, err := downloadPerfHTTP(inst.BaseURL, path)
		if err != nil {
			b.Fatalf("download failed: %v", err)
		}
		if status != http.StatusOK {
			b.Fatalf("unexpected status: %d", status)
		}
		if len(downloaded) != size {
			b.Fatalf("size mismatch: expected %d, got %d", size, len(downloaded))
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkHTTPStreamingUpload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	chunkSize := 256 * 1024
	if size < chunkSize {
		chunkSize = size
	}
	fileName := fmt.Sprintf("perf_stream_ul_%s.dat", formatSize(int64(size)))

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/perf/http/stream_upload/%s/%d", formatSize(int64(size)), i)
		if err := streamingUploadPerfHTTP(inst.BaseURL, path, fileName, data, chunkSize); err != nil {
			b.Fatalf("streaming upload failed: %v", err)
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkHTTPStreamingDownload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	chunkSize := 256 * 1024
	if size < chunkSize {
		chunkSize = size
	}
	fileName := fmt.Sprintf("perf_stream_dl_%s.dat", formatSize(int64(size)))
	path := fmt.Sprintf("/perf/http/stream_download/%s/file.dat", formatSize(int64(size)))

	if err := streamingUploadPerfHTTP(inst.BaseURL, path, fileName, data, chunkSize); err != nil {
		b.Fatalf("setup streaming upload failed: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		downloaded, err := streamingDownloadPerfHTTP(inst.BaseURL, path, chunkSize)
		if err != nil {
			b.Fatalf("streaming download failed: %v", err)
		}
		if len(downloaded) != size {
			b.Fatalf("size mismatch: expected %d, got %d", size, len(downloaded))
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkGRPCUpload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/perf/grpc/upload/%s/%d", formatSize(int64(size)), i)
		_, err := inst.FM.UploadFile(path, data)
		if err != nil {
			b.Fatalf("upload failed: %v", err)
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkGRPCDownload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	path := fmt.Sprintf("/perf/grpc/download/%s/file.dat", formatSize(int64(size)))

	_, err := inst.FM.UploadFile(path, data)
	if err != nil {
		b.Fatalf("setup upload failed: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		downloaded, err := inst.FM.DownloadFile(path)
		if err != nil {
			b.Fatalf("download failed: %v", err)
		}
		if len(downloaded) != size {
			b.Fatalf("size mismatch: expected %d, got %d", size, len(downloaded))
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkGRPCStreamingUpload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	chunkSize := 256 * 1024
	if size < chunkSize {
		chunkSize = size
	}
	totalSize := int64(len(data))
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/perf/grpc/stream_upload/%s/%d", formatSize(int64(size)), i)
		sessionID, err := inst.TransferSvc.CreateUploadSession(path, filepath.Base(path), totalSize, "perf", hash)
		if err != nil {
			b.Fatalf("create session failed: %v", err)
		}

		for offset := 0; offset < len(data); offset += chunkSize {
			end := offset + chunkSize
			if end > len(data) {
				end = len(data)
			}
			if err := inst.TransferSvc.UploadChunk(sessionID, data[offset:end], int64(offset)); err != nil {
				b.Fatalf("upload chunk failed: %v", err)
			}
		}

		if err := inst.TransferSvc.CompleteUpload(sessionID); err != nil {
			b.Fatalf("complete upload failed: %v", err)
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func benchmarkGRPCStreamingDownload(b *testing.B, size int) {
	inst := initPerfCluster(b)
	data := generatePerfData(size)
	chunkSize := 256 * 1024
	if size < chunkSize {
		chunkSize = size
	}
	totalSize := int64(len(data))
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	path := fmt.Sprintf("/perf/grpc/stream_download/%s/file.dat", formatSize(int64(size)))

	sessionID, err := inst.TransferSvc.CreateUploadSession(path, filepath.Base(path), totalSize, "perf", hash)
	if err != nil {
		b.Fatalf("setup create session failed: %v", err)
	}
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		inst.TransferSvc.UploadChunk(sessionID, data[offset:end], int64(offset))
	}
	inst.TransferSvc.CompleteUpload(sessionID)

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		dlSessionID, err := inst.TransferSvc.CreateDownloadSession(path, "perf")
		if err != nil {
			b.Fatalf("create download session failed: %v", err)
		}

		var reassembled []byte
		var offset int64
		for offset < totalSize {
			sz := int64(chunkSize)
			if totalSize-offset < sz {
				sz = totalSize - offset
			}
			chunk, err := inst.TransferSvc.DownloadChunk(dlSessionID, int(sz), offset)
			if err != nil {
				b.Fatalf("download chunk failed: %v", err)
			}
			reassembled = append(reassembled, chunk...)
			offset += int64(len(chunk))
		}

		if len(reassembled) != size {
			b.Fatalf("size mismatch: expected %d, got %d", size, len(reassembled))
		}
	}
	b.StopTimer()
	perfCluster.CleanFiles()
}

func TestPostgreSQLRedis_Performance(t *testing.T) {
	sizes := []int64{
		1 * 1024,
		64 * 1024,
		256 * 1024,
		1024 * 1024,
		10 * 1024 * 1024,
		50 * 1024 * 1024,
	}

	cluster := NewPerfCluster(t)
	defer cluster.Cleanup()

	inst := cluster.Instances[0]
	chunkSize := 256 * 1024
	iterations := 3

	var results []PerfResult

	for _, size := range sizes {
		data := generatePerfData(int(size))
		sizeLabel := formatSize(size)
		hash := fmt.Sprintf("%x", sha256.Sum256(data))
		t.Logf("=== File Size: %s ===", sizeLabel)

		var durations []time.Duration

		for i := 0; i < iterations; i++ {
			path := fmt.Sprintf("/perf/http/upload/%s/%d", sizeLabel, i)
			start := time.Now()
			resp, err := uploadPerfHTTP(inst.BaseURL, path, fmt.Sprintf("perf_%s.dat", sizeLabel), data)
			dur := time.Since(start)
			if err != nil {
				results = append(results, PerfResult{Operation: "Upload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "HTTP", Error: err.Error()})
				continue
			}
			resp.Body.Close()
			durations = append(durations, dur)
		}
		avgDur := avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "Upload",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "HTTP",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  HTTP Upload:     %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		uploadPath := fmt.Sprintf("/perf/http/upload/%s/0", sizeLabel)
		for i := 0; i < iterations; i++ {
			start := time.Now()
			downloaded, status, err := downloadPerfHTTP(inst.BaseURL, uploadPath)
			dur := time.Since(start)
			if err != nil || status != 200 {
				results = append(results, PerfResult{Operation: "Download", FileSize: sizeLabel, FileSizeInt: size, Protocol: "HTTP", Error: fmt.Sprintf("err=%v status=%d", err, status)})
				continue
			}
			if len(downloaded) != int(size) {
				results = append(results, PerfResult{Operation: "Download", FileSize: sizeLabel, FileSizeInt: size, Protocol: "HTTP", Error: "size mismatch"})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "Download",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "HTTP",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  HTTP Download:   %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		for i := 0; i < iterations; i++ {
			path := fmt.Sprintf("/perf/http/stream_upload/%s/%d", sizeLabel, i)
			start := time.Now()
			err := streamingUploadPerfHTTP(inst.BaseURL, path, fmt.Sprintf("perf_stream_%s.dat", sizeLabel), data, chunkSize)
			dur := time.Since(start)
			if err != nil {
				results = append(results, PerfResult{Operation: "StreamUpload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "HTTP", Error: err.Error()})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "StreamUpload",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "HTTP",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  HTTP Stream UL:  %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		streamPath := fmt.Sprintf("/perf/http/stream_upload/%s/0", sizeLabel)
		for i := 0; i < iterations; i++ {
			start := time.Now()
			downloaded, err := streamingDownloadPerfHTTP(inst.BaseURL, streamPath, chunkSize)
			dur := time.Since(start)
			if err != nil {
				results = append(results, PerfResult{Operation: "StreamDownload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "HTTP", Error: err.Error()})
				continue
			}
			if len(downloaded) != int(size) {
				results = append(results, PerfResult{Operation: "StreamDownload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "HTTP", Error: fmt.Sprintf("size mismatch: got %d", len(downloaded))})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "StreamDownload",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "HTTP",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  HTTP Stream DL:  %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		cluster.CleanFiles()

		for i := 0; i < iterations; i++ {
			path := fmt.Sprintf("/perf/grpc/upload/%s/%d", sizeLabel, i)
			start := time.Now()
			_, err := inst.FM.UploadFile(path, data)
			dur := time.Since(start)
			if err != nil {
				results = append(results, PerfResult{Operation: "Upload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: err.Error()})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "Upload",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "gRPC",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  gRPC Upload:     %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		grpcPath := fmt.Sprintf("/perf/grpc/upload/%s/0", sizeLabel)
		for i := 0; i < iterations; i++ {
			start := time.Now()
			downloaded, err := inst.FM.DownloadFile(grpcPath)
			dur := time.Since(start)
			if err != nil {
				results = append(results, PerfResult{Operation: "Download", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: err.Error()})
				continue
			}
			if len(downloaded) != int(size) {
				results = append(results, PerfResult{Operation: "Download", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: "size mismatch"})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "Download",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "gRPC",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  gRPC Download:   %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		cluster.CleanFiles()

		for i := 0; i < iterations; i++ {
			path := fmt.Sprintf("/perf/grpc/stream_upload/%s/%d", sizeLabel, i)
			start := time.Now()
			sessionID, err := inst.TransferSvc.CreateUploadSession(path, fmt.Sprintf("perf_%s.dat", sizeLabel), int64(size), "perf", hash)
			if err != nil {
				results = append(results, PerfResult{Operation: "StreamUpload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: err.Error()})
				continue
			}
			cs := chunkSize
			if int(size) < cs {
				cs = int(size)
			}
			for offset := 0; offset < int(size); offset += cs {
				end := offset + cs
				if end > int(size) {
					end = int(size)
				}
				inst.TransferSvc.UploadChunk(sessionID, data[offset:end], int64(offset))
			}
			err = inst.TransferSvc.CompleteUpload(sessionID)
			dur := time.Since(start)
			if err != nil {
				results = append(results, PerfResult{Operation: "StreamUpload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: err.Error()})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "StreamUpload",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "gRPC",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  gRPC Stream UL:  %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		grpcStreamPath := fmt.Sprintf("/perf/grpc/stream_upload/%s/0", sizeLabel)
		for i := 0; i < iterations; i++ {
			start := time.Now()
			dlSessionID, err := inst.TransferSvc.CreateDownloadSession(grpcStreamPath, "perf")
			if err != nil {
				results = append(results, PerfResult{Operation: "StreamDownload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: err.Error()})
				continue
			}
			var reassembled []byte
			var offset int64
			cs := int64(chunkSize)
			if int64(size) < cs {
				cs = int64(size)
			}
			for offset < int64(size) {
				sz := cs
				if int64(size)-offset < sz {
					sz = int64(size) - offset
				}
				chunk, err := inst.TransferSvc.DownloadChunk(dlSessionID, int(sz), offset)
				if err != nil {
					break
				}
				reassembled = append(reassembled, chunk...)
				offset += int64(len(chunk))
			}
			dur := time.Since(start)
			if len(reassembled) != int(size) {
				results = append(results, PerfResult{Operation: "StreamDownload", FileSize: sizeLabel, FileSizeInt: size, Protocol: "gRPC", Error: fmt.Sprintf("size mismatch: got %d", len(reassembled))})
				continue
			}
			durations = append(durations, dur)
		}
		avgDur = avgDuration(durations)
		results = append(results, PerfResult{
			Operation:   "StreamDownload",
			FileSize:    sizeLabel,
			FileSizeInt: size,
			Protocol:    "gRPC",
			Duration:    avgDur,
			Throughput:  float64(size) / avgDur.Seconds() / 1024 / 1024,
		})
		t.Logf("  gRPC Stream DL:  %v (%.2f MB/s)", avgDur, float64(size)/avgDur.Seconds()/1024/1024)
		durations = nil

		cluster.CleanFiles()
	}

	t.Log("\n========== Performance Summary (PostgreSQL + Redis) ==========")
	t.Log(strings.Repeat("-", 100))
	t.Logf("%-15s %-12s %-15s %-12s %-15s %s", "Operation", "Protocol", "FileSize", "Avg Time", "Throughput", "Error")
	t.Log(strings.Repeat("-", 100))

	sort.Slice(results, func(i, j int) bool {
		if results[i].FileSizeInt != results[j].FileSizeInt {
			return results[i].FileSizeInt < results[j].FileSizeInt
		}
		if results[i].Operation != results[j].Operation {
			return results[i].Operation < results[j].Operation
		}
		return results[i].Protocol < results[j].Protocol
	})

	for _, r := range results {
		errStr := ""
		if r.Error != "" {
			errStr = r.Error
		}
		t.Logf("%-15s %-12s %-15s %-12v %-12.2f MB/s %s",
			r.Operation, r.Protocol, r.FileSize, r.Duration.Round(time.Microsecond), r.Throughput, errStr)
	}

	t.Log(strings.Repeat("-", 100))

	t.Log("\n========== HTTP vs gRPC Comparison ==========")
	for _, size := range sizes {
		sizeLabel := formatSize(size)
		t.Logf("\n--- %s ---", sizeLabel)
		for _, op := range []string{"Upload", "Download", "StreamUpload", "StreamDownload"} {
			var httpResult, grpcResult *PerfResult
			for _, r := range results {
				if r.FileSizeInt == size && r.Operation == op && r.Protocol == "HTTP" && r.Error == "" {
					httpResult = &r
				}
				if r.FileSizeInt == size && r.Operation == op && r.Protocol == "gRPC" && r.Error == "" {
					grpcResult = &r
				}
			}
			if httpResult != nil && grpcResult != nil {
				ratio := grpcResult.Throughput / httpResult.Throughput
				faster := "gRPC"
				if httpResult.Throughput > grpcResult.Throughput {
					ratio = httpResult.Throughput / grpcResult.Throughput
					faster = "HTTP"
				}
				t.Logf("  %-15s: HTTP=%.2f MB/s, gRPC=%.2f MB/s, %s %.2fx faster",
					op, httpResult.Throughput, grpcResult.Throughput, faster, ratio)
			}
		}
	}
}

func avgDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}
