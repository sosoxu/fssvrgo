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
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type RedisInstance struct {
	ID          int
	BaseURL     string
	Server      *httpserver.Server
	Listener    net.Listener
	FM          *filemanager.FileManager
	DirSvc      *directory.DirectoryManager
	FlSvc       *filelist.FileListService
	TransferSvc *transfer.FileTransferService
}

type RedisCluster struct {
	Instances    []*RedisInstance
	StorageDir   string
	DBPath       string
	TempDir      string
	SharedDB     *database.DB
	SharedStore  storage.StorageAdapter
	dbObj        *database.Database
	RedisManager *distributed.RedisManager
}

func NewRedisCluster(t *testing.T, numInstances int) *RedisCluster {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "fsserver-redis-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	storageDir := filepath.Join(tempDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create storage dir: %v", err)
	}

	dbPath := filepath.Join(tempDir, "shared.db")
	dbCfg := config.DatabaseConfig{Type: "sqlite", Path: dbPath}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to connect to database: %v", err)
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
		t.Fatalf("Failed to run migrations: %v", err)
	}

	store := storage.NewLocalStorage(storageDir)

	redisMgr, err := distributed.NewRedisManager("localhost:6379", "", 0, 10)
	if err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	redisClient := redisMgr.GetClient()
	ctx := context.Background()
	keys, _ := redisClient.Keys(ctx, "fsserver:*").Result()
	if len(keys) > 0 {
		redisClient.Del(ctx, keys...)
	}

	cluster := &RedisCluster{
		StorageDir:   storageDir,
		DBPath:       dbPath,
		TempDir:      tempDir,
		SharedDB:     qdb,
		SharedStore:  store,
		dbObj:        dbObj,
		RedisManager: redisMgr,
	}

	distLock := redisMgr.GetLock()
	sessionStore := redisMgr.GetSessionStore()

	for i := 0; i < numInstances; i++ {
		inst := createRedisInstance(t, i, qdb, store, distLock, sessionStore)
		cluster.Instances = append(cluster.Instances, inst)
	}

	return cluster
}

func createRedisInstance(t *testing.T, id int, db *database.DB, store storage.StorageAdapter, distLock distributed.DistributedLock, sessionStore distributed.SessionStore) *RedisInstance {
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
		MaxUploadSizeMB:    1024,
		MaxChunkSizeMB:     64,
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

	return &RedisInstance{
		ID:          id,
		BaseURL:     baseURL,
		Server:      srv,
		FM:          fm,
		DirSvc:      dirSvc,
		FlSvc:       flSvc,
		TransferSvc: transferSvc,
	}
}

func (c *RedisCluster) Cleanup() {
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

	c.dbObj.Close()
	os.RemoveAll(c.TempDir)
}

func uploadRedisHTTP(baseURL, filePath, fileName string, data []byte) (*http.Response, error) {
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

func downloadRedisHTTP(baseURL, filePath string) ([]byte, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/files" + filePath)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return data, resp.StatusCode, nil
}

func deleteRedisHTTP(baseURL, filePath string) error {
	req, _ := http.NewRequest("DELETE", baseURL+"/api/v1/files"+filePath, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func getRedisMetadataHTTP(baseURL, filePath string) (map[string]interface{}, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/metadata" + filePath)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode, nil
}

func listRedisFilesHTTP(baseURL string) (map[string]interface{}, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/files")
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode, nil
}

func computeRedisHash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

func generateRedisData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func TestRedisLock_HTTPUploadDownloadConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024 * 100)
	expectedHash := computeRedisHash(data)
	filePath := "/redis_consistency_test.dat"

	resp, err := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, "redis_consistency_test.dat", data)
	if err != nil {
		t.Fatalf("Upload to instance 0 failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Upload returned status %d", resp.StatusCode)
	}

	for i, inst := range cluster.Instances {
		downloaded, status, err := downloadRedisHTTP(inst.BaseURL, filePath)
		if err != nil {
			t.Errorf("Download from instance %d failed: %v", i, err)
			continue
		}
		if status != http.StatusOK {
			t.Errorf("Download from instance %d returned status %d", i, status)
			continue
		}
		if len(downloaded) != len(data) {
			t.Errorf("Instance %d: size mismatch, expected %d, got %d", i, len(data), len(downloaded))
			continue
		}
		if computeRedisHash(downloaded) != expectedHash {
			t.Errorf("Instance %d: hash mismatch", i)
		}
	}
}

func TestRedisLock_HTTPMetadataConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024 * 50)
	filePath := "/redis_metadata_test.dat"

	resp, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, "redis_metadata_test.dat", data)
	resp.Body.Close()

	meta1, status1, _ := getRedisMetadataHTTP(cluster.Instances[0].BaseURL, filePath)
	if status1 != http.StatusOK {
		t.Fatalf("Get metadata from instance 0 failed: status=%d", status1)
	}
	meta1Inner, ok := meta1["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("Metadata response missing 'metadata' field: %v", meta1)
	}

	for i, inst := range cluster.Instances[1:] {
		meta, status, _ := getRedisMetadataHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: metadata returned status %d", i+1, status)
			continue
		}
		metaInner, ok := meta["metadata"].(map[string]interface{})
		if !ok {
			t.Errorf("Instance %d: metadata response missing 'metadata' field", i+1)
			continue
		}
		if metaInner["Path"] != meta1Inner["Path"] {
			t.Errorf("Instance %d: Path mismatch", i+1)
		}
		if metaInner["Size"] != meta1Inner["Size"] {
			t.Errorf("Instance %d: Size mismatch", i+1)
		}
		if metaInner["Hash"] != meta1Inner["Hash"] {
			t.Errorf("Instance %d: Hash mismatch", i+1)
		}
	}
}

func TestRedisLock_HTTPDeleteConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024)
	filePath := "/redis_delete_test.dat"

	resp, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, "redis_delete_test.dat", data)
	resp.Body.Close()

	if err := deleteRedisHTTP(cluster.Instances[1].BaseURL, filePath); err != nil {
		t.Fatalf("Delete from instance 1 failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		_, status, _ := downloadRedisHTTP(inst.BaseURL, filePath)
		if status != http.StatusNotFound {
			t.Errorf("Instance %d: file should be deleted, got status %d", i, status)
		}
	}
}

func TestRedisLock_HTTPRenameConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024)
	filePath := "/redis_rename_test.dat"

	resp, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, "redis_rename_test.dat", data)
	resp.Body.Close()

	body := `{"new_name":"redis_renamed.dat"}`
	req, _ := http.NewRequest("PATCH", cluster.Instances[1].BaseURL+"/api/v1/files"+filePath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	renameResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Rename from instance 1 failed: %v", err)
	}
	renameResp.Body.Close()
	if renameResp.StatusCode != http.StatusOK {
		t.Fatalf("Rename returned status %d", renameResp.StatusCode)
	}

	_, status, _ := downloadRedisHTTP(cluster.Instances[0].BaseURL, filePath)
	if status != http.StatusNotFound {
		t.Errorf("Old path should not exist after rename, got status %d", status)
	}

	newPath := "/redis_renamed.dat"
	for i, inst := range cluster.Instances {
		downloaded, status, _ := downloadRedisHTTP(inst.BaseURL, newPath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: new path should be downloadable, got status %d", i, status)
			continue
		}
		if computeRedisHash(downloaded) != computeRedisHash(data) {
			t.Errorf("Instance %d: data hash mismatch after rename", i)
		}
	}
}

func uploadRedisHTTPWithRetry(baseURL, filePath, fileName string, data []byte, maxRetries int) (*http.Response, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		resp, err := uploadRedisHTTP(baseURL, filePath, fileName, data)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		if resp.StatusCode == http.StatusInternalServerError {
			resp.Body.Close()
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("after %d retries: %v", maxRetries, lastErr)
}

func TestRedisLock_HTTPConcurrentWriteConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	numFiles := 5
	var wg sync.WaitGroup
	errors := make(chan error, numFiles*len(cluster.Instances))

	for i := 0; i < numFiles; i++ {
		for j, inst := range cluster.Instances {
			wg.Add(1)
			go func(fileIdx, instIdx int, baseURL string) {
				defer wg.Done()
				data := generateRedisData(1024 * (10 + fileIdx))
				filePath := fmt.Sprintf("/redis_concurrent_%d_%d.dat", instIdx, fileIdx)
				resp, err := uploadRedisHTTPWithRetry(baseURL, filePath, fmt.Sprintf("file_%d_%d.dat", instIdx, fileIdx), data, 10)
				if err != nil {
					errors <- fmt.Errorf("upload instance %d file %d: %v", instIdx, fileIdx, err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusCreated {
					errors <- fmt.Errorf("upload instance %d file %d: status %d", instIdx, fileIdx, resp.StatusCode)
					return
				}
			}(i, j, inst.BaseURL)
		}
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent write error: %v", err)
	}

	for i := 0; i < numFiles; i++ {
		for j := range cluster.Instances {
			filePath := fmt.Sprintf("/redis_concurrent_%d_%d.dat", j, i)
			for k, inst := range cluster.Instances {
				_, status, _ := downloadRedisHTTP(inst.BaseURL, filePath)
				if status != http.StatusOK {
					t.Errorf("Instance %d: cannot download file %s, status %d", k, filePath, status)
				}
			}
		}
	}
}

func TestRedisLock_HTTPConcurrentOverwriteConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data1 := generateRedisData(1024)
	data2 := generateRedisData(1024)
	data3 := generateRedisData(1024)
	filePath := "/redis_overwrite_test.dat"

	resp1, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, "redis_overwrite_test.dat", data1)
	resp1.Body.Close()
	resp2, _ := uploadRedisHTTP(cluster.Instances[1].BaseURL, filePath, "redis_overwrite_test.dat", data2)
	resp2.Body.Close()
	resp3, _ := uploadRedisHTTP(cluster.Instances[2].BaseURL, filePath, "redis_overwrite_test.dat", data3)
	resp3.Body.Close()

	time.Sleep(200 * time.Millisecond)

	downloaded1, status1, _ := downloadRedisHTTP(cluster.Instances[0].BaseURL, filePath)
	downloaded2, status2, _ := downloadRedisHTTP(cluster.Instances[1].BaseURL, filePath)
	downloaded3, status3, _ := downloadRedisHTTP(cluster.Instances[2].BaseURL, filePath)

	if status1 != http.StatusOK || status2 != http.StatusOK || status3 != http.StatusOK {
		t.Fatalf("Download status mismatch: inst0=%d, inst1=%d, inst2=%d", status1, status2, status3)
	}

	hash1 := computeRedisHash(downloaded1)
	hash2 := computeRedisHash(downloaded2)
	hash3 := computeRedisHash(downloaded3)

	if hash1 != hash2 || hash2 != hash3 {
		t.Errorf("Data inconsistency after concurrent overwrites: hash0=%s, hash1=%s, hash2=%s", hash1[:8], hash2[:8], hash3[:8])
	}

	validHashes := map[string]bool{
		computeRedisHash(data1): true,
		computeRedisHash(data2): true,
		computeRedisHash(data3): true,
	}
	if !validHashes[hash1] {
		t.Errorf("Downloaded data does not match any uploaded version")
	}
}

func TestRedisLock_HTTPStreamingUploadSessionShared(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateRedisData(1024 * 50)
	hash := computeRedisHash(data)
	filePath := "/redis_stream_shared_test.dat"

	body := fmt.Sprintf(`{"file_path":"%s","file_name":"redis_stream_shared_test.dat","total_size":%d,"hash":"%s"}`,
		filePath, len(data), hash)
	resp, err := http.Post(inst1.BaseURL+"/api/v1/uploads", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusCreated {
		resp.Body.Close()
		t.Fatalf("Create upload session on instance 0 failed: status=%d, err=%v", resp.StatusCode, err)
	}
	var sessionResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&sessionResp)
	resp.Body.Close()
	sessionID := sessionResp["session_id"].(string)

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, _ := writer.CreateFormFile("data", "chunk")
	part.Write(data)
	writer.WriteField("offset", "0")
	writer.Close()

	req, _ := http.NewRequest("PUT", inst2.BaseURL+"/api/v1/uploads/"+sessionID+"/chunk", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	chunkResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("Cross-instance chunk upload returned error: %v", err)
	} else {
		respBody, _ := io.ReadAll(chunkResp.Body)
		chunkResp.Body.Close()
		t.Logf("Cross-instance chunk upload: status=%d, body=%s", chunkResp.StatusCode, string(respBody))
		if chunkResp.StatusCode == http.StatusOK {
			t.Log("Cross-instance chunk upload succeeded (Redis session store is working)")
		} else {
			t.Log("Cross-instance chunk upload failed (temp file not shared across instances)")
		}
	}

	var buf2 bytes.Buffer
	writer2 := multipart.NewWriter(&buf2)
	part2, _ := writer2.CreateFormFile("data", "chunk")
	part2.Write(data)
	writer2.WriteField("offset", "0")
	writer2.Close()

	req2, _ := http.NewRequest("PUT", inst1.BaseURL+"/api/v1/uploads/"+sessionID+"/chunk", &buf2)
	req2.Header.Set("Content-Type", writer2.FormDataContentType())
	chunkResp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("Same-instance chunk upload failed: %v", err)
	}
	chunkResp2.Body.Close()
	if chunkResp2.StatusCode != http.StatusOK {
		t.Fatalf("Same-instance chunk upload returned status %d", chunkResp2.StatusCode)
	}

	completeResp, err := http.Post(inst1.BaseURL+"/api/v1/uploads/"+sessionID+"/complete", "application/json", nil)
	if err != nil {
		t.Fatalf("Complete upload failed: %v", err)
	}
	completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("Complete upload returned status %d", completeResp.StatusCode)
	}

	for i, inst := range cluster.Instances {
		downloaded, status, _ := downloadRedisHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: completed streaming upload should be downloadable, got status %d", i, status)
			continue
		}
		if computeRedisHash(downloaded) != hash {
			t.Errorf("Instance %d: downloaded data hash mismatch", i)
		}
	}
}

func TestRedisLock_HTTPStreamingDownloadConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024 * 100)
	expectedHash := computeRedisHash(data)
	filePath := "/redis_stream_dl_test.dat"

	resp, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, "redis_stream_dl_test.dat", data)
	resp.Body.Close()

	for i, inst := range cluster.Instances {
		req, _ := http.NewRequest("GET", inst.BaseURL+"/api/v1/files"+filePath, nil)
		req.Header.Set("Range", "bytes=0-1023")
		rangeResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("Instance %d: Range download failed: %v", i, err)
			continue
		}
		chunk, _ := io.ReadAll(rangeResp.Body)
		rangeResp.Body.Close()
		if rangeResp.StatusCode != http.StatusPartialContent && rangeResp.StatusCode != http.StatusOK {
			t.Errorf("Instance %d: Range download returned status %d", i, rangeResp.StatusCode)
			continue
		}
		if string(chunk) != string(data[:1024]) {
			t.Errorf("Instance %d: Range data mismatch", i)
		}
	}

	for i, inst := range cluster.Instances {
		downloaded, status, _ := downloadRedisHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: full download failed, status %d", i, status)
			continue
		}
		if computeRedisHash(downloaded) != expectedHash {
			t.Errorf("Instance %d: full download hash mismatch", i)
		}
	}
}

func TestRedisLock_GRPCUploadDownloadConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024 * 100)
	filePath := "/redis_grpc_consistency_test.dat"

	_, err := cluster.Instances[0].FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload via instance 0 failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		downloaded, err := inst.FM.DownloadFile(filePath)
		if err != nil {
			t.Errorf("Download from instance %d failed: %v", i, err)
			continue
		}
		if len(downloaded) != len(data) {
			t.Errorf("Instance %d: size mismatch, expected %d, got %d", i, len(data), len(downloaded))
			continue
		}
		if string(downloaded) != string(data) {
			t.Errorf("Instance %d: data mismatch", i)
		}
	}
}

func TestRedisLock_GRPCMetadataConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024 * 50)
	filePath := "/redis_grpc_metadata_test.dat"

	_, err := cluster.Instances[0].FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	meta1, err := cluster.Instances[0].FM.GetFileMetadata(filePath)
	if err != nil {
		t.Fatalf("Get metadata from instance 0 failed: %v", err)
	}

	for i, inst := range cluster.Instances[1:] {
		meta, err := inst.FM.GetFileMetadata(filePath)
		if err != nil {
			t.Errorf("Get metadata from instance %d failed: %v", i+1, err)
			continue
		}
		if meta.Path != meta1.Path {
			t.Errorf("Instance %d: Path mismatch", i+1)
		}
		if meta.Size != meta1.Size {
			t.Errorf("Instance %d: Size mismatch", i+1)
		}
		if meta.Hash != meta1.Hash {
			t.Errorf("Instance %d: Hash mismatch", i+1)
		}
	}
}

func TestRedisLock_GRPCDeleteConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024)
	filePath := "/redis_grpc_delete_test.dat"

	_, err := cluster.Instances[0].FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	if err := cluster.Instances[1].FM.DeleteFile(filePath); err != nil {
		t.Fatalf("Delete from instance 1 failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		if inst.FM.Exists(filePath) {
			t.Errorf("Instance %d: file should be deleted", i)
		}
	}
}

func TestRedisLock_GRPCRenameConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data := generateRedisData(1024)
	filePath := "/redis_grpc_rename_test.dat"

	_, err := cluster.Instances[0].FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	if err := cluster.Instances[1].FM.RenameFile(filePath, "redis_grpc_renamed.dat"); err != nil {
		t.Fatalf("Rename from instance 1 failed: %v", err)
	}

	if cluster.Instances[0].FM.Exists(filePath) {
		t.Error("Old path should not exist after rename")
	}

	newPath := "/redis_grpc_renamed.dat"
	for i, inst := range cluster.Instances {
		if !inst.FM.Exists(newPath) {
			t.Errorf("Instance %d: new path should exist after rename", i)
			continue
		}
		downloaded, err := inst.FM.DownloadFile(newPath)
		if err != nil {
			t.Errorf("Instance %d: download from new path failed: %v", i, err)
			continue
		}
		if len(downloaded) != len(data) {
			t.Errorf("Instance %d: size mismatch after rename", i)
		}
	}
}

func TestRedisLock_GRPCConcurrentWriteConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	numFiles := 5
	var wg sync.WaitGroup
	errors := make(chan error, numFiles*len(cluster.Instances))

	for i := 0; i < numFiles; i++ {
		for j, inst := range cluster.Instances {
			wg.Add(1)
			go func(fileIdx, instIdx int, fm *filemanager.FileManager) {
				defer wg.Done()
				data := generateRedisData(1024 * (10 + fileIdx))
				filePath := fmt.Sprintf("/redis_grpc_concurrent_%d_%d.dat", instIdx, fileIdx)
				var lastErr error
				for retry := 0; retry < 10; retry++ {
					_, lastErr = fm.UploadFile(filePath, data)
					if lastErr == nil {
						return
					}
					if strings.Contains(lastErr.Error(), "SQLITE_BUSY") || strings.Contains(lastErr.Error(), "database is locked") {
						time.Sleep(time.Duration(100*(retry+1)) * time.Millisecond)
						continue
					}
					break
				}
				errors <- fmt.Errorf("upload instance %d file %d: %v", instIdx, fileIdx, lastErr)
			}(i, j, inst.FM)
		}
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent write error: %v", err)
	}

	for i := 0; i < numFiles; i++ {
		for j := range cluster.Instances {
			filePath := fmt.Sprintf("/redis_grpc_concurrent_%d_%d.dat", j, i)
			for k, inst := range cluster.Instances {
				_, err := inst.FM.DownloadFile(filePath)
				if err != nil {
					t.Errorf("Instance %d: cannot download file %s: %v", k, filePath, err)
				}
			}
		}
	}
}

func TestRedisLock_GRPCConcurrentOverwriteConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	data1 := generateRedisData(1024)
	data2 := generateRedisData(1024)
	data3 := generateRedisData(1024)
	filePath := "/redis_grpc_overwrite_test.dat"

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); cluster.Instances[0].FM.UploadFile(filePath, data1) }()
	go func() { defer wg.Done(); cluster.Instances[1].FM.UploadFile(filePath, data2) }()
	go func() { defer wg.Done(); cluster.Instances[2].FM.UploadFile(filePath, data3) }()
	wg.Wait()

	downloaded1, err1 := cluster.Instances[0].FM.DownloadFile(filePath)
	downloaded2, err2 := cluster.Instances[1].FM.DownloadFile(filePath)
	downloaded3, err3 := cluster.Instances[2].FM.DownloadFile(filePath)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("Download failed: err0=%v, err1=%v, err2=%v", err1, err2, err3)
	}

	d1 := string(downloaded1)
	d2 := string(downloaded2)
	d3 := string(downloaded3)

	if d1 != d2 || d2 != d3 {
		t.Errorf("Data inconsistency after concurrent overwrites: d0==d1=%v, d1==d2=%v, d0==d2=%v",
			d1 == d2, d2 == d3, d1 == d3)
	}

	validData := map[string]bool{string(data1): true, string(data2): true, string(data3): true}
	if !validData[d1] {
		t.Error("Downloaded data does not match any uploaded version")
	}
}

func TestRedisLock_GRPCStreamingUploadWithRedisSession(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateRedisData(1024 * 50)
	hash := computeRedisHash(data)
	filePath := "/redis_grpc_stream_test.dat"
	chunkSize := int64(1024 * 10)

	sessionID, err := inst1.TransferSvc.CreateUploadSession(filePath, "redis_grpc_stream_test.dat", int64(len(data)), "test", hash)
	if err != nil {
		t.Fatalf("Create upload session on instance 0 failed: %v", err)
	}

	err = inst2.TransferSvc.UploadChunk(sessionID, data[:1024], 0)
	if err != nil {
		t.Logf("Cross-instance chunk upload failed (expected - temp file not shared): %v", err)
	} else {
		t.Log("Cross-instance chunk upload succeeded (Redis session store is working)")
	}

	var offset int64
	for offset < int64(len(data)) {
		end := offset + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[offset:end]
		if err := inst1.TransferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("Same-instance chunk upload at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if _, err := inst1.TransferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("Complete upload failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		downloaded, err := inst.FM.DownloadFile(filePath)
		if err != nil {
			t.Errorf("Instance %d: download failed: %v", i, err)
			continue
		}
		if computeRedisHash(downloaded) != hash {
			t.Errorf("Instance %d: hash mismatch", i)
		}
	}
}

func TestRedisLock_GRPCStreamingDownloadConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateRedisData(1024 * 50)
	filePath := "/redis_grpc_stream_dl_test.dat"

	_, err := inst1.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	sessionID1, err := inst1.TransferSvc.CreateDownloadSession(filePath, "test")
	if err != nil {
		t.Fatalf("Create download session on instance 0 failed: %v", err)
	}

	sessionID2, err := inst2.TransferSvc.CreateDownloadSession(filePath, "test")
	if err != nil {
		t.Fatalf("Create download session on instance 1 failed: %v", err)
	}

	chunkSize := 1024 * 10
	var downloaded1, downloaded2 []byte
	var dlOffset int64

	for dlOffset < int64(len(data)) {
		remaining := int64(len(data)) - dlOffset
		readSize := chunkSize
		if remaining < int64(chunkSize) {
			readSize = int(remaining)
		}

		chunk1, err := inst1.TransferSvc.DownloadChunk(sessionID1, readSize, dlOffset)
		if err != nil {
			t.Fatalf("Download chunk from instance 0 at offset %d failed: %v", dlOffset, err)
		}
		downloaded1 = append(downloaded1, chunk1...)

		chunk2, err := inst2.TransferSvc.DownloadChunk(sessionID2, readSize, dlOffset)
		if err != nil {
			t.Fatalf("Download chunk from instance 1 at offset %d failed: %v", dlOffset, err)
		}
		downloaded2 = append(downloaded2, chunk2...)

		dlOffset += int64(readSize)
	}

	inst1.TransferSvc.CompleteDownload(sessionID1)
	inst2.TransferSvc.CompleteDownload(sessionID2)

	if string(downloaded1) != string(data) {
		t.Error("Instance 0: downloaded data does not match original")
	}
	if string(downloaded2) != string(data) {
		t.Error("Instance 1: downloaded data does not match original")
	}
	if string(downloaded1) != string(downloaded2) {
		t.Error("Downloaded data from two instances should be identical")
	}
}

func TestRedisLock_DistributedLockMutualExclusion(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	distLock := cluster.RedisManager.GetLock()
	ctx := context.Background()

	token1, err := distLock.Lock(ctx, "test_mutex", 10*time.Second)
	if err != nil {
		t.Fatalf("First lock acquisition failed: %v", err)
	}

	_, err = distLock.Lock(ctx, "test_mutex", 10*time.Second)
	if err == nil {
		t.Error("Second lock acquisition should fail (mutual exclusion)")
	} else if err != distributed.ErrLockConflict {
		t.Errorf("Expected ErrLockConflict, got: %v", err)
	}

	if err := distLock.Unlock(ctx, "test_mutex", token1); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	token2, err := distLock.Lock(ctx, "test_mutex", 10*time.Second)
	if err != nil {
		t.Fatalf("Lock acquisition after unlock failed: %v", err)
	}
	distLock.Unlock(ctx, "test_mutex", token2)
}

func TestRedisLock_DistributedLockWithRetry(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	distLock := cluster.RedisManager.GetLock()
	ctx := context.Background()

	token1, err := distLock.Lock(ctx, "test_retry", 5*time.Second)
	if err != nil {
		t.Fatalf("First lock acquisition failed: %v", err)
	}

	acquired := make(chan string, 1)
	go func() {
		token, err := distributed.AcquireLock(ctx, distLock, "test_retry", 5*time.Second, 20, 200*time.Millisecond)
		if err != nil {
			acquired <- ""
			return
		}
		acquired <- token
	}()

	time.Sleep(500 * time.Millisecond)
	distLock.Unlock(ctx, "test_retry", token1)

	select {
	case token2 := <-acquired:
		if token2 == "" {
			t.Error("Retry lock acquisition failed")
		} else {
			t.Log("Retry lock acquisition succeeded after unlock")
			distLock.Unlock(ctx, "test_retry", token2)
		}
	case <-time.After(10 * time.Second):
		t.Error("Retry lock acquisition timed out")
	}
}

func TestRedisLock_GRPCListFilesConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	numFilesPerInst := 5
	for i := 0; i < numFilesPerInst; i++ {
		data := generateRedisData(100)
		filePath := fmt.Sprintf("/redis_grpc_list_%d.dat", i)
		if _, err := cluster.Instances[0].FM.UploadFile(filePath, data); err != nil {
			t.Fatalf("Upload file %d to instance 0 failed: %v", i, err)
		}
	}
	for i := 0; i < numFilesPerInst; i++ {
		data := generateRedisData(100)
		filePath := fmt.Sprintf("/redis_grpc_list_%d.dat", numFilesPerInst+i)
		if _, err := cluster.Instances[1].FM.UploadFile(filePath, data); err != nil {
			t.Fatalf("Upload file %d to instance 1 failed: %v", numFilesPerInst+i, err)
		}
	}

	for i, inst := range cluster.Instances {
		result, err := inst.FlSvc.ListFiles("/", false, 1, 100, "name", "asc")
		if err != nil {
			t.Errorf("List files from instance %d failed: %v", i, err)
			continue
		}
		if result.Total != numFilesPerInst*2 {
			t.Errorf("Instance %d: expected %d files, got %d", i, numFilesPerInst*2, result.Total)
		}
	}
}

func TestRedisLock_HTTPListFilesConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	numFilesPerInst := 5
	for i := 0; i < numFilesPerInst; i++ {
		data := generateRedisData(100)
		filePath := fmt.Sprintf("/redis_list_%d.dat", i)
		resp, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, fmt.Sprintf("list_%d.dat", i), data)
		resp.Body.Close()
	}
	for i := 0; i < numFilesPerInst; i++ {
		data := generateRedisData(100)
		filePath := fmt.Sprintf("/redis_list_%d.dat", numFilesPerInst+i)
		resp, _ := uploadRedisHTTP(cluster.Instances[1].BaseURL, filePath, fmt.Sprintf("list_%d.dat", numFilesPerInst+i), data)
		resp.Body.Close()
	}

	for i, inst := range cluster.Instances {
		result, status, _ := listRedisFilesHTTP(inst.BaseURL)
		if status != http.StatusOK {
			t.Errorf("Instance %d: list files returned status %d", i, status)
			continue
		}
		total := result["Total"].(float64)
		if int(total) != numFilesPerInst*2 {
			t.Errorf("Instance %d: expected %d files, got %d", i, numFilesPerInst*2, int(total))
		}
	}
}

func TestRedisLock_DeleteThenVerifyConsistency(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	for i := 0; i < 5; i++ {
		data := generateRedisData(100)
		filePath := fmt.Sprintf("/redis_del_%d.dat", i)
		resp, _ := uploadRedisHTTP(cluster.Instances[0].BaseURL, filePath, fmt.Sprintf("del_%d.dat", i), data)
		resp.Body.Close()
	}

	deleteRedisHTTP(cluster.Instances[1].BaseURL, "/redis_del_2.dat")
	deleteRedisHTTP(cluster.Instances[2].BaseURL, "/redis_del_4.dat")

	for i, inst := range cluster.Instances {
		result, status, _ := listRedisFilesHTTP(inst.BaseURL)
		if status != http.StatusOK {
			t.Errorf("Instance %d: list files after delete failed", i)
			continue
		}
		total := result["Total"].(float64)
		if int(total) != 3 {
			t.Errorf("Instance %d: expected 3 files after delete, got %d", i, int(total))
		}
	}

	deletedFiles := []string{"/redis_del_2.dat", "/redis_del_4.dat"}
	for _, f := range deletedFiles {
		for i, inst := range cluster.Instances {
			_, status, _ := downloadRedisHTTP(inst.BaseURL, f)
			if status != http.StatusNotFound {
				t.Errorf("Instance %d: deleted file %s should return 404, got %d", i, f, status)
			}
		}
	}

	existingFiles := []string{"/redis_del_0.dat", "/redis_del_1.dat", "/redis_del_3.dat"}
	for _, f := range existingFiles {
		for i, inst := range cluster.Instances {
			_, status, _ := downloadRedisHTTP(inst.BaseURL, f)
			if status != http.StatusOK {
				t.Errorf("Instance %d: existing file %s should return 200, got %d", i, f, status)
			}
		}
	}
}

func TestRedisLock_GRPCStreamingUploadWithHashVerification(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]

	data := generateRedisData(1024 * 50)
	hash := computeRedisHash(data)
	filePath := "/redis_grpc_stream_hash_test.dat"
	chunkSize := int64(1024 * 10)

	sessionID, err := inst1.TransferSvc.CreateUploadSession(filePath, "redis_grpc_stream_hash_test.dat", int64(len(data)), "test", hash)
	if err != nil {
		t.Fatalf("Create upload session failed: %v", err)
	}

	var offset int64
	for offset < int64(len(data)) {
		end := offset + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[offset:end]
		if err := inst1.TransferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("Chunk upload at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if _, err := inst1.TransferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("Complete upload with hash verification failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		downloaded, err := inst.FM.DownloadFile(filePath)
		if err != nil {
			t.Errorf("Instance %d: download failed: %v", i, err)
			continue
		}
		actualHash := computeRedisHash(downloaded)
		if actualHash != hash {
			t.Errorf("Instance %d: hash mismatch, expected %s, got %s", i, hash[:8], actualHash[:8])
		}
	}
}

func TestRedisLock_CrossInstanceStreamingUploadDownload(t *testing.T) {
	cluster := NewRedisCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	data := generateRedisData(1024 * 30)
	hash := computeRedisHash(data)
	filePath := "/redis_cross_stream_test.dat"
	chunkSize := int64(1024 * 5)

	sessionID, err := inst1.TransferSvc.CreateUploadSession(filePath, "redis_cross_stream_test.dat", int64(len(data)), "test", hash)
	if err != nil {
		t.Fatalf("Create upload session on instance 0 failed: %v", err)
	}

	var offset int64
	for offset < int64(len(data)) {
		end := offset + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[offset:end]
		if err := inst1.TransferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("Chunk upload at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if _, err := inst1.TransferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("Complete upload failed: %v", err)
	}

	for i, inst := range []*RedisInstance{inst2, inst3} {
		dlSessionID, err := inst.TransferSvc.CreateDownloadSession(filePath, "test")
		if err != nil {
			t.Errorf("Instance %d: create download session failed: %v", i+1, err)
			continue
		}

		var downloaded []byte
		var dlOffset int64
		for dlOffset < int64(len(data)) {
			remaining := int64(len(data)) - dlOffset
			readSize := int(chunkSize)
			if remaining < chunkSize {
				readSize = int(remaining)
			}
			chunk, err := inst.TransferSvc.DownloadChunk(dlSessionID, readSize, dlOffset)
			if err != nil {
				t.Errorf("Instance %d: download chunk at offset %d failed: %v", i+1, dlOffset, err)
				break
			}
			downloaded = append(downloaded, chunk...)
			dlOffset += int64(readSize)
		}

		inst.TransferSvc.CompleteDownload(dlSessionID)

		if len(downloaded) != len(data) {
			t.Errorf("Instance %d: size mismatch, expected %d, got %d", i+1, len(data), len(downloaded))
			continue
		}
		actualHash := computeRedisHash(downloaded)
		if actualHash != hash {
			t.Errorf("Instance %d: hash mismatch, expected %s, got %s", i+1, hash[:8], actualHash[:8])
		}
	}
}
