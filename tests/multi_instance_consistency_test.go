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
	"strconv"
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
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type MultiInstanceCluster struct {
	Instances   []*Instance
	StorageDir  string
	DBPath      string
	TempDir     string
	SharedDB    *database.DB
	SharedStore storage.StorageAdapter
}

type Instance struct {
	ID          int
	BaseURL     string
	Server      *httpserver.Server
	Listener    net.Listener
	AuthSvc     *auth.AuthService
	CacheSvc    cache.CacheAdapter
	TransferSvc *transfer.FileTransferService
	FM          *filemanager.FileManager
	DirSvc      *directory.DirectoryManager
	FlSvc       *filelist.FileListService
}

func NewMultiInstanceCluster(t *testing.T, numInstances int) *MultiInstanceCluster {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "fsserver-multi-*")
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

	_ = logger.Initialize("", "error")

	cluster := &MultiInstanceCluster{
		StorageDir:  storageDir,
		DBPath:      dbPath,
		TempDir:     tempDir,
		SharedDB:    qdb,
		SharedStore: store,
	}

	for i := 0; i < numInstances; i++ {
		inst := createInstance(t, i, qdb, store)
		cluster.Instances = append(cluster.Instances, inst)
	}

	return cluster
}

func createInstance(t *testing.T, id int, db *database.DB, store storage.StorageAdapter) *Instance {
	t.Helper()

	fm := filemanager.NewFileManager(store, db)
	dirSvc := directory.NewDirectoryManager(db)
	flSvc := filelist.NewFileListService(db)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferService(store, db)

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

	return &Instance{
		ID:          id,
		BaseURL:     baseURL,
		Server:      srv,
		AuthSvc:     authSvc,
		CacheSvc:    cacheSvc,
		TransferSvc: transferSvc,
		FM:          fm,
		DirSvc:      dirSvc,
		FlSvc:       flSvc,
	}
}

func (c *MultiInstanceCluster) Cleanup() {
	for _, inst := range c.Instances {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		inst.Server.Shutdown(ctx)
		cancel()
	}
	c.SharedDB.Close()
	os.RemoveAll(c.TempDir)
}

func uploadHTTP(baseURL, filePath, fileName string, data []byte) (*http.Response, error) {
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

func downloadHTTP(baseURL, filePath string) ([]byte, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/files" + filePath)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return data, resp.StatusCode, nil
}

func deleteHTTP(baseURL, filePath string) error {
	req, _ := http.NewRequest("DELETE", baseURL+"/api/v1/files"+filePath, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func getMetadataHTTP(baseURL, filePath string) (map[string]interface{}, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/metadata" + filePath)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode, nil
}

func listFilesHTTP(baseURL string) (map[string]interface{}, int, error) {
	resp, err := http.Get(baseURL + "/api/v1/files")
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode, nil
}

func createDirectoryHTTP(baseURL, dirPath string) (int, error) {
	body := fmt.Sprintf(`{"path":"%s"}`, dirPath)
	resp, err := http.Post(baseURL+"/api/v1/directories", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func renameHTTP(baseURL, oldPath, newName string) (int, string, error) {
	body := fmt.Sprintf(`{"new_name":"%s"}`, newName)
	req, _ := http.NewRequest("PATCH", baseURL+"/api/v1/files"+oldPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respBody), nil
}

func generateData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func computeHash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

func TestMultiInstance_HTTPUploadDownloadConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]

	data := generateData(1024 * 100)
	expectedHash := computeHash(data)
	filePath := "/consistency_test.dat"

	resp, err := uploadHTTP(inst1.BaseURL, filePath, "consistency_test.dat", data)
	if err != nil {
		t.Fatalf("Upload to instance 1 failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Upload to instance 1 returned status %d", resp.StatusCode)
	}

	for i, inst := range cluster.Instances {
		downloaded, status, err := downloadHTTP(inst.BaseURL, filePath)
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
		actualHash := computeHash(downloaded)
		if actualHash != expectedHash {
			t.Errorf("Instance %d: hash mismatch, expected %s, got %s", i, expectedHash, actualHash)
		}
	}
}

func TestMultiInstance_HTTPMetadataConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	data := generateData(1024 * 50)
	filePath := "/metadata_test.dat"

	resp, err := uploadHTTP(inst1.BaseURL, filePath, "metadata_test.dat", data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Upload returned status %d", resp.StatusCode)
	}

	meta1, status1, err := getMetadataHTTP(inst1.BaseURL, filePath)
	if err != nil || status1 != http.StatusOK {
		t.Fatalf("Get metadata from instance 1 failed: status=%d, err=%v", status1, err)
	}
	meta1Inner, ok := meta1["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("Metadata response from instance 1 missing 'metadata' field: %v", meta1)
	}

	for i, inst := range []*Instance{inst2, inst3} {
		meta, status, err := getMetadataHTTP(inst.BaseURL, filePath)
		if err != nil {
			t.Errorf("Get metadata from instance %d failed: %v", i+1, err)
			continue
		}
		if status != http.StatusOK {
			t.Errorf("Get metadata from instance %d returned status %d", i+1, status)
			continue
		}

		metaInner, ok := meta["metadata"].(map[string]interface{})
		if !ok {
			t.Errorf("Instance %d: metadata response missing 'metadata' field: %v", i+1, meta)
			continue
		}

		if metaInner["Path"] != meta1Inner["Path"] {
			t.Errorf("Instance %d: Path mismatch, expected %v, got %v", i+1, meta1Inner["Path"], metaInner["Path"])
		}
		if metaInner["Size"] != meta1Inner["Size"] {
			t.Errorf("Instance %d: Size mismatch, expected %v, got %v", i+1, meta1Inner["Size"], metaInner["Size"])
		}
		if metaInner["Hash"] != meta1Inner["Hash"] {
			t.Errorf("Instance %d: Hash mismatch, expected %v, got %v", i+1, meta1Inner["Hash"], metaInner["Hash"])
		}
		if metaInner["Name"] != meta1Inner["Name"] {
			t.Errorf("Instance %d: Name mismatch, expected %v, got %v", i+1, meta1Inner["Name"], metaInner["Name"])
		}
	}
}

func TestMultiInstance_HTTPDeleteConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateData(1024)
	filePath := "/delete_test.dat"

	resp, err := uploadHTTP(inst1.BaseURL, filePath, "delete_test.dat", data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	resp.Body.Close()

	for _, inst := range cluster.Instances {
		_, status, _ := downloadHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Fatalf("File should exist on all instances before delete, got status %d", status)
		}
	}

	if err := deleteHTTP(inst2.BaseURL, filePath); err != nil {
		t.Fatalf("Delete from instance 2 failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		_, status, _ := downloadHTTP(inst.BaseURL, filePath)
		if status != http.StatusNotFound {
			t.Errorf("Instance %d: file should be deleted, got status %d", i, status)
		}
	}

	for i, inst := range cluster.Instances {
		_, status, _ := getMetadataHTTP(inst.BaseURL, filePath)
		if status != http.StatusNotFound {
			t.Errorf("Instance %d: metadata should be deleted, got status %d", i, status)
		}
	}
}

func TestMultiInstance_HTTPRenameConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateData(1024)
	filePath := "/rename_test.dat"

	resp, err := uploadHTTP(inst1.BaseURL, filePath, "rename_test.dat", data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	status, respBody, err := renameHTTP(inst2.BaseURL, filePath, "renamed_test.dat")
	if err != nil {
		t.Fatalf("Rename from instance 2 failed: %v", err)
	}
	if status != http.StatusOK {
		t.Logf("First rename attempt returned status %d: %s, retrying...", status, respBody)
		time.Sleep(500 * time.Millisecond)
		status, respBody, err = renameHTTP(inst2.BaseURL, filePath, "renamed_test.dat")
		if err != nil {
			t.Fatalf("Rename retry from instance 2 failed: %v", err)
		}
		if status != http.StatusOK {
			newPath := "/renamed_test.dat"
			_, checkStatus, _ := downloadHTTP(inst1.BaseURL, newPath)
			if checkStatus == http.StatusOK {
				t.Logf("Rename succeeded despite status %d (file found at new path)", status)
			} else {
				_, oldStatus, _ := downloadHTTP(inst1.BaseURL, filePath)
				t.Fatalf("Rename returned status %d (%s), old path status=%d, new path status=%d", status, respBody, oldStatus, checkStatus)
			}
		}
	}

	_, status, _ = downloadHTTP(inst1.BaseURL, filePath)
	if status != http.StatusNotFound {
		t.Errorf("Old path should not exist after rename, got status %d", status)
	}

	meta, status, _ := getMetadataHTTP(inst1.BaseURL, filePath)
	if status != http.StatusNotFound {
		t.Errorf("Old metadata should not exist after rename, got status %d, meta=%v", status, meta)
	}

	newPath := "/renamed_test.dat"
	downloaded, status, _ := downloadHTTP(inst2.BaseURL, newPath)
	if status != http.StatusOK {
		t.Errorf("New path should be downloadable, got status %d", status)
	} else if computeHash(downloaded) != computeHash(data) {
		t.Errorf("Downloaded data hash mismatch after rename")
	}

	meta, status, _ = getMetadataHTTP(inst2.BaseURL, newPath)
	if status != http.StatusOK {
		t.Errorf("New metadata should exist, got status %d", status)
	} else {
		metaInner, ok := meta["metadata"].(map[string]interface{})
		if !ok {
			t.Errorf("Metadata response missing 'metadata' field: %v", meta)
		} else if metaInner["Name"] != "renamed_test.dat" {
			t.Errorf("Metadata name should be 'renamed_test.dat', got %v", metaInner["Name"])
		}
	}
}

func TestMultiInstance_HTTPDirectoryConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	dirPath := "/testdir"
	status, err := createDirectoryHTTP(inst1.BaseURL, dirPath)
	if err != nil {
		t.Fatalf("Create directory failed: %v", err)
	}
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("Create directory returned status %d", status)
	}

	data := generateData(1024)
	filePath := "/testdir/file_in_dir.dat"
	resp, err := uploadHTTP(inst2.BaseURL, filePath, "file_in_dir.dat", data)
	if err != nil {
		t.Fatalf("Upload to directory failed: %v", err)
	}
	resp.Body.Close()

	for i, inst := range cluster.Instances {
		downloaded, status, _ := downloadHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: file in directory should be downloadable, got status %d", i, status)
			continue
		}
		if computeHash(downloaded) != computeHash(data) {
			t.Errorf("Instance %d: data hash mismatch for file in directory", i)
		}
	}
}

func uploadHTTPWithRetry(baseURL, filePath, fileName string, data []byte, maxRetries int) (*http.Response, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		resp, err := uploadHTTP(baseURL, filePath, fileName, data)
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

func TestMultiInstance_HTTPConcurrentWriteConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	numFiles := 5
	var wg sync.WaitGroup
	errors := make(chan error, numFiles*len(cluster.Instances))

	for i := 0; i < numFiles; i++ {
		for j, inst := range cluster.Instances {
			wg.Add(1)
			go func(fileIdx, instIdx int, baseURL string) {
				defer wg.Done()
				data := generateData(1024 * (10 + fileIdx))
				filePath := fmt.Sprintf("/concurrent_%d_%d.dat", instIdx, fileIdx)
				resp, err := uploadHTTPWithRetry(baseURL, filePath, fmt.Sprintf("file_%d_%d.dat", instIdx, fileIdx), data, 5)
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
			filePath := fmt.Sprintf("/concurrent_%d_%d.dat", j, i)
			for k, inst := range cluster.Instances {
				_, status, _ := downloadHTTP(inst.BaseURL, filePath)
				if status != http.StatusOK {
					t.Errorf("Instance %d: cannot download file %s uploaded by instance %d, status %d", k, filePath, j, status)
				}
			}
		}
	}
}

func TestMultiInstance_HTTPStreamingUploadSessionIsolation(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateData(1024 * 100)
	hash := computeHash(data)
	filePath := "/stream_session_test.dat"

	body := fmt.Sprintf(`{"file_path":"%s","file_name":"stream_session_test.dat","total_size":%d,"hash":"%s"}`,
		filePath, len(data), hash)
	resp, err := http.Post(inst1.BaseURL+"/api/v1/uploads", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusCreated {
		resp.Body.Close()
		t.Fatalf("Create upload session on instance 1 failed: status=%d, err=%v", resp.StatusCode, err)
	}

	var sessionResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&sessionResp)
	resp.Body.Close()

	sessionID, ok := sessionResp["session_id"].(string)
	if !ok || sessionID == "" {
		t.Fatal("No session_id in response")
	}

	chunkURL := inst2.BaseURL + "/api/v1/uploads/" + sessionID + "/chunk"
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, _ := writer.CreateFormFile("data", "chunk")
	part.Write(data[:1024])
	writer.WriteField("offset", "0")
	writer.Close()

	req, _ := http.NewRequest("PUT", chunkURL, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	chunkResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("Cross-instance chunk upload returned error (expected): %v", err)
	} else {
		body, _ := io.ReadAll(chunkResp.Body)
		chunkResp.Body.Close()
		if chunkResp.StatusCode == http.StatusOK {
			t.Error("Cross-instance chunk upload should fail because sessions are local to each instance")
		} else {
			t.Logf("Cross-instance chunk upload correctly failed with status %d: %s", chunkResp.StatusCode, string(body))
		}
	}

	chunkURL1 := inst1.BaseURL + "/api/v1/uploads/" + sessionID + "/chunk"
	var buf2 bytes.Buffer
	writer2 := multipart.NewWriter(&buf2)
	part2, _ := writer2.CreateFormFile("data", "chunk")
	part2.Write(data)
	writer2.WriteField("offset", "0")
	writer2.Close()

	req2, _ := http.NewRequest("PUT", chunkURL1, &buf2)
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
		downloaded, status, _ := downloadHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: completed streaming upload should be downloadable, got status %d", i, status)
			continue
		}
		if computeHash(downloaded) != hash {
			t.Errorf("Instance %d: downloaded data hash mismatch", i)
		}
	}
}

func TestMultiInstance_HTTPStreamingDownloadSessionIsolation(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateData(1024 * 100)
	expectedHash := computeHash(data)
	filePath := "/stream_dl_session_test.dat"

	resp, err := uploadHTTP(inst1.BaseURL, filePath, "stream_dl_session_test.dat", data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	resp.Body.Close()

	req1, _ := http.NewRequest("GET", inst1.BaseURL+"/api/v1/files"+filePath, nil)
	req1.Header.Set("Range", "bytes=0-1023")
	rangeResp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("Range download from instance 1 failed: %v", err)
	}
	chunk1, _ := io.ReadAll(rangeResp1.Body)
	rangeResp1.Body.Close()
	if rangeResp1.StatusCode != http.StatusPartialContent && rangeResp1.StatusCode != http.StatusOK {
		t.Fatalf("Range download from instance 1 returned status %d", rangeResp1.StatusCode)
	}

	req2, _ := http.NewRequest("GET", inst2.BaseURL+"/api/v1/files"+filePath, nil)
	req2.Header.Set("Range", "bytes=0-1023")
	rangeResp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("Range download from instance 2 failed: %v", err)
	}
	chunk2, _ := io.ReadAll(rangeResp2.Body)
	rangeResp2.Body.Close()
	if rangeResp2.StatusCode != http.StatusPartialContent && rangeResp2.StatusCode != http.StatusOK {
		t.Fatalf("Range download from instance 2 returned status %d", rangeResp2.StatusCode)
	}

	if string(chunk1) != string(chunk2) {
		t.Error("Range download data should be consistent across instances")
	}

	fullData1, status1, _ := downloadHTTP(inst1.BaseURL, filePath)
	fullData2, status2, _ := downloadHTTP(inst2.BaseURL, filePath)
	if status1 != http.StatusOK || status2 != http.StatusOK {
		t.Fatalf("Full download failed: inst1 status=%d, inst2 status=%d", status1, status2)
	}
	if computeHash(fullData1) != expectedHash || computeHash(fullData2) != expectedHash {
		t.Error("Full download data hash mismatch across instances")
	}
}

func TestMultiInstance_HTTPConcurrentOverwriteConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	data1 := generateData(1024)
	data2 := generateData(1024)
	data3 := generateData(1024)
	filePath := "/overwrite_test.dat"

	resp1, _ := uploadHTTP(inst1.BaseURL, filePath, "overwrite_test.dat", data1)
	resp1.Body.Close()

	resp2, _ := uploadHTTP(inst2.BaseURL, filePath, "overwrite_test.dat", data2)
	resp2.Body.Close()

	resp3, _ := uploadHTTP(inst3.BaseURL, filePath, "overwrite_test.dat", data3)
	resp3.Body.Close()

	time.Sleep(100 * time.Millisecond)

	downloaded1, status1, _ := downloadHTTP(inst1.BaseURL, filePath)
	downloaded2, status2, _ := downloadHTTP(inst2.BaseURL, filePath)
	downloaded3, status3, _ := downloadHTTP(inst3.BaseURL, filePath)

	if status1 != http.StatusOK || status2 != http.StatusOK || status3 != http.StatusOK {
		t.Fatalf("Download status mismatch: inst1=%d, inst2=%d, inst3=%d", status1, status2, status3)
	}

	hash1 := computeHash(downloaded1)
	hash2 := computeHash(downloaded2)
	hash3 := computeHash(downloaded3)

	if hash1 != hash2 || hash2 != hash3 {
		t.Errorf("Data inconsistency after concurrent overwrites: hash1=%s, hash2=%s, hash3=%s", hash1[:8], hash2[:8], hash3[:8])
	}

	validHashes := map[string]bool{
		computeHash(data1): true,
		computeHash(data2): true,
		computeHash(data3): true,
	}
	if !validHashes[hash1] {
		t.Errorf("Downloaded data does not match any uploaded version: hash=%s", hash1[:8])
	}
}

func TestMultiInstance_HTTPLargeFileConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	sizes := []struct {
		size int
		name string
	}{
		{1 * 1024, "1KB"},
		{64 * 1024, "64KB"},
		{1024 * 1024, "1MB"},
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			data := generateData(tc.size)
			expectedHash := computeHash(data)
			filePath := fmt.Sprintf("/large_%s.dat", tc.name)

			resp, err := uploadHTTP(inst1.BaseURL, filePath, fmt.Sprintf("large_%s.dat", tc.name), data)
			if err != nil {
				t.Fatalf("Upload failed: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("Upload returned status %d", resp.StatusCode)
			}

			downloaded, status, _ := downloadHTTP(inst2.BaseURL, filePath)
			if status != http.StatusOK {
				t.Fatalf("Download from instance 2 returned status %d", status)
			}
			if len(downloaded) != len(data) {
				t.Fatalf("Size mismatch: expected %d, got %d", len(data), len(downloaded))
			}
			if computeHash(downloaded) != expectedHash {
				t.Fatalf("Hash mismatch for %s file", tc.name)
			}
		})
	}
}

func TestMultiInstance_HTTPStreamingUploadCompleteConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]

	data := generateData(1024 * 50)
	hash := computeHash(data)
	filePath := "/stream_complete_test.dat"
	chunkSize := 1024 * 10

	body := fmt.Sprintf(`{"file_path":"%s","file_name":"stream_complete_test.dat","total_size":%d,"hash":"%s"}`,
		filePath, len(data), hash)
	resp, err := http.Post(inst1.BaseURL+"/api/v1/uploads", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusCreated {
		resp.Body.Close()
		t.Fatalf("Create session failed: status=%d, err=%v", resp.StatusCode, err)
	}
	var sessionResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&sessionResp)
	resp.Body.Close()
	sessionID := sessionResp["session_id"].(string)

	var offset int64
	for offset < int64(len(data)) {
		end := offset + int64(chunkSize)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[offset:end]

		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)
		part, _ := writer.CreateFormFile("data", "chunk")
		part.Write(chunk)
		writer.WriteField("offset", strconv.FormatInt(offset, 10))
		writer.Close()

		req, _ := http.NewRequest("PUT", inst1.BaseURL+"/api/v1/uploads/"+sessionID+"/chunk", &buf)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		chunkResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Chunk upload failed at offset %d: %v", offset, err)
		}
		chunkResp.Body.Close()
		if chunkResp.StatusCode != http.StatusOK {
			t.Fatalf("Chunk upload at offset %d returned status %d", offset, chunkResp.StatusCode)
		}
		offset = end
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
		downloaded, status, _ := downloadHTTP(inst.BaseURL, filePath)
		if status != http.StatusOK {
			t.Errorf("Instance %d: cannot download streaming uploaded file, status %d", i, status)
			continue
		}
		if computeHash(downloaded) != hash {
			t.Errorf("Instance %d: streaming uploaded file hash mismatch", i)
		}
	}
}

func TestMultiInstance_HTTPListFilesConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	numFiles := 5
	for i := 0; i < numFiles; i++ {
		data := generateData(100)
		filePath := fmt.Sprintf("/list_test_%d.dat", i)
		resp, err := uploadHTTP(inst1.BaseURL, filePath, fmt.Sprintf("list_%d.dat", i), data)
		if err != nil {
			t.Fatalf("Upload file %d failed: %v", i, err)
		}
		resp.Body.Close()
	}

	for i := 0; i < numFiles; i++ {
		data := generateData(100)
		filePath := fmt.Sprintf("/list_test_%d.dat", numFiles+i)
		resp, err := uploadHTTP(inst2.BaseURL, filePath, fmt.Sprintf("list_%d.dat", numFiles+i), data)
		if err != nil {
			t.Fatalf("Upload file %d to instance 2 failed: %v", numFiles+i, err)
		}
		resp.Body.Close()
	}

	for i, inst := range cluster.Instances {
		result, status, err := listFilesHTTP(inst.BaseURL)
		if err != nil {
			t.Errorf("List files from instance %d failed: %v", i, err)
			continue
		}
		if status != http.StatusOK {
			t.Errorf("List files from instance %d returned status %d", i, status)
			continue
		}

		total, ok := result["Total"].(float64)
		if !ok {
			t.Errorf("Instance %d: Total field missing or wrong type", i)
			continue
		}
		if int(total) != numFiles*2 {
			t.Errorf("Instance %d: expected %d files, got %d", i, numFiles*2, int(total))
		}
	}
}

func TestMultiInstance_HTTPDeleteThenVerifyConsistency(t *testing.T) {
	cluster := NewMultiInstanceCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	for i := 0; i < 5; i++ {
		data := generateData(100)
		filePath := fmt.Sprintf("/del_verify_%d.dat", i)
		resp, _ := uploadHTTP(inst1.BaseURL, filePath, fmt.Sprintf("del_%d.dat", i), data)
		resp.Body.Close()
	}

	for i, inst := range cluster.Instances {
		result, status, _ := listFilesHTTP(inst.BaseURL)
		if status != http.StatusOK {
			t.Errorf("Instance %d: list files failed", i)
			continue
		}
		total := result["Total"].(float64)
		if int(total) != 5 {
			t.Errorf("Instance %d: expected 5 files, got %d", i, int(total))
		}
	}

	deleteHTTP(inst2.BaseURL, "/del_verify_2.dat")
	deleteHTTP(inst3.BaseURL, "/del_verify_4.dat")

	for i, inst := range cluster.Instances {
		result, status, _ := listFilesHTTP(inst.BaseURL)
		if status != http.StatusOK {
			t.Errorf("Instance %d: list files after delete failed", i)
			continue
		}
		total := result["Total"].(float64)
		if int(total) != 3 {
			t.Errorf("Instance %d: expected 3 files after delete, got %d", i, int(total))
		}
	}

	deletedFiles := []string{"/del_verify_2.dat", "/del_verify_4.dat"}
	for _, f := range deletedFiles {
		for i, inst := range cluster.Instances {
			_, status, _ := downloadHTTP(inst.BaseURL, f)
			if status != http.StatusNotFound {
				t.Errorf("Instance %d: deleted file %s should return 404, got %d", i, f, status)
			}
		}
	}

	existingFiles := []string{"/del_verify_0.dat", "/del_verify_1.dat", "/del_verify_3.dat"}
	for _, f := range existingFiles {
		for i, inst := range cluster.Instances {
			_, status, _ := downloadHTTP(inst.BaseURL, f)
			if status != http.StatusOK {
				t.Errorf("Instance %d: existing file %s should return 200, got %d", i, f, status)
			}
		}
	}
}
