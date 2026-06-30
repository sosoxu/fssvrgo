package tests

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type GRPCInstance struct {
	ID          int
	FM          *filemanager.FileManager
	DirSvc      *directory.DirectoryManager
	FlSvc       *filelist.FileListService
	TransferSvc *transfer.FileTransferService
}

type GRPCCluster struct {
	Instances   []*GRPCInstance
	StorageDir  string
	DBPath      string
	TempDir     string
	SharedDB    *database.DB
	SharedStore storage.StorageAdapter
	dbObj       *database.Database
}

func NewGRPCCluster(t *testing.T, numInstances int) *GRPCCluster {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "fsserver-grpc-multi-*")
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

	cluster := &GRPCCluster{
		StorageDir:  storageDir,
		DBPath:      dbPath,
		TempDir:     tempDir,
		SharedDB:    qdb,
		SharedStore: store,
		dbObj:       dbObj,
	}

	for i := 0; i < numInstances; i++ {
		fm := filemanager.NewFileManager(store, qdb)
		dirSvc := directory.NewDirectoryManager(qdb)
		flSvc := filelist.NewFileListService(qdb)
		transferSvc := transfer.NewFileTransferService(store, qdb)

		cluster.Instances = append(cluster.Instances, &GRPCInstance{
			ID:          i,
			FM:          fm,
			DirSvc:      dirSvc,
			FlSvc:       flSvc,
			TransferSvc: transferSvc,
		})
	}

	return cluster
}

func (c *GRPCCluster) Cleanup() {
	c.dbObj.Close()
	os.RemoveAll(c.TempDir)
}

func generateGRPCData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func TestMultiInstance_GRPCUploadDownloadConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]

	data := generateGRPCData(1024 * 100)
	filePath := "/grpc_consistency_test.dat"

	_, err := inst1.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload via instance 1 failed: %v", err)
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
		for j := range data {
			if downloaded[j] != data[j] {
				t.Errorf("Instance %d: data mismatch at byte %d", i, j)
				break
			}
		}
	}
}

func TestMultiInstance_GRPCMetadataConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	data := generateGRPCData(1024 * 50)
	filePath := "/grpc_metadata_test.dat"

	_, err := inst1.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	meta1, err := inst1.FM.GetFileMetadata(filePath)
	if err != nil {
		t.Fatalf("Get metadata from instance 1 failed: %v", err)
	}

	for i, inst := range []*GRPCInstance{inst2, inst3} {
		meta, err := inst.FM.GetFileMetadata(filePath)
		if err != nil {
			t.Errorf("Get metadata from instance %d failed: %v", i+1, err)
			continue
		}
		if meta.Path != meta1.Path {
			t.Errorf("Instance %d: Path mismatch, expected %s, got %s", i+1, meta1.Path, meta.Path)
		}
		if meta.Size != meta1.Size {
			t.Errorf("Instance %d: Size mismatch, expected %d, got %d", i+1, meta1.Size, meta.Size)
		}
		if meta.Hash != meta1.Hash {
			t.Errorf("Instance %d: Hash mismatch, expected %s, got %s", i+1, meta1.Hash, meta.Hash)
		}
		if meta.Name != meta1.Name {
			t.Errorf("Instance %d: Name mismatch, expected %s, got %s", i+1, meta1.Name, meta.Name)
		}
	}
}

func TestMultiInstance_GRPCDeleteConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateGRPCData(1024)
	filePath := "/grpc_delete_test.dat"

	_, err := inst1.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	for _, inst := range cluster.Instances {
		if !inst.FM.Exists(filePath) {
			t.Fatalf("File should exist on all instances before delete")
		}
	}

	if err := inst2.FM.DeleteFile(filePath); err != nil {
		t.Fatalf("Delete from instance 2 failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		if inst.FM.Exists(filePath) {
			t.Errorf("Instance %d: file should be deleted", i)
		}

		_, err := inst.FM.DownloadFile(filePath)
		if err == nil {
			t.Errorf("Instance %d: download should fail after delete", i)
		}
	}
}

func TestMultiInstance_GRPCRenameConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateGRPCData(1024)
	filePath := "/grpc_rename_test.dat"

	_, err := inst1.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	if err := inst2.FM.RenameFile(filePath, "grpc_renamed.dat"); err != nil {
		t.Fatalf("Rename from instance 2 failed: %v", err)
	}

	if inst1.FM.Exists(filePath) {
		t.Error("Old path should not exist after rename")
	}

	newPath := "/grpc_renamed.dat"
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

func TestMultiInstance_GRPCDirectoryConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	if err := inst1.DirSvc.CreateDirectory("/grpc_testdir"); err != nil {
		t.Fatalf("Create directory failed: %v", err)
	}

	data := generateGRPCData(1024)
	filePath := "/grpc_testdir/file_in_dir.dat"

	_, err := inst2.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload to directory failed: %v", err)
	}

	for i, inst := range cluster.Instances {
		downloaded, err := inst.FM.DownloadFile(filePath)
		if err != nil {
			t.Errorf("Instance %d: download file in directory failed: %v", i, err)
			continue
		}
		if len(downloaded) != len(data) {
			t.Errorf("Instance %d: size mismatch for file in directory", i)
		}
	}
}

func TestMultiInstance_GRPCConcurrentWriteConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	numFiles := 5
	var wg sync.WaitGroup
	errors := make(chan error, numFiles*len(cluster.Instances))

	for i := 0; i < numFiles; i++ {
		for j, inst := range cluster.Instances {
			wg.Add(1)
			go func(fileIdx, instIdx int, fm *filemanager.FileManager) {
				defer wg.Done()
				data := generateGRPCData(1024 * (10 + fileIdx))
				filePath := fmt.Sprintf("/grpc_concurrent_%d_%d.dat", instIdx, fileIdx)
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
			filePath := fmt.Sprintf("/grpc_concurrent_%d_%d.dat", j, i)
			for k, inst := range cluster.Instances {
				_, err := inst.FM.DownloadFile(filePath)
				if err != nil {
					t.Errorf("Instance %d: cannot download file %s uploaded by instance %d: %v", k, filePath, j, err)
				}
			}
		}
	}
}

func TestMultiInstance_GRPCConcurrentOverwriteConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	data1 := generateGRPCData(1024)
	data2 := generateGRPCData(1024)
	data3 := generateGRPCData(1024)
	filePath := "/grpc_overwrite_test.dat"

	inst1.FM.UploadFile(filePath, data1)
	inst2.FM.UploadFile(filePath, data2)
	inst3.FM.UploadFile(filePath, data3)

	downloaded1, err1 := inst1.FM.DownloadFile(filePath)
	downloaded2, err2 := inst2.FM.DownloadFile(filePath)
	downloaded3, err3 := inst3.FM.DownloadFile(filePath)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("Download failed: err1=%v, err2=%v, err3=%v", err1, err2, err3)
	}

	d1 := string(downloaded1)
	d2 := string(downloaded2)
	d3 := string(downloaded3)

	if d1 != d2 || d2 != d3 {
		t.Errorf("Data inconsistency after concurrent overwrites: data matches: d1==d2=%v, d2==d3=%v, d1==d3=%v",
			d1 == d2, d2 == d3, d1 == d3)
	}

	validData := map[string]bool{
		string(data1): true,
		string(data2): true,
		string(data3): true,
	}
	if !validData[d1] {
		t.Error("Downloaded data does not match any uploaded version")
	}
}

func TestMultiInstance_GRPCStreamingUploadSessionIsolation(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateGRPCData(1024 * 50)
	filePath := "/grpc_stream_session_test.dat"
	chunkSize := int64(1024 * 10)

	sessionID, err := inst1.TransferSvc.CreateUploadSession(filePath, "grpc_stream_session_test.dat", int64(len(data)), "test", "")
	if err != nil {
		t.Fatalf("Create upload session on instance 1 failed: %v", err)
	}

	err = inst2.TransferSvc.UploadChunk(sessionID, data[:1024], 0)
	if err == nil {
		t.Error("Cross-instance chunk upload should fail because sessions are local to each instance")
	} else {
		t.Logf("Cross-instance chunk upload correctly failed: %v", err)
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
			t.Errorf("Instance %d: cannot download streaming uploaded file: %v", i, err)
			continue
		}
		if len(downloaded) != len(data) {
			t.Errorf("Instance %d: size mismatch, expected %d, got %d", i, len(data), len(downloaded))
		}
	}
}

func TestMultiInstance_GRPCStreamingDownloadConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	data := generateGRPCData(1024 * 50)
	filePath := "/grpc_stream_dl_test.dat"

	_, err := inst1.FM.UploadFile(filePath, data)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	sessionID1, err := inst1.TransferSvc.CreateDownloadSession(filePath, "test")
	if err != nil {
		t.Fatalf("Create download session on instance 1 failed: %v", err)
	}

	sessionID2, err := inst2.TransferSvc.CreateDownloadSession(filePath, "test")
	if err != nil {
		t.Fatalf("Create download session on instance 2 failed: %v", err)
	}

	chunkSize := 1024 * 10
	var downloaded1, downloaded2 []byte
	var offset int64

	for offset < int64(len(data)) {
		remaining := int64(len(data)) - offset
		readSize := chunkSize
		if remaining < int64(chunkSize) {
			readSize = int(remaining)
		}

		chunk1, err := inst1.TransferSvc.DownloadChunk(sessionID1, readSize, offset)
		if err != nil {
			t.Fatalf("Download chunk from instance 1 at offset %d failed: %v", offset, err)
		}
		downloaded1 = append(downloaded1, chunk1...)

		chunk2, err := inst2.TransferSvc.DownloadChunk(sessionID2, readSize, offset)
		if err != nil {
			t.Fatalf("Download chunk from instance 2 at offset %d failed: %v", offset, err)
		}
		downloaded2 = append(downloaded2, chunk2...)

		offset += int64(readSize)
	}

	inst1.TransferSvc.CompleteDownload(sessionID1)
	inst2.TransferSvc.CompleteDownload(sessionID2)

	if len(downloaded1) != len(data) {
		t.Errorf("Instance 1: total downloaded size mismatch, expected %d, got %d", len(data), len(downloaded1))
	}
	if len(downloaded2) != len(data) {
		t.Errorf("Instance 2: total downloaded size mismatch, expected %d, got %d", len(data), len(downloaded2))
	}

	if string(downloaded1) != string(data) {
		t.Error("Instance 1: downloaded data does not match original")
	}
	if string(downloaded2) != string(data) {
		t.Error("Instance 2: downloaded data does not match original")
	}
	if string(downloaded1) != string(downloaded2) {
		t.Error("Downloaded data from two instances should be identical")
	}
}

func TestMultiInstance_GRPCStreamingUploadWithHashVerification(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]

	data := generateGRPCData(1024 * 50)
	hash := computeHashGRPC(data)
	filePath := "/grpc_stream_hash_test.dat"
	chunkSize := int64(1024 * 10)

	sessionID, err := inst1.TransferSvc.CreateUploadSession(filePath, "grpc_stream_hash_test.dat", int64(len(data)), "test", hash)
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
		actualHash := computeHashGRPC(downloaded)
		if actualHash != hash {
			t.Errorf("Instance %d: hash mismatch after streaming upload with verification, expected %s, got %s", i, hash[:8], actualHash[:8])
		}
	}
}

func TestMultiInstance_GRPCListFilesConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	numFilesPerInst := 5
	for i := 0; i < numFilesPerInst; i++ {
		data := generateGRPCData(100)
		filePath := fmt.Sprintf("/grpc_list_%d.dat", i)
		if _, err := inst1.FM.UploadFile(filePath, data); err != nil {
			t.Fatalf("Upload file %d to instance 1 failed: %v", i, err)
		}
	}
	for i := 0; i < numFilesPerInst; i++ {
		data := generateGRPCData(100)
		filePath := fmt.Sprintf("/grpc_list_%d.dat", numFilesPerInst+i)
		if _, err := inst2.FM.UploadFile(filePath, data); err != nil {
			t.Fatalf("Upload file %d to instance 2 failed: %v", numFilesPerInst+i, err)
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

func TestMultiInstance_GRPCDeleteThenVerifyConsistency(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]

	for i := 0; i < 5; i++ {
		data := generateGRPCData(100)
		filePath := fmt.Sprintf("/grpc_del_%d.dat", i)
		if _, err := inst1.FM.UploadFile(filePath, data); err != nil {
			t.Fatalf("Upload file %d failed: %v", i, err)
		}
	}

	inst2.FM.DeleteFile("/grpc_del_2.dat")
	inst2.FM.DeleteFile("/grpc_del_4.dat")

	for i, inst := range cluster.Instances {
		result, err := inst.FlSvc.ListFiles("/", false, 1, 100, "name", "asc")
		if err != nil {
			t.Errorf("Instance %d: list files failed: %v", i, err)
			continue
		}
		if result.Total != 3 {
			t.Errorf("Instance %d: expected 3 files after delete, got %d", i, result.Total)
		}
	}

	deletedPaths := []string{"/grpc_del_2.dat", "/grpc_del_4.dat"}
	for _, p := range deletedPaths {
		for i, inst := range cluster.Instances {
			if inst.FM.Exists(p) {
				t.Errorf("Instance %d: deleted file %s should not exist", i, p)
			}
		}
	}

	existingPaths := []string{"/grpc_del_0.dat", "/grpc_del_1.dat", "/grpc_del_3.dat"}
	for _, p := range existingPaths {
		for i, inst := range cluster.Instances {
			if !inst.FM.Exists(p) {
				t.Errorf("Instance %d: existing file %s should exist", i, p)
			}
		}
	}
}

func TestMultiInstance_GRPCCrossInstanceStreamingUploadDownload(t *testing.T) {
	cluster := NewGRPCCluster(t, 3)
	defer cluster.Cleanup()

	inst1 := cluster.Instances[0]
	inst2 := cluster.Instances[1]
	inst3 := cluster.Instances[2]

	data := generateGRPCData(1024 * 30)
	hash := computeHashGRPC(data)
	filePath := "/grpc_cross_stream_test.dat"
	chunkSize := int64(1024 * 5)

	sessionID, err := inst1.TransferSvc.CreateUploadSession(filePath, "grpc_cross_stream_test.dat", int64(len(data)), "test", hash)
	if err != nil {
		t.Fatalf("Create upload session on instance 1 failed: %v", err)
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

	for i, inst := range []*GRPCInstance{inst2, inst3} {
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
		actualHash := computeHashGRPC(downloaded)
		if actualHash != hash {
			t.Errorf("Instance %d: hash mismatch, expected %s, got %s", i+1, hash[:8], actualHash[:8])
		}
	}
}

func computeHashGRPC(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}
