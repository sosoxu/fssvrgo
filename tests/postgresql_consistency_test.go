package tests

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func setupPostgreSQLTest(t *testing.T) (*filemanager.FileManager, *directory.DirectoryManager, *filelist.FileListService, *transfer.FileTransferService, storage.StorageAdapter, *database.DB, func()) {
	t.Helper()

	storageDir, err := os.MkdirTemp("", "pg-test-storage-*")
	if err != nil {
		t.Fatalf("failed to create storage dir: %v", err)
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
		os.RemoveAll(storageDir)
		t.Skipf("PostgreSQL not available, skipping: %v", err)
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
		os.RemoveAll(storageDir)
		t.Fatalf("failed to run migrations: %v", err)
	}

	store := storage.NewLocalStorage(storageDir)
	fm := filemanager.NewFileManager(store, qdb)
	dirSvc := directory.NewDirectoryManager(qdb)
	flSvc := filelist.NewFileListService(qdb)
	transferSvc := transfer.NewFileTransferService(store, qdb)

	cleanup := func() {
		_, _ = qdb.Exec("DELETE FROM transfer_tasks")
		_, _ = qdb.Exec("DELETE FROM audit_log")
		_, _ = qdb.Exec("DELETE FROM api_keys")
		_, _ = qdb.Exec("DELETE FROM files")
		_, _ = qdb.Exec("DELETE FROM directories")
		_, _ = qdb.Exec("DELETE FROM schema_migrations")
		dbObj.Close()
		os.RemoveAll(storageDir)
	}

	return fm, dirSvc, flSvc, transferSvc, store, qdb, cleanup
}

func TestPostgreSQL_FileUploadDownload(t *testing.T) {
	fm, _, _, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	data := []byte("postgresql upload download test")
	_, err := fm.UploadFile("/pg_test.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	downloaded, err := fm.DownloadFile("/pg_test.txt")
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	if !bytes.Equal(downloaded, data) {
		t.Errorf("downloaded content mismatch")
	}
}

func TestPostgreSQL_FileMetadata(t *testing.T) {
	fm, _, _, _, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	data := []byte("metadata test for postgresql")
	_, err := fm.UploadFile("/pg_meta.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, err := database.NewFileMetadataService(db).GetByPath("/pg_meta.txt")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if meta == nil {
		t.Fatalf("metadata not found")
	}
	if meta.Name != "pg_meta.txt" {
		t.Errorf("expected name pg_meta.txt, got %s", meta.Name)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), meta.Size)
	}
	if meta.IsDeleted {
		t.Errorf("expected is_deleted to be false")
	}
}

func TestPostgreSQL_DeleteFile(t *testing.T) {
	fm, _, _, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	data := []byte("delete test for postgresql")
	_, err := fm.UploadFile("/pg_delete.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	err = fm.DeleteFile("/pg_delete.txt")
	if err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	exists := fm.Exists("/pg_delete.txt")
	if exists {
		t.Errorf("file should not exist after deletion")
	}
}

func TestPostgreSQL_RenameFile(t *testing.T) {
	fm, _, _, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	data := []byte("rename test for postgresql")
	_, err := fm.UploadFile("/pg_rename_old.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	err = fm.RenameFile("/pg_rename_old.txt", "pg_rename_new.txt")
	if err != nil {
		t.Fatalf("RenameFile failed: %v", err)
	}

	if fm.Exists("/pg_rename_old.txt") {
		t.Errorf("old file should not exist after rename")
	}
	if !fm.Exists("/pg_rename_new.txt") {
		t.Errorf("new file should exist after rename")
	}
}

func TestPostgreSQL_DirectoryOperations(t *testing.T) {
	fm, dirSvc, _, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	err := dirSvc.CreateDirectory("/pg_testdir")
	if err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	if !dirSvc.Exists("/pg_testdir") {
		t.Errorf("directory should exist")
	}

	data := []byte("file in directory")
	_, err = fm.UploadFile("/pg_testdir/file1.txt", data)
	if err != nil {
		t.Fatalf("UploadFile in directory failed: %v", err)
	}

	err = dirSvc.DeleteDirectory("/pg_testdir", true)
	if err != nil {
		t.Fatalf("DeleteDirectory recursive failed: %v", err)
	}

	if dirSvc.Exists("/pg_testdir") {
		t.Errorf("directory should not exist after deletion")
	}
}

func TestPostgreSQL_StreamingUploadDownload(t *testing.T) {
	_, _, _, transferSvc, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	totalSize := int64(1024 * 1024)
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, err := transferSvc.CreateUploadSession("/pg_stream.bin", "pg_stream.bin", totalSize, "test", expectedHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	chunkSize := int64(256 * 1024)
	var offset int64
	for offset < totalSize {
		end := offset + chunkSize
		if end > totalSize {
			end = totalSize
		}
		if err := transferSvc.UploadChunk(sessionID, data[offset:end], offset); err != nil {
			t.Fatalf("UploadChunk at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if err := transferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	meta, err := database.NewFileMetadataService(db).GetByPath("/pg_stream.bin")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if meta == nil {
		t.Fatalf("file metadata not found")
	}
	if meta.Size != totalSize {
		t.Errorf("expected size %d, got %d", totalSize, meta.Size)
	}

	sessionID, err = transferSvc.CreateDownloadSession("/pg_stream.bin", "test")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	var reassembled []byte
	offset = 0
	for offset < totalSize {
		sz := int(chunkSize)
		if totalSize-offset < chunkSize {
			sz = int(totalSize - offset)
		}
		chunk, err := transferSvc.DownloadChunk(sessionID, sz, offset)
		if err != nil {
			t.Fatalf("DownloadChunk at offset %d failed: %v", offset, err)
		}
		reassembled = append(reassembled, chunk...)
		offset += int64(len(chunk))
	}

	if !bytes.Equal(reassembled, data) {
		t.Errorf("downloaded content mismatch")
	}
}

func TestPostgreSQL_ListFiles(t *testing.T) {
	fm, _, flSvc, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("file %d content", i))
		_, err := fm.UploadFile(fmt.Sprintf("/pg_list_%d.txt", i), data)
		if err != nil {
			t.Fatalf("UploadFile failed: %v", err)
		}
	}

	result, err := flSvc.ListFiles("/", false, 1, 100, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}

	if result.Total < 5 {
		t.Errorf("expected at least 5 files, got %d", result.Total)
	}
}

func TestPostgreSQL_ConcurrentWrites(t *testing.T) {
	fm, _, _, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			data := []byte(fmt.Sprintf("concurrent data %d", idx))
			_, err := fm.UploadFile(fmt.Sprintf("/pg_concurrent_%d.txt", idx), data)
			errCh <- err
		}(i)
	}

	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent upload %d failed: %v", i, err)
		}
	}

	for i := 0; i < 10; i++ {
		if !fm.Exists(fmt.Sprintf("/pg_concurrent_%d.txt", i)) {
			t.Errorf("file /pg_concurrent_%d.txt should exist", i)
		}
	}
}

func TestPostgreSQL_ApiKeyOperations(t *testing.T) {
	_, _, _, _, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	svc := database.NewApiKeyService(db)

	key := &database.ApiKey{
		ID:          utils.GenerateUUID(),
		KeyHash:     "test_hash_pg",
		Name:        "Test Key PG",
		Description: "Test key for PostgreSQL",
		Permissions: "read,write",
		CreatedAt:   utils.GetCurrentTimestamp(),
		IsActive:    true,
	}

	if err := svc.Create(key); err != nil {
		t.Fatalf("Create api key failed: %v", err)
	}

	retrieved, err := svc.GetByKeyHash("test_hash_pg")
	if err != nil {
		t.Fatalf("GetByKeyHash failed: %v", err)
	}
	if retrieved == nil {
		t.Fatalf("api key not found")
	}
	if retrieved.Name != "Test Key PG" {
		t.Errorf("expected name 'Test Key PG', got %s", retrieved.Name)
	}
	if !retrieved.IsActive {
		t.Errorf("expected is_active to be true")
	}

	if err := svc.Deactivate(retrieved.ID); err != nil {
		t.Fatalf("Deactivate failed: %v", err)
	}

	retrieved, err = svc.GetByKeyHash("test_hash_pg")
	if err != nil {
		t.Fatalf("GetByKeyHash after deactivate failed: %v", err)
	}
	if retrieved != nil {
		t.Errorf("deactivated key should not be returned by GetByKeyHash")
	}
}

func TestPostgreSQL_AuditLogOperations(t *testing.T) {
	_, _, _, _, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	svc := database.NewAuditLogService(db)

	log := &database.AuditLog{
		ID:             utils.GenerateUUID(),
		Timestamp:      utils.GetCurrentTimestamp(),
		Operation:      "upload",
		ResourcePath:   "/test/pg_file.txt",
		UserIdentifier: "test_user",
		ClientIP:       "127.0.0.1",
		UserAgent:      "test-agent",
		Success:        true,
		Details:        "Test audit log for PostgreSQL",
	}

	if err := svc.Create(log); err != nil {
		t.Fatalf("Create audit log failed: %v", err)
	}

	logs, err := svc.List("upload", "", 1, 10)
	if err != nil {
		t.Fatalf("List audit logs failed: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("expected 1 audit log, got %d", len(logs))
	}
	if logs[0].Operation != "upload" {
		t.Errorf("expected operation 'upload', got %s", logs[0].Operation)
	}
}

func TestPostgreSQL_DialectTranslation(t *testing.T) {
	_, _, _, _, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	dialect := db.GetDialect()
	if dialect != database.DialectPostgreSQL {
		t.Errorf("expected PostgreSQL dialect, got %v", dialect)
	}

	translated := dialect.Translate("SELECT * FROM files WHERE path = ? AND is_deleted = FALSE")
	if translated != "SELECT * FROM files WHERE path = $1 AND is_deleted = FALSE" {
		t.Errorf("unexpected translation: %s", translated)
	}

	translated = dialect.Translate("INSERT INTO files (id, path) VALUES (?, ?)")
	if translated != "INSERT INTO files (id, path) VALUES ($1, $2)" {
		t.Errorf("unexpected translation: %s", translated)
	}
}

func TestPostgreSQL_DirectoryRename(t *testing.T) {
	fm, dirSvc, _, _, _, _, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	err := dirSvc.CreateDirectory("/pg_renamedir")
	if err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	data1 := []byte("file1 in renamed dir")
	data2 := []byte("file2 in renamed dir")
	_, err = fm.UploadFile("/pg_renamedir/file1.txt", data1)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
	_, err = fm.UploadFile("/pg_renamedir/subdir/file2.txt", data2)
	if err != nil {
		t.Fatalf("UploadFile in subdir failed: %v", err)
	}

	err = dirSvc.RenameDirectory("/pg_renamedir", "pg_newdir")
	if err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}

	if dirSvc.Exists("/pg_renamedir") {
		t.Errorf("old directory should not exist after rename")
	}
	if !dirSvc.Exists("/pg_newdir") {
		t.Errorf("new directory should exist after rename")
	}
	if !fm.Exists("/pg_newdir/file1.txt") {
		t.Errorf("file1 should exist in renamed directory")
	}
	if !fm.Exists("/pg_newdir/subdir/file2.txt") {
		t.Errorf("file2 should exist in renamed subdirectory")
	}
}

func TestPostgreSQL_TransferTaskOperations(t *testing.T) {
	_, _, _, _, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	svc := database.NewTransferTaskService(db)

	task := &database.TransferTask{
		ID:        utils.GenerateUUID(),
		Type:      "upload",
		FileID:    utils.GenerateUUID(),
		ClientID:  "test_client",
		Offset:    0,
		TotalSize: 1024,
		Status:    "pending",
		CreatedAt: utils.GetCurrentTimestamp(),
		UpdatedAt: utils.GetCurrentTimestamp(),
	}

	if err := svc.Create(task); err != nil {
		t.Fatalf("Create transfer task failed: %v", err)
	}

	retrieved, err := svc.GetById(task.ID)
	if err != nil {
		t.Fatalf("GetById failed: %v", err)
	}
	if retrieved == nil {
		t.Fatalf("transfer task not found")
	}
	if retrieved.Status != "pending" {
		t.Errorf("expected status pending, got %s", retrieved.Status)
	}

	if err := svc.UpdateProgress(task.ID, 512); err != nil {
		t.Fatalf("UpdateProgress failed: %v", err)
	}

	if err := svc.CompleteTask(task.ID); err != nil {
		t.Fatalf("CompleteTask failed: %v", err)
	}

	retrieved, _ = svc.GetById(task.ID)
	if retrieved.Status != "completed" {
		t.Errorf("expected status completed, got %s", retrieved.Status)
	}
}

func TestPostgreSQL_SoftDeleteAndRestore(t *testing.T) {
	fm, _, _, _, _, db, cleanup := setupPostgreSQLTest(t)
	defer cleanup()

	data := []byte("soft delete and restore test")
	_, err := fm.UploadFile("/pg_softdel.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, _ := database.NewFileMetadataService(db).GetByPath("/pg_softdel.txt")
	if meta == nil {
		t.Fatalf("metadata not found before delete")
	}

	err = fm.DeleteFile("/pg_softdel.txt")
	if err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	if fm.Exists("/pg_softdel.txt") {
		t.Errorf("file should not exist after soft delete")
	}

	var deletedCount int
	err = db.QueryRow("SELECT COUNT(*) FROM files WHERE path = 'pg_softdel.txt' AND is_deleted = TRUE").Scan(&deletedCount)
	if err != nil {
		t.Fatalf("query deleted files failed: %v", err)
	}
	if deletedCount != 1 {
		t.Errorf("expected 1 deleted record, got %d", deletedCount)
	}

	_, err = fm.UploadFile("/pg_softdel.txt", data)
	if err != nil {
		t.Fatalf("UploadFile (restore) failed: %v", err)
	}

	if !fm.Exists("/pg_softdel.txt") {
		t.Errorf("file should exist after restore")
	}
}

func TestPostgreSQL_DatabaseSwitch(t *testing.T) {
	storageDir, err := os.MkdirTemp("", "pg-switch-test-*")
	if err != nil {
		t.Fatalf("failed to create storage dir: %v", err)
	}
	defer os.RemoveAll(storageDir)

	sqlitePath := filepath.Join(storageDir, "sqlite_test.db")
	sqliteCfg := config.DatabaseConfig{Type: "sqlite", Path: sqlitePath}
	sqliteDbObj := database.NewDatabase()
	if err := sqliteDbObj.Connect(sqliteCfg); err != nil {
		t.Fatalf("SQLite connect failed: %v", err)
	}
	sqliteQdb := sqliteDbObj.GetQueryDB()

	mgr := database.NewMigrationManager(sqliteQdb)
	mgr.Register(database.Migration{Version: 1, Name: "init", Up: func() error { return database.InitTables(sqliteQdb) }})
	if err := mgr.RunMigrations(); err != nil {
		t.Fatalf("SQLite migration failed: %v", err)
	}

	store := storage.NewLocalStorage(filepath.Join(storageDir, "files"))
	fm := filemanager.NewFileManager(store, sqliteQdb)

	data := []byte("sqlite data")
	_, err = fm.UploadFile("/switch_test.txt", data)
	if err != nil {
		t.Fatalf("SQLite UploadFile failed: %v", err)
	}

	sqliteDialect := sqliteQdb.GetDialect()
	if sqliteDialect != database.DialectSQLite {
		t.Errorf("expected SQLite dialect, got %v", sqliteDialect)
	}

	sqliteDbObj.Close()

	pgCfg := config.DatabaseConfig{
		Type:     "postgresql",
		Host:     "localhost",
		Port:     5432,
		Name:     "fsserver",
		User:     "fsserver",
		Password: "fsserver123",
		SSLMode:  "disable",
	}
	pgDbObj := database.NewDatabase()
	if err := pgDbObj.Connect(pgCfg); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	pgQdb := pgDbObj.GetQueryDB()

	_, _ = pgQdb.Exec("DELETE FROM files")
	_, _ = pgQdb.Exec("DELETE FROM directories")
	_, _ = pgQdb.Exec("DELETE FROM schema_migrations")

	pgMgr := database.NewMigrationManager(pgQdb)
	pgMgr.Register(database.Migration{Version: 1, Name: "init", Up: func() error { return database.InitTables(pgQdb) }})
	if err := pgMgr.RunMigrations(); err != nil {
		t.Fatalf("PostgreSQL migration failed: %v", err)
	}

	pgFm := filemanager.NewFileManager(store, pgQdb)
	_, err = pgFm.UploadFile("/pg_switch_test.txt", data)
	if err != nil {
		t.Fatalf("PostgreSQL UploadFile failed: %v", err)
	}

	pgDialect := pgQdb.GetDialect()
	if pgDialect != database.DialectPostgreSQL {
		t.Errorf("expected PostgreSQL dialect, got %v", pgDialect)
	}

	_, _ = pgQdb.Exec("DELETE FROM files")
	_, _ = pgQdb.Exec("DELETE FROM schema_migrations")
	pgDbObj.Close()
}
