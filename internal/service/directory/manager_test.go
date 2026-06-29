package directory

import (
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// setupTestDB creates an in-memory SQLite database with all required tables.
func setupTestDB(t *testing.T) *database.DB {
	t.Helper()
	dbCfg := config.DatabaseConfig{Type: "sqlite", Path: ":memory:", PoolSize: 1}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}
	qdb := dbObj.GetQueryDB()
	if err := database.InitTables(qdb); err != nil {
		dbObj.Close()
		t.Fatalf("failed to init tables: %v", err)
	}
	t.Cleanup(func() {
		dbObj.Close()
	})
	return qdb
}

// setupTestStore creates a LocalStorage backed by a per-test temp directory.
func setupTestStore(t *testing.T) *storage.LocalStorage {
	t.Helper()
	return storage.NewLocalStorage(t.TempDir())
}

// createFileRecord inserts a file metadata row so the directory manager sees a
// file inside a directory (without touching the storage backend).
func createFileRecord(t *testing.T, db *database.DB, path string, size int64) {
	t.Helper()
	now := utils.GetCurrentTimestamp()
	name := utils.GetFileName(path)
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            path,
		Name:            name,
		Size:            size,
		Hash:            "",
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("failed to create file metadata for %q: %v", path, err)
	}
}

func TestCreateDirectory(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	// 正常创建
	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if !dm.Exists("foo") {
		t.Errorf("Exists should return true for created directory")
	}

	// 重复创建报错
	err := dm.CreateDirectory("foo")
	if err == nil {
		t.Errorf("CreateDirectory should fail on duplicate")
	}
}

func TestDeleteDirectory_NonRecursive_Empty(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	// 非递归删除空目录
	if err := dm.DeleteDirectory("foo", false); err != nil {
		t.Fatalf("DeleteDirectory non-recursive failed: %v", err)
	}
	if dm.Exists("foo") {
		t.Errorf("Exists should return false after deletion")
	}
}

func TestDeleteDirectory_NonRecursive_NotEmpty(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	createFileRecord(t, db, "foo/bar.txt", 10)

	// 非递归删除非空目录报错
	err := dm.DeleteDirectory("foo", false)
	if err == nil {
		t.Errorf("DeleteDirectory non-recursive should fail on non-empty directory")
	}
	if !dm.Exists("foo") {
		t.Errorf("Exists should still return true when delete failed")
	}
}

func TestDeleteDirectory_Recursive_WithFiles(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if err := dm.CreateDirectory("foo/sub"); err != nil {
		t.Fatalf("CreateDirectory sub failed: %v", err)
	}
	createFileRecord(t, db, "foo/bar.txt", 10)
	createFileRecord(t, db, "foo/sub/nested.txt", 5)

	// 递归删除含文件的目录
	if err := dm.DeleteDirectory("foo", true); err != nil {
		t.Fatalf("DeleteDirectory recursive failed: %v", err)
	}
	if dm.Exists("foo") {
		t.Errorf("Exists should return false after recursive deletion")
	}
	// 子目录与文件的元数据都应该被软删除
	subMeta, err := database.NewDirectoryMetadataService(db).GetByPath("foo/sub")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if subMeta != nil {
		t.Errorf("sub-directory metadata should be soft-deleted")
	}
	fileMeta, err := database.NewFileMetadataService(db).GetByPath("foo/bar.txt")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if fileMeta != nil {
		t.Errorf("file metadata should be soft-deleted")
	}
}

func TestRenameDirectory(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	// 正常重命名
	if err := dm.RenameDirectory("foo", "bar"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}
	if !dm.Exists("bar") {
		t.Errorf("Exists should return true for renamed directory")
	}
	if dm.Exists("foo") {
		t.Errorf("Exists should return false for old directory name")
	}

	meta, err := dm.GetDirectoryMetadata("bar")
	if err != nil {
		t.Fatalf("GetDirectoryMetadata failed: %v", err)
	}
	if meta.Name != "bar" {
		t.Errorf("expected Name bar, got %s", meta.Name)
	}
	if meta.Path != "bar" {
		t.Errorf("expected Path bar, got %s", meta.Path)
	}
}

func TestRenameDirectory_TargetExists(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory foo failed: %v", err)
	}
	if err := dm.CreateDirectory("bar"); err != nil {
		t.Fatalf("CreateDirectory bar failed: %v", err)
	}
	// 目标已存在报错
	err := dm.RenameDirectory("foo", "bar")
	if err == nil {
		t.Errorf("RenameDirectory should fail when target exists")
	}
}

func TestRenameDirectory_WithChildren(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	if err := dm.CreateDirectory("foo/sub"); err != nil {
		t.Fatalf("CreateDirectory sub failed: %v", err)
	}
	createFileRecord(t, db, "foo/bar.txt", 10)

	if err := dm.RenameDirectory("foo", "baz"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}
	if !dm.Exists("baz") {
		t.Errorf("Exists should return true for renamed directory")
	}
	if !dm.Exists("baz/sub") {
		t.Errorf("Exists should return true for renamed child directory")
	}
	fileMeta, err := database.NewFileMetadataService(db).GetByPath("baz/bar.txt")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if fileMeta == nil {
		t.Errorf("file metadata should be moved to new path")
	}
}

func TestGetDirectoryMetadata_Exists(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	// 获取存在的目录
	meta, err := dm.GetDirectoryMetadata("foo")
	if err != nil {
		t.Fatalf("GetDirectoryMetadata failed: %v", err)
	}
	if meta == nil {
		t.Fatalf("expected metadata, got nil")
	}
	if meta.Name != "foo" {
		t.Errorf("expected Name foo, got %s", meta.Name)
	}
	if meta.Path != "foo" {
		t.Errorf("expected Path foo, got %s", meta.Path)
	}
}

func TestGetDirectoryMetadata_NotExists(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	// 获取不存在的目录
	_, err := dm.GetDirectoryMetadata("nonexistent")
	if err == nil {
		t.Errorf("GetDirectoryMetadata should fail for non-existent directory")
	}
}

func TestExists(t *testing.T) {
	db := setupTestDB(t)
	dm := NewDirectoryManager(db)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	// 目录存在
	if !dm.Exists("foo") {
		t.Errorf("Exists should return true for existing directory")
	}
	// 目录不存在
	if dm.Exists("nonexistent") {
		t.Errorf("Exists should return false for non-existent directory")
	}
}

func TestDirectoryManagerWithStore_DeleteRemovesStorageObject(t *testing.T) {
	db := setupTestDB(t)
	store := setupTestStore(t)
	dm := NewDirectoryManagerWithStore(db, store)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	// 在存储中写入文件并插入 DB 记录
	if err := store.Write("foo/bar.txt", []byte("hello")); err != nil {
		t.Fatalf("store.Write failed: %v", err)
	}
	if !store.Exists("foo/bar.txt") {
		t.Fatalf("storage file should exist before deletion")
	}
	createFileRecord(t, db, "foo/bar.txt", 5)

	// 递归删除应同时删除存储对象
	if err := dm.DeleteDirectory("foo", true); err != nil {
		t.Fatalf("DeleteDirectory recursive failed: %v", err)
	}
	if store.Exists("foo/bar.txt") {
		t.Errorf("storage file should be removed after deletion")
	}
}

func TestDirectoryManagerWithStore_RenameMovesStorageObject(t *testing.T) {
	db := setupTestDB(t)
	store := setupTestStore(t)
	dm := NewDirectoryManagerWithStore(db, store)

	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}
	// 在存储中写入文件并插入 DB 记录
	if err := store.Write("foo/bar.txt", []byte("hello")); err != nil {
		t.Fatalf("store.Write failed: %v", err)
	}
	if !store.Exists("foo/bar.txt") {
		t.Fatalf("storage file should exist before rename")
	}
	createFileRecord(t, db, "foo/bar.txt", 5)

	// 重命名应同时移动存储对象
	if err := dm.RenameDirectory("foo", "baz"); err != nil {
		t.Fatalf("RenameDirectory failed: %v", err)
	}
	if store.Exists("foo/bar.txt") {
		t.Errorf("storage file should not exist at old path after rename")
	}
	if !store.Exists("baz/bar.txt") {
		t.Errorf("storage file should exist at new path after rename")
	}
	// DB 元数据也应更新
	fileMeta, err := database.NewFileMetadataService(db).GetByPath("baz/bar.txt")
	if err != nil {
		t.Fatalf("GetByPath failed: %v", err)
	}
	if fileMeta == nil {
		t.Errorf("file metadata should exist at new path")
	}
}
