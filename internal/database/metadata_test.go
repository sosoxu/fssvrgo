package database

import (
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// newMetadataTestDB provisions an in-memory SQLite database (single connection
// so the in-memory store is shared across statements) and initializes all
// metadata tables via InitTables. The database is closed automatically when
// the test finishes.
func newMetadataTestDB(t *testing.T) *DB {
	t.Helper()
	dbObj := NewDatabase()
	cfg := config.DatabaseConfig{Type: "sqlite", Path: ":memory:", PoolSize: 1}
	if err := dbObj.Connect(cfg); err != nil {
		t.Fatalf("connect database: %v", err)
	}
	t.Cleanup(func() { _ = dbObj.Close() })

	qdb := dbObj.GetQueryDB()
	if err := InitTables(qdb); err != nil {
		t.Fatalf("init tables: %v", err)
	}
	return qdb
}

func newFileMetadata(id, path string) *FileMetadata {
	now := time.Now().UTC().Format(time.RFC3339)
	return &FileMetadata{
		ID:              id,
		Path:            path,
		Name:            utils.GetFileName(path),
		Size:            1024,
		Hash:            "deadbeef",
		StorageType:     "local",
		StorageLocation: "/data/" + id,
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}
}

// 1. TestFileMetadata_CreateAndGet
func TestFileMetadata_CreateAndGet(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewFileMetadataService(db)

	m := newFileMetadata("fmd-1", "/docs/readme.md")
	if err := svc.Create(m); err != nil {
		t.Fatalf("create: %v", err)
	}

	gotByPath, err := svc.GetByPath("/docs/readme.md")
	if err != nil {
		t.Fatalf("get by path: %v", err)
	}
	if gotByPath == nil {
		t.Fatalf("expected metadata, got nil")
	}
	if gotByPath.ID != "fmd-1" {
		t.Errorf("expected ID fmd-1, got %s", gotByPath.ID)
	}
	if gotByPath.Name != "readme.md" {
		t.Errorf("expected Name readme.md, got %s", gotByPath.Name)
	}
	if gotByPath.Size != 1024 {
		t.Errorf("expected Size 1024, got %d", gotByPath.Size)
	}
	if gotByPath.Hash != "deadbeef" {
		t.Errorf("expected Hash deadbeef, got %s", gotByPath.Hash)
	}
	if gotByPath.StorageType != "local" {
		t.Errorf("expected StorageType local, got %s", gotByPath.StorageType)
	}
	if gotByPath.IsDeleted {
		t.Errorf("expected IsDeleted false, got true")
	}
	if gotByPath.Path != "docs/readme.md" {
		t.Errorf("expected normalized path docs/readme.md, got %s", gotByPath.Path)
	}

	gotByID, err := svc.GetById("fmd-1")
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if gotByID == nil {
		t.Fatalf("expected metadata by id, got nil")
	}
	if gotByID.Path != "docs/readme.md" {
		t.Errorf("expected normalized path docs/readme.md, got %s", gotByID.Path)
	}

	// nonexistent id returns nil, nil
	missing, err := svc.GetById("does-not-exist")
	if err != nil {
		t.Fatalf("get missing by id: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing id, got %+v", missing)
	}
}

// 2. TestFileMetadata_Update
func TestFileMetadata_Update(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewFileMetadataService(db)

	m := newFileMetadata("fmd-2", "/pics/cat.png")
	if err := svc.Create(m); err != nil {
		t.Fatalf("create: %v", err)
	}

	updated := time.Now().UTC().Format(time.RFC3339)
	m.Size = 2048
	m.Hash = "newhash"
	m.StorageLocation = "/data/new-location"
	m.UpdatedAt = updated
	if err := svc.Update(m); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := svc.GetById("fmd-2")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got == nil {
		t.Fatalf("expected metadata after update, got nil")
	}
	if got.Size != 2048 {
		t.Errorf("expected Size 2048, got %d", got.Size)
	}
	if got.Hash != "newhash" {
		t.Errorf("expected Hash newhash, got %s", got.Hash)
	}
	if got.StorageLocation != "/data/new-location" {
		t.Errorf("expected StorageLocation /data/new-location, got %s", got.StorageLocation)
	}
	if got.UpdatedAt != updated {
		t.Errorf("expected UpdatedAt %s, got %s", updated, got.UpdatedAt)
	}
}

// 3. TestFileMetadata_Remove
func TestFileMetadata_Remove(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewFileMetadataService(db)

	m := newFileMetadata("fmd-3", "/tmp/scratch.txt")
	if err := svc.Create(m); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := svc.Remove("fmd-3"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	got, err := svc.GetById("fmd-3")
	if err != nil {
		t.Fatalf("get after remove: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after soft delete, got %+v", got)
	}

	gotByPath, err := svc.GetByPath("/tmp/scratch.txt")
	if err != nil {
		t.Fatalf("get by path after remove: %v", err)
	}
	if gotByPath != nil {
		t.Errorf("expected nil by path after soft delete, got %+v", gotByPath)
	}

	exists, err := svc.Exists("/tmp/scratch.txt")
	if err != nil {
		t.Fatalf("exists after remove: %v", err)
	}
	if exists {
		t.Errorf("expected Exists false after soft delete")
	}
}

// 4. TestFileMetadata_ListFiles
func TestFileMetadata_ListFiles(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewFileMetadataService(db)

	files := []*FileMetadata{
		newFileMetadata("fmd-l1", "/dir/alpha.txt"),
		newFileMetadata("fmd-l2", "/dir/beta.txt"),
		newFileMetadata("fmd-l3", "/other/gamma.txt"),
	}
	for _, f := range files {
		if err := svc.Create(f); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	// list all (NormalizePath("/") == "" so no LIKE filter is applied)
	all, err := svc.List("/", "name", "asc", 1, 100)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 files, got %d", len(all))
	}
	if all[0].Name != "alpha.txt" {
		t.Errorf("expected first alpha.txt, got %s", all[0].Name)
	}

	// list with directory filter (normalized to "dir")
	dirFiles, err := svc.List("/dir", "name", "asc", 1, 100)
	if err != nil {
		t.Fatalf("list dir: %v", err)
	}
	if len(dirFiles) != 2 {
		t.Fatalf("expected 2 files in dir, got %d", len(dirFiles))
	}

	// descending order
	desc, err := svc.List("/", "name", "desc", 1, 100)
	if err != nil {
		t.Fatalf("list desc: %v", err)
	}
	if len(desc) != 3 {
		t.Fatalf("expected 3 files desc, got %d", len(desc))
	}
	if desc[0].Name != "gamma.txt" {
		t.Errorf("expected first gamma.txt in desc, got %s", desc[0].Name)
	}

	// pagination: page 1 size 2
	page1, err := svc.List("/", "name", "asc", 1, 2)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 files on page 1, got %d", len(page1))
	}
	page2, err := svc.List("/", "name", "asc", 2, 2)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected 1 file on page 2, got %d", len(page2))
	}

	// invalid sort column falls back to "name"
	invalidSort, err := svc.List("/", "nonexistent_col", "asc", 1, 100)
	if err != nil {
		t.Fatalf("list invalid sort: %v", err)
	}
	if len(invalidSort) != 3 {
		t.Fatalf("expected 3 files with invalid sort, got %d", len(invalidSort))
	}
}

// 5. TestDirectoryMetadata_CreateAndGet
func TestDirectoryMetadata_CreateAndGet(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewDirectoryMetadataService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	dm := &DirectoryMetadata{
		ID:        "dir-1",
		Path:      "/projects/api",
		Name:      "api",
		CreatedAt: now,
		UpdatedAt: now,
		IsDeleted: false,
	}
	if err := svc.Create(dm); err != nil {
		t.Fatalf("create: %v", err)
	}

	gotByPath, err := svc.GetByPath("/projects/api")
	if err != nil {
		t.Fatalf("get by path: %v", err)
	}
	if gotByPath == nil {
		t.Fatalf("expected directory, got nil")
	}
	if gotByPath.ID != "dir-1" {
		t.Errorf("expected ID dir-1, got %s", gotByPath.ID)
	}
	if gotByPath.Name != "api" {
		t.Errorf("expected Name api, got %s", gotByPath.Name)
	}
	if gotByPath.IsDeleted {
		t.Errorf("expected IsDeleted false")
	}
	if gotByPath.Path != "projects/api" {
		t.Errorf("expected normalized path projects/api, got %s", gotByPath.Path)
	}

	gotByID, err := svc.GetById("dir-1")
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if gotByID == nil {
		t.Fatalf("expected directory by id, got nil")
	}
	if gotByID.Path != "projects/api" {
		t.Errorf("expected normalized path projects/api, got %s", gotByID.Path)
	}

	missing, err := svc.GetById("nope")
	if err != nil {
		t.Fatalf("get missing by id: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing dir, got %+v", missing)
	}
}

// 6. TestDirectoryMetadata_Remove
func TestDirectoryMetadata_Remove(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewDirectoryMetadataService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	dm := &DirectoryMetadata{
		ID:        "dir-2",
		Path:      "/projects/old",
		Name:      "old",
		CreatedAt: now,
		UpdatedAt: now,
		IsDeleted: false,
	}
	if err := svc.Create(dm); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := svc.Remove("dir-2"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	got, err := svc.GetById("dir-2")
	if err != nil {
		t.Fatalf("get after remove: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after soft delete, got %+v", got)
	}

	gotByPath, err := svc.GetByPath("/projects/old")
	if err != nil {
		t.Fatalf("get by path after remove: %v", err)
	}
	if gotByPath != nil {
		t.Errorf("expected nil by path after soft delete, got %+v", gotByPath)
	}
}

// 7. TestDirectoryMetadata_Exists
func TestDirectoryMetadata_Exists(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewDirectoryMetadataService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	dm := &DirectoryMetadata{
		ID:        "dir-3",
		Path:      "/projects/check",
		Name:      "check",
		CreatedAt: now,
		UpdatedAt: now,
		IsDeleted: false,
	}
	if err := svc.Create(dm); err != nil {
		t.Fatalf("create: %v", err)
	}

	exists, err := svc.Exists("/projects/check")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Errorf("expected exists true")
	}

	notExists, err := svc.Exists("/projects/missing")
	if err != nil {
		t.Fatalf("exists missing: %v", err)
	}
	if notExists {
		t.Errorf("expected exists false for missing dir")
	}

	if err := svc.Remove("dir-3"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	existsAfterRemove, err := svc.Exists("/projects/check")
	if err != nil {
		t.Fatalf("exists after remove: %v", err)
	}
	if existsAfterRemove {
		t.Errorf("expected exists false after soft delete")
	}
}

// 8. TestAuditLog_CreateAndList
func TestAuditLog_CreateAndList(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewAuditLogService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	logs := []*AuditLog{
		{
			ID:             "audit-1",
			Timestamp:      now,
			Operation:      "upload",
			ResourcePath:   "/files/a.txt",
			UserIdentifier: "user1",
			ClientIP:       "127.0.0.1",
			UserAgent:      "test-agent",
			Success:        true,
			Details:        "uploaded a.txt",
		},
		{
			ID:             "audit-2",
			Timestamp:      now,
			Operation:      "download",
			ResourcePath:   "/files/a.txt",
			UserIdentifier: "user2",
			ClientIP:       "10.0.0.1",
			UserAgent:      "test-agent",
			Success:        false,
			Details:        "failed to download a.txt",
		},
		{
			ID:             "audit-3",
			Timestamp:      now,
			Operation:      "upload",
			ResourcePath:   "/files/b.txt",
			UserIdentifier: "user1",
			ClientIP:       "127.0.0.1",
			UserAgent:      "test-agent",
			Success:        true,
			Details:        "uploaded b.txt",
		},
	}
	for _, l := range logs {
		if err := svc.Create(l); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	// list all
	all, err := svc.List("", "", 1, 100)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 logs, got %d", len(all))
	}

	// filter by operation
	uploads, err := svc.List("upload", "", 1, 100)
	if err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(uploads) != 2 {
		t.Fatalf("expected 2 uploads, got %d", len(uploads))
	}
	for _, l := range uploads {
		if l.Operation != "upload" {
			t.Errorf("expected operation upload, got %s", l.Operation)
		}
	}

	// filter by resource path (LIKE)
	aLogs, err := svc.List("", "/files/a.txt", 1, 100)
	if err != nil {
		t.Fatalf("list by resource: %v", err)
	}
	if len(aLogs) != 2 {
		t.Fatalf("expected 2 logs for a.txt, got %d", len(aLogs))
	}

	// pagination
	page1, err := svc.List("", "", 1, 2)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 on page 1, got %d", len(page1))
	}
	page2, err := svc.List("", "", 2, 2)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected 1 on page 2, got %d", len(page2))
	}
}

// 9. TestApiKey_CreateAndGetByKeyHash
func TestApiKey_CreateAndGetByKeyHash(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewApiKeyService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	future := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	key := &ApiKey{
		ID:          "key-1",
		KeyHash:     "hash-abc-123",
		Name:        "test-key",
		Description: "for testing",
		Permissions: "read,write",
		CreatedAt:   now,
		ExpiresAt:   future,
		IsActive:    true,
	}
	if err := svc.Create(key); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := svc.GetByKeyHash("hash-abc-123")
	if err != nil {
		t.Fatalf("get by key hash: %v", err)
	}
	if got == nil {
		t.Fatalf("expected api key, got nil")
	}
	if got.ID != "key-1" {
		t.Errorf("expected ID key-1, got %s", got.ID)
	}
	if got.Name != "test-key" {
		t.Errorf("expected Name test-key, got %s", got.Name)
	}
	if got.Description != "for testing" {
		t.Errorf("expected Description for testing, got %s", got.Description)
	}
	if got.Permissions != "read,write" {
		t.Errorf("expected Permissions read,write, got %s", got.Permissions)
	}
	if got.ExpiresAt != future {
		t.Errorf("expected ExpiresAt %s, got %s", future, got.ExpiresAt)
	}
	if !got.IsActive {
		t.Errorf("expected IsActive true")
	}

	// GetById also works
	byID, err := svc.GetById("key-1")
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if byID == nil {
		t.Fatalf("expected api key by id, got nil")
	}
	if byID.KeyHash != "hash-abc-123" {
		t.Errorf("expected KeyHash hash-abc-123, got %s", byID.KeyHash)
	}
}

// 10. TestApiKey_GetByKeyHash_NotFound
func TestApiKey_GetByKeyHash_NotFound(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewApiKeyService(db)

	// nonexistent hash returns nil, nil
	missing, err := svc.GetByKeyHash("does-not-exist")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing hash, got %+v", missing)
	}

	// inactive key should not be returned by GetByKeyHash
	now := time.Now().UTC().Format(time.RFC3339)
	inactiveKey := &ApiKey{
		ID:        "key-inactive",
		KeyHash:   "hash-inactive",
		Name:      "inactive-key",
		CreatedAt: now,
		IsActive:  false,
	}
	if err := svc.Create(inactiveKey); err != nil {
		t.Fatalf("create inactive: %v", err)
	}
	gotInactive, err := svc.GetByKeyHash("hash-inactive")
	if err != nil {
		t.Fatalf("get inactive: %v", err)
	}
	if gotInactive != nil {
		t.Errorf("expected nil for inactive key, got %+v", gotInactive)
	}

	// expired (but active) key should not be returned by GetByKeyHash
	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	expiredKey := &ApiKey{
		ID:        "key-expired",
		KeyHash:   "hash-expired",
		Name:      "expired-key",
		CreatedAt: now,
		ExpiresAt: past,
		IsActive:  true,
	}
	if err := svc.Create(expiredKey); err != nil {
		t.Fatalf("create expired: %v", err)
	}
	expiredGot, err := svc.GetByKeyHash("hash-expired")
	if err != nil {
		t.Fatalf("get expired: %v", err)
	}
	if expiredGot != nil {
		t.Errorf("expected nil for expired key, got %+v", expiredGot)
	}
}

// 11. TestApiKey_IsExpired
func TestApiKey_IsExpired(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewApiKeyService(db)

	// no expiry -> not expired
	noExpiry := &ApiKey{ID: "k1", KeyHash: "h1", ExpiresAt: ""}
	if svc.IsExpired(noExpiry) {
		t.Errorf("expected not expired for empty ExpiresAt")
	}

	// past expiry -> expired
	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	expired := &ApiKey{ID: "k2", KeyHash: "h2", ExpiresAt: past}
	if !svc.IsExpired(expired) {
		t.Errorf("expected expired for past ExpiresAt")
	}

	// future expiry -> not expired
	future := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	notExpired := &ApiKey{ID: "k3", KeyHash: "h3", ExpiresAt: future}
	if svc.IsExpired(notExpired) {
		t.Errorf("expected not expired for future ExpiresAt")
	}
}

// 12. TestApiKey_UpdateLastUsed
func TestApiKey_UpdateLastUsed(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewApiKeyService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	key := &ApiKey{
		ID:        "key-ulu",
		KeyHash:   "hash-ulu",
		Name:      "ulu-key",
		CreatedAt: now,
		IsActive:  true,
	}
	if err := svc.Create(key); err != nil {
		t.Fatalf("create: %v", err)
	}

	// initially no last_used_at
	got, err := svc.GetById("key-ulu")
	if err != nil {
		t.Fatalf("get before update: %v", err)
	}
	if got.LastUsedAt != "" {
		t.Errorf("expected empty LastUsedAt, got %s", got.LastUsedAt)
	}

	if err := svc.UpdateLastUsed("key-ulu"); err != nil {
		t.Fatalf("update last used: %v", err)
	}

	got, err = svc.GetById("key-ulu")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.LastUsedAt == "" {
		t.Errorf("expected non-empty LastUsedAt after update")
	}
}

// 13. TestApiKey_Deactivate
func TestApiKey_Deactivate(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewApiKeyService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	key := &ApiKey{
		ID:        "key-deact",
		KeyHash:   "hash-deact",
		Name:      "deact-key",
		CreatedAt: now,
		IsActive:  true,
	}
	if err := svc.Create(key); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := svc.Deactivate("key-deact"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}

	got, err := svc.GetById("key-deact")
	if err != nil {
		t.Fatalf("get after deactivate: %v", err)
	}
	if got.IsActive {
		t.Errorf("expected IsActive false after deactivate")
	}

	// deactivated key no longer returned by GetByKeyHash
	byHash, err := svc.GetByKeyHash("hash-deact")
	if err != nil {
		t.Fatalf("get by hash after deactivate: %v", err)
	}
	if byHash != nil {
		t.Errorf("expected nil by hash after deactivate, got %+v", byHash)
	}
}

// 14. TestApiKey_List
func TestApiKey_List(t *testing.T) {
	db := newMetadataTestDB(t)
	svc := NewApiKeyService(db)

	now := time.Now().UTC().Format(time.RFC3339)
	future := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	keys := []*ApiKey{
		{ID: "k-l1", KeyHash: "h1", Name: "key1", CreatedAt: now, ExpiresAt: future, IsActive: true},
		{ID: "k-l2", KeyHash: "h2", Name: "key2", CreatedAt: now, ExpiresAt: future, IsActive: false},
		{ID: "k-l3", KeyHash: "h3", Name: "key3", CreatedAt: now, ExpiresAt: future, IsActive: true},
	}
	for _, k := range keys {
		if err := svc.Create(k); err != nil {
			t.Fatalf("create: %v", err)
		}
		// List scans last_used_at directly into a string; populate it so the
		// scan succeeds for keys that have never been used.
		if err := svc.UpdateLastUsed(k.ID); err != nil {
			t.Fatalf("update last used: %v", err)
		}
	}

	// list all
	all, err := svc.List(false, 1, 100)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(all))
	}

	// list active only
	active, err := svc.List(true, 1, 100)
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("expected 2 active keys, got %d", len(active))
	}
	for _, k := range active {
		if !k.IsActive {
			t.Errorf("expected IsActive true in active-only list")
		}
	}

	// pagination
	page1, err := svc.List(false, 1, 2)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 keys on page 1, got %d", len(page1))
	}
	page2, err := svc.List(false, 2, 2)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected 1 key on page 2, got %d", len(page2))
	}

	// deactivate one and check active count
	if err := svc.Deactivate("k-l1"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	activeAfter, err := svc.List(true, 1, 100)
	if err != nil {
		t.Fatalf("list active after deactivation: %v", err)
	}
	if len(activeAfter) != 1 {
		t.Fatalf("expected 1 active key after deactivation, got %d", len(activeAfter))
	}
}
