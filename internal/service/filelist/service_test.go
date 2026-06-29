package filelist

import (
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// setupTestDB creates an in-memory SQLite database with all required tables
// initialized, returning the query DB handle. The database is automatically
// closed when the test finishes.
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

// insertFile inserts a file metadata row with the given path, name and size.
// The created_at timestamp is set explicitly so sort-by-created_at tests are
// deterministic even when many rows are inserted in quick succession.
func insertFile(t *testing.T, db *database.DB, path, name string, size int64, createdAt string) {
	t.Helper()
	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            path,
		Name:            name,
		Size:            size,
		Hash:            "",
		StorageType:     "local",
		StorageLocation: "",
		CreatedAt:       createdAt,
		UpdatedAt:       createdAt,
		IsDeleted:       false,
	}
	if err := database.NewFileMetadataService(db).Create(meta); err != nil {
		t.Fatalf("failed to create file metadata %q: %v", path, err)
	}
}

// insertDir inserts a directory metadata row.
func insertDir(t *testing.T, db *database.DB, path, name, createdAt string) {
	t.Helper()
	meta := &database.DirectoryMetadata{
		ID:        utils.GenerateUUID(),
		Path:      path,
		Name:      name,
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
		IsDeleted: false,
	}
	if err := database.NewDirectoryMetadataService(db).Create(meta); err != nil {
		t.Fatalf("failed to create directory metadata %q: %v", path, err)
	}
}

// namesFromItems returns a slice of the Name field from each item.
func namesFromItems(items []FileListItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Name)
	}
	return out
}

func TestListFiles_Empty(t *testing.T) {
	db := setupTestDB(t)
	svc := NewFileListService(db)

	result, err := svc.ListFiles("", false, 1, 10, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles failed on empty database: %v", err)
	}
	if result.Total != 0 {
		t.Errorf("expected Total=0 on empty database, got %d", result.Total)
	}
	if result.Page != 1 {
		t.Errorf("expected Page=1, got %d", result.Page)
	}
	if result.PageSize != 10 {
		t.Errorf("expected PageSize=10, got %d", result.PageSize)
	}
	if len(result.Items) != 0 {
		t.Errorf("expected empty Items, got %d items: %v", len(result.Items), result.Items)
	}
}

func TestListFiles_WithFiles(t *testing.T) {
	db := setupTestDB(t)
	svc := NewFileListService(db)

	ts := utils.GetCurrentTimestamp()
	insertFile(t, db, "alpha.txt", "alpha.txt", 100, ts)
	insertFile(t, db, "beta.txt", "beta.txt", 200, ts)
	insertDir(t, db, "subdir", "subdir", ts)

	result, err := svc.ListFiles("", false, 1, 50, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("expected Total=3, got %d", result.Total)
	}
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items, got %d: %v", len(result.Items), result.Items)
	}

	gotNames := namesFromItems(result.Items)
	wantNames := []string{"alpha.txt", "beta.txt", "subdir"}
	for _, want := range wantNames {
		found := false
		for _, got := range gotNames {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected item %q in result, got %v", want, gotNames)
		}
	}

	// Verify the file vs directory types are reported correctly.
	types := make(map[string]string, 3)
	for _, it := range result.Items {
		types[it.Name] = it.Type
	}
	if types["alpha.txt"] != "file" {
		t.Errorf("alpha.txt should be type %q, got %q", "file", types["alpha.txt"])
	}
	if types["subdir"] != "directory" {
		t.Errorf("subdir should be type %q, got %q", "directory", types["subdir"])
	}

	// Verify file size is reported and directory size is zero.
	for _, it := range result.Items {
		if it.Name == "alpha.txt" && it.Size != 100 {
			t.Errorf("alpha.txt size = %d, want 100", it.Size)
		}
		if it.Name == "subdir" && it.Size != 0 {
			t.Errorf("subdir size = %d, want 0", it.Size)
		}
	}
}

func TestListFiles_Pagination(t *testing.T) {
	db := setupTestDB(t)
	svc := NewFileListService(db)

	// Insert 5 files + 5 directories = 10 top-level entries.
	ts := utils.GetCurrentTimestamp()
	for i := 0; i < 5; i++ {
		name := "file" + string(rune('a'+i)) + ".txt"
		insertFile(t, db, name, name, int64(10+i), ts)
	}
	for i := 0; i < 5; i++ {
		name := "dir" + string(rune('a'+i))
		insertDir(t, db, name, name, ts)
	}

	// Page 1 with pageSize=3 → 3 items, total 10.
	page1, err := svc.ListFiles("", false, 1, 3, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 1 failed: %v", err)
	}
	if page1.Total != 10 {
		t.Errorf("expected Total=10, got %d", page1.Total)
	}
	if page1.Page != 1 || page1.PageSize != 3 {
		t.Errorf("page/pageSize = %d/%d, want 1/3", page1.Page, page1.PageSize)
	}
	if len(page1.Items) != 3 {
		t.Errorf("expected 3 items on page 1, got %d", len(page1.Items))
	}

	// Page 4 with pageSize=3 → only 1 item (10 - 3*3 = 1).
	page4, err := svc.ListFiles("", false, 4, 3, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 4 failed: %v", err)
	}
	if page4.Total != 10 {
		t.Errorf("expected Total=10, got %d", page4.Total)
	}
	if len(page4.Items) != 1 {
		t.Errorf("expected 1 item on page 4, got %d: %v", len(page4.Items), page4.Items)
	}

	// Page 5 with pageSize=3 → 0 items (already returned all 10).
	page5, err := svc.ListFiles("", false, 5, 3, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 5 failed: %v", err)
	}
	if len(page5.Items) != 0 {
		t.Errorf("expected 0 items on page 5, got %d: %v", len(page5.Items), page5.Items)
	}

	// Pages must not overlap: page1 item names and page2 item names are disjoint.
	page2, err := svc.ListFiles("", false, 2, 3, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 2 failed: %v", err)
	}
	page1Names := map[string]bool{}
	for _, it := range page1.Items {
		page1Names[it.Name] = true
	}
	for _, it := range page2.Items {
		if page1Names[it.Name] {
			t.Errorf("item %q appears on both page 1 and page 2", it.Name)
		}
	}

	// page < 1 should be normalized to page 1.
	pageZero, err := svc.ListFiles("", false, 0, 3, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 0 failed: %v", err)
	}
	if pageZero.Page != 1 {
		t.Errorf("page 0 should be normalized to 1, got Page=%d", pageZero.Page)
	}

	// pageSize < 1 should be normalized to 20.
	pageNoSize, err := svc.ListFiles("", false, 1, 0, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles pageSize 0 failed: %v", err)
	}
	if pageNoSize.PageSize != 20 {
		t.Errorf("pageSize 0 should be normalized to 20, got PageSize=%d", pageNoSize.PageSize)
	}
}

func TestListFiles_SortBy(t *testing.T) {
	db := setupTestDB(t)
	svc := NewFileListService(db)

	// Use distinct timestamps so created_at ordering is deterministic.
	insertFile(t, db, "c.txt", "c.txt", 100, "2024-01-03T00:00:00Z")
	insertFile(t, db, "a.txt", "a.txt", 300, "2024-01-01T00:00:00Z")
	insertFile(t, db, "b.txt", "b.txt", 200, "2024-01-02T00:00:00Z")

	cases := []struct {
		name      string
		sortBy    string
		sortOrder string
		wantNames []string
	}{
		{"name asc", "name", "asc", []string{"a.txt", "b.txt", "c.txt"}},
		{"name desc", "name", "desc", []string{"c.txt", "b.txt", "a.txt"}},
		{"size asc", "size", "asc", []string{"c.txt", "b.txt", "a.txt"}},
		{"size desc", "size", "desc", []string{"a.txt", "b.txt", "c.txt"}},
		{"created_at asc", "created_at", "asc", []string{"a.txt", "b.txt", "c.txt"}},
		{"created_at desc", "created_at", "desc", []string{"c.txt", "b.txt", "a.txt"}},
		{"unknown sort column falls back to name", "nonexistent", "asc", []string{"a.txt", "b.txt", "c.txt"}},
		{"sort order case-insensitive desc", "name", "DESC", []string{"c.txt", "b.txt", "a.txt"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := svc.ListFiles("", false, 1, 10, tc.sortBy, tc.sortOrder)
			if err != nil {
				t.Fatalf("ListFiles sortBy=%s order=%s failed: %v", tc.sortBy, tc.sortOrder, err)
			}
			if result.Total != 3 {
				t.Fatalf("expected Total=3, got %d", result.Total)
			}
			gotNames := namesFromItems(result.Items)
			if len(gotNames) != len(tc.wantNames) {
				t.Fatalf("expected %d items, got %d: %v", len(tc.wantNames), len(gotNames), gotNames)
			}
			for i, want := range tc.wantNames {
				if gotNames[i] != want {
					t.Errorf("position %d: want %q, got %q (full=%v)", i, want, gotNames[i], gotNames)
				}
			}
		})
	}
}

func TestListFiles_Recursive(t *testing.T) {
	db := setupTestDB(t)
	svc := NewFileListService(db)

	ts := utils.GetCurrentTimestamp()
	// Top-level file (should NOT appear when querying path "a").
	insertFile(t, db, "root.txt", "root.txt", 1, ts)
	// Directory "a" itself. With ListFiles("a", ...) the prefix is "a/", so the
	// directory row whose path is exactly "a" should NOT be returned either.
	insertDir(t, db, "a", "a", ts)

	// Direct children of "a".
	insertFile(t, db, "a/file1.txt", "file1.txt", 10, ts)
	insertDir(t, db, "a/sub", "sub", ts)
	// Deeper descendants.
	insertFile(t, db, "a/sub/file2.txt", "file2.txt", 20, ts)
	insertDir(t, db, "a/sub/nested", "nested", ts)
	insertFile(t, db, "a/sub/nested/file3.txt", "file3.txt", 30, ts)

	// Non-recursive listing of "a": only direct children of "a".
	nonRec, err := svc.ListFiles("a", false, 1, 50, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles non-recursive failed: %v", err)
	}
	nonRecPaths := make(map[string]bool, len(nonRec.Items))
	for _, it := range nonRec.Items {
		nonRecPaths[it.Path] = true
	}
	if nonRec.Total != 2 {
		t.Errorf("non-recursive Total = %d, want 2 (direct children only); items=%v", nonRec.Total, nonRec.Items)
	}
	if !nonRecPaths["a/file1.txt"] {
		t.Errorf("non-recursive result missing direct child a/file1.txt; got %v", nonRecPaths)
	}
	if !nonRecPaths["a/sub"] {
		t.Errorf("non-recursive result missing direct child a/sub; got %v", nonRecPaths)
	}
	// Deeper paths must not appear in non-recursive listing.
	for _, deep := range []string{"a/sub/file2.txt", "a/sub/nested", "a/sub/nested/file3.txt"} {
		if nonRecPaths[deep] {
			t.Errorf("non-recursive listing should not include deeper path %q", deep)
		}
	}

	// Recursive listing of "a": all descendants under "a/".
	rec, err := svc.ListFiles("a", true, 1, 50, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles recursive failed: %v", err)
	}
	recPaths := make(map[string]bool, len(rec.Items))
	for _, it := range rec.Items {
		recPaths[it.Path] = true
	}
	// Expect 5 descendants: file1.txt, sub, sub/file2.txt, sub/nested, sub/nested/file3.txt.
	if rec.Total != 5 {
		t.Errorf("recursive Total = %d, want 5; items=%v", rec.Total, rec.Items)
	}
	expected := []string{
		"a/file1.txt",
		"a/sub",
		"a/sub/file2.txt",
		"a/sub/nested",
		"a/sub/nested/file3.txt",
	}
	for _, p := range expected {
		if !recPaths[p] {
			t.Errorf("recursive result missing %q; got %v", p, recPaths)
		}
	}
}
