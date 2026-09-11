package filelist

import (
	"fmt"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
)

// fakeStore is an in-memory Listing. These tests exercise the service's
// pagination and validation logic without a PostgreSQL instance — the point of
// the narrow interface introduced for issue #114.
type fakeStore struct {
	rows       []database.FileListRow
	queries    []database.FileListQuery
	countCalls int
}

func (f *fakeStore) List(query database.FileListQuery) ([]database.FileListRow, error) {
	f.queries = append(f.queries, query)
	if query.Offset >= len(f.rows) {
		return nil, nil
	}
	end := query.Offset + query.Limit
	if end > len(f.rows) {
		end = len(f.rows)
	}
	return f.rows[query.Offset:end], nil
}

func (f *fakeStore) Count(database.FileListQuery) (int, error) {
	f.countCalls++
	return len(f.rows), nil
}

func newFakeStore(n int) *fakeStore {
	rows := make([]database.FileListRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, database.FileListRow{
			ID:   fmt.Sprintf("id%02d", i),
			Path: fmt.Sprintf("/f%02d.txt", i),
			Name: fmt.Sprintf("f%02d.txt", i),
			Type: "file",
		})
	}
	return &fakeStore{rows: rows}
}

func TestServicePaginationUsesFetchLimitAndReportsHasMore(t *testing.T) {
	store := newFakeStore(25)
	svc := NewFileListService(store)

	page1, err := svc.ListFiles("/", false, 1, 10, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 1: %v", err)
	}
	if len(page1.Items) != 10 || !page1.HasMore {
		t.Fatalf("page 1: items=%d hasMore=%v, want 10/true", len(page1.Items), page1.HasMore)
	}
	if page1.Total != -1 {
		t.Errorf("Total = %d, want -1 when includeTotal is false", page1.Total)
	}
	// The service must ask for one extra row so HasMore needs no COUNT.
	if got := store.queries[0]; got.Limit != 11 || got.Offset != 0 {
		t.Errorf("store query = limit %d offset %d, want limit 11 offset 0", got.Limit, got.Offset)
	}

	page3, err := svc.ListFiles("/", false, 3, 10, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 3: %v", err)
	}
	if len(page3.Items) != 5 || page3.HasMore {
		t.Fatalf("page 3: items=%d hasMore=%v, want 5/false", len(page3.Items), page3.HasMore)
	}
	if got := store.queries[1]; got.Offset != 20 || got.Limit != 11 {
		t.Errorf("page 3 query = limit %d offset %d, want limit 11 offset 20", got.Limit, got.Offset)
	}
}

func TestServiceNormalizesSortInput(t *testing.T) {
	store := newFakeStore(1)
	svc := NewFileListService(store)

	if _, err := svc.ListFiles("/", false, 1, 10, "evil; DROP TABLE files", "asc"); err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if got := store.queries[0]; got.SortBy != "name" || got.SortOrder != "ASC" {
		t.Errorf("unexpected normalized sort: %q %q, want name ASC", got.SortBy, got.SortOrder)
	}

	if _, err := svc.ListFiles("/", false, 1, 10, "size", "DESC"); err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if got := store.queries[1]; got.SortBy != "size" || got.SortOrder != "DESC" {
		t.Errorf("unexpected sort: %q %q, want size DESC", got.SortBy, got.SortOrder)
	}
}

func TestServiceIncludeTotalAsksStoreForCount(t *testing.T) {
	store := newFakeStore(25)
	svc := NewFileListService(store)

	result, err := svc.ListFilesWithTotal("/", false, 1, 10, "name", "asc")
	if err != nil {
		t.Fatalf("ListFilesWithTotal: %v", err)
	}
	if result.Total != 25 {
		t.Errorf("Total = %d, want 25", result.Total)
	}
	if store.countCalls != 1 {
		t.Errorf("Count called %d times, want 1", store.countCalls)
	}
}
