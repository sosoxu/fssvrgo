package filelist

import (
	"strings"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type FileListItem struct {
	ID        string
	Path      string
	Name      string
	Size      int64
	Type      string
	CreatedAt string
}

// FileListResult holds a page of list results.
//
// Total semantics: when includeTotal is false (the default), Total is -1 to
// signal "not computed" — clients that only need pagination should use HasMore
// instead of Total. When includeTotal is true, Total is the exact count of all
// items matching the filter (computed via a separate COUNT query).
type FileListResult struct {
	Total    int  // -1 when not computed (includeTotal=false); exact count otherwise
	Page     int
	PageSize int
	HasMore  bool // true if there are more items beyond this page
	Items    []FileListItem
}

// Listing is the data-access seam of this service. The SQL lives behind it
// (database.FileListStore); the service itself is pagination and validation
// logic, so unit tests can drive it with an in-memory fake instead of a
// PostgreSQL instance.
type Listing interface {
	List(query database.FileListQuery) ([]database.FileListRow, error)
	Count(query database.FileListQuery) (int, error)
}

type FileListService struct {
	store Listing
}

func NewFileListService(store Listing) *FileListService {
	return &FileListService{store: store}
}

// NewFileListServiceFromDB wires the service to the SQL-backed store. Use this
// in production wiring; tests can pass a fake Listing instead.
func NewFileListServiceFromDB(db *database.DB) *FileListService {
	return NewFileListService(database.NewFileListStore(db))
}

// ListFiles lists files and directories under path with pagination. It does NOT
// compute Total (Total=-1); use ListFilesWithTotal if you need the exact count.
// HasMore is set so callers can paginate without Total.
func (s *FileListService) ListFiles(path string, recursive bool, page, pageSize int, sortBy, sortOrder string) (*FileListResult, error) {
	return s.listFiles(path, recursive, page, pageSize, sortBy, sortOrder, false)
}

// ListFilesWithTotal is like ListFiles but also computes the exact Total via a
// COUNT query. Use this only when the caller genuinely needs the total (e.g. a
// UI showing "N items"); it costs an extra full-scan COUNT on large directories.
func (s *FileListService) ListFilesWithTotal(path string, recursive bool, page, pageSize int, sortBy, sortOrder string) (*FileListResult, error) {
	return s.listFiles(path, recursive, page, pageSize, sortBy, sortOrder, true)
}

func (s *FileListService) listFiles(path string, recursive bool, page, pageSize int, sortBy, sortOrder string, includeTotal bool) (*FileListResult, error) {
	path = utils.NormalizePath(path)
	if path == "." {
		path = ""
	}
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}

	allowedSort := map[string]bool{
		"name": true, "path": true, "size": true, "created_at": true, "type": true,
	}
	if !allowedSort[sortBy] {
		sortBy = "name"
	}
	order := "ASC"
	if strings.EqualFold(sortOrder, "desc") {
		order = "DESC"
	}

	// Fetch one extra row (pageSize+1) to determine HasMore without a COUNT.
	// This is O(pageSize) rather than O(N) for the common case where the caller
	// does not need an exact total.
	query := database.FileListQuery{
		Path:      path,
		Recursive: recursive,
		SortBy:    sortBy,
		SortOrder: order,
		Limit:     pageSize + 1,
		Offset:    (page - 1) * pageSize,
	}

	rows, err := s.store.List(query)
	if err != nil {
		return nil, err
	}

	items := make([]FileListItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, FileListItem{
			ID:        row.ID,
			Path:      row.Path,
			Name:      row.Name,
			Size:      row.Size,
			Type:      row.Type,
			CreatedAt: row.CreatedAt,
		})
	}

	hasMore := len(items) > pageSize
	if hasMore {
		// Trim the extra row we fetched only to detect HasMore.
		items = items[:pageSize]
	}

	total := -1 // -1 signals "not computed"
	if includeTotal {
		exact, err := s.store.Count(query)
		if err != nil {
			return nil, err
		}
		total = exact
	}

	if items == nil {
		items = []FileListItem{}
	}

	return &FileListResult{
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		HasMore:  hasMore,
		Items:    items,
	}, nil
}
