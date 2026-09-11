package filelist

import (
	"fmt"
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

type FileListService struct {
	db *database.DB
}

func NewFileListService(db *database.DB) *FileListService {
	return &FileListService{db: db}
}

func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
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

	var whereClause string
	var args []interface{}

	if path == "" {
		if recursive {
			whereClause = "is_deleted = FALSE"
		} else {
			whereClause = "is_deleted = FALSE AND path NOT LIKE '%/%'"
		}
	} else {
		prefix := path + "/"
		escapedPrefix := escapeLikePattern(prefix)
		if recursive {
			whereClause = "is_deleted = FALSE AND path LIKE ? ESCAPE '\\'"
			args = append(args, escapedPrefix+"%")
		} else {
			whereClause = "is_deleted = FALSE AND path LIKE ? ESCAPE '\\' AND path NOT LIKE ? ESCAPE '\\'"
			args = append(args, escapedPrefix+"%", escapedPrefix+"%/%")
		}
	}

	// Fetch one extra row (pageSize+1) to determine HasMore without a COUNT.
	// This is O(pageSize) rather than O(N) for the common case where the caller
	// does not need an exact total.
	fetchLimit := pageSize + 1
	offset := (page - 1) * pageSize

	itemsQuery := fmt.Sprintf(
		`SELECT id, path, name, size, 'file' AS type, created_at FROM files WHERE %s
		UNION ALL
		SELECT id, path, name, 0 AS size, 'directory' AS type, created_at FROM directories WHERE %s
		ORDER BY %s %s LIMIT ? OFFSET ?`,
		whereClause, whereClause, sortBy, order,
	)

	itemsArgs := make([]interface{}, 0, len(args)*2+2)
	itemsArgs = append(itemsArgs, args...)
	itemsArgs = append(itemsArgs, args...)
	itemsArgs = append(itemsArgs, fetchLimit, offset)

	rows, err := s.db.Query(itemsQuery, itemsArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to list items: %w", err)
	}
	defer rows.Close()

	var items []FileListItem
	for rows.Next() {
		var item FileListItem
		if err := rows.Scan(&item.ID, &item.Path, &item.Name, &item.Size, &item.Type, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan item: %w", err)
		}
		items = append(items, item)
	}

	hasMore := len(items) > pageSize
	if hasMore {
		// Trim the extra row we fetched only to detect HasMore.
		items = items[:pageSize]
	}

	total := -1 // -1 signals "not computed"
	if includeTotal {
		countQuery := fmt.Sprintf(
			`SELECT COUNT(*) FROM (SELECT id FROM files WHERE %s UNION ALL SELECT id FROM directories WHERE %s) AS t`,
			whereClause, whereClause,
		)
		countArgs := make([]interface{}, 0, len(args)*2)
		countArgs = append(countArgs, args...)
		countArgs = append(countArgs, args...)
		if err := s.db.QueryRow(countQuery, countArgs...).Scan(&total); err != nil {
			return nil, fmt.Errorf("failed to count items: %w", err)
		}
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
