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

type FileListResult struct {
	Total    int
	Page     int
	PageSize int
	Items    []FileListItem
}

type FileListService struct {
	db *database.DB
}

func NewFileListService(db *database.DB) *FileListService {
	return &FileListService{db: db}
}

func (s *FileListService) ListFiles(path string, recursive bool, page, pageSize int, sortBy, sortOrder string) (*FileListResult, error) {
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
		if recursive {
			whereClause = "is_deleted = FALSE AND path LIKE ?"
			args = append(args, prefix+"%")
		} else {
			whereClause = "is_deleted = FALSE AND path LIKE ? AND path NOT LIKE ?"
			args = append(args, prefix+"%", prefix+"%/%")
		}
	}

	countQuery := fmt.Sprintf(
		`SELECT COUNT(*) FROM (SELECT id FROM files WHERE %s UNION ALL SELECT id FROM directories WHERE %s)`,
		whereClause, whereClause,
	)

	countArgs := make([]interface{}, 0, len(args)*2)
	countArgs = append(countArgs, args...)
	countArgs = append(countArgs, args...)

	var total int
	if err := s.db.QueryRow(countQuery, countArgs...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count items: %w", err)
	}

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
	itemsArgs = append(itemsArgs, pageSize, offset)

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

	if items == nil {
		items = []FileListItem{}
	}

	return &FileListResult{
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		Items:    items,
	}, nil
}
