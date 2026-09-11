package database

import (
	"fmt"
)

// FileListQuery describes a file/directory listing in domain terms: the caller
// says what it wants, not how to build the SQL.
type FileListQuery struct {
	Path      string // normalized absolute path; "" means the root
	Recursive bool
	SortBy    string // whitelisted by the service before it reaches SQL
	SortOrder string // "ASC" or "DESC"
	Limit     int
	Offset    int
}

// FileListRow is one entry of a listing: a file or a directory.
type FileListRow struct {
	ID        string
	Path      string
	Name      string
	Size      int64
	Type      string
	CreatedAt string
}

// FileListStore runs the listing queries that back the file-list service.
//
// Keeping the SQL here (rather than in the service) is what lets the service be
// a small piece of pure pagination/validation logic that unit tests can drive
// through a fake, with no database in sight.
type FileListStore struct {
	db *DB
}

func NewFileListStore(db *DB) *FileListStore {
	return &FileListStore{db: db}
}

// List returns up to query.Limit rows starting at query.Offset.
func (s *FileListStore) List(query FileListQuery) ([]FileListRow, error) {
	whereClause, args, err := fileListWhere(query)
	if err != nil {
		return nil, err
	}

	itemsQuery := fmt.Sprintf(
		`SELECT id, path, name, size, 'file' AS type, created_at FROM files WHERE %s
		UNION ALL
		SELECT id, path, name, 0 AS size, 'directory' AS type, created_at FROM directories WHERE %s
		ORDER BY %s %s LIMIT ? OFFSET ?`,
		whereClause, whereClause, query.SortBy, query.SortOrder,
	)

	itemsArgs := make([]interface{}, 0, len(args)*2+2)
	itemsArgs = append(itemsArgs, args...)
	itemsArgs = append(itemsArgs, args...)
	itemsArgs = append(itemsArgs, query.Limit, query.Offset)

	rows, err := s.db.Query(itemsQuery, itemsArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to list items: %w", err)
	}
	defer rows.Close()

	var items []FileListRow
	for rows.Next() {
		var item FileListRow
		if err := rows.Scan(&item.ID, &item.Path, &item.Name, &item.Size, &item.Type, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read items: %w", err)
	}
	return items, nil
}

// Count returns the exact number of rows matching the query, ignoring
// Limit/Offset.
func (s *FileListStore) Count(query FileListQuery) (int, error) {
	whereClause, args, err := fileListWhere(query)
	if err != nil {
		return 0, err
	}

	countQuery := fmt.Sprintf(
		`SELECT COUNT(*) FROM (SELECT id FROM files WHERE %s UNION ALL SELECT id FROM directories WHERE %s) AS t`,
		whereClause, whereClause,
	)
	countArgs := make([]interface{}, 0, len(args)*2)
	countArgs = append(countArgs, args...)
	countArgs = append(countArgs, args...)

	var total int
	if err := s.db.QueryRow(countQuery, countArgs...).Scan(&total); err != nil {
		return 0, fmt.Errorf("failed to count items: %w", err)
	}
	return total, nil
}

// fileListWhere builds the shared WHERE clause and its arguments.
func fileListWhere(query FileListQuery) (string, []interface{}, error) {
	switch {
	case query.SortBy == "" || query.SortOrder == "":
		return "", nil, fmt.Errorf("file list query requires sortBy and sortOrder")
	case query.Path == "" && query.Recursive:
		return "is_deleted = FALSE", nil, nil
	case query.Path == "":
		return "is_deleted = FALSE AND path NOT LIKE '%/%'", nil, nil
	}

	escapedPrefix := escapeLikePattern(query.Path + "/")
	if query.Recursive {
		return "is_deleted = FALSE AND path LIKE ? ESCAPE '\\'",
			[]interface{}{escapedPrefix + "%"}, nil
	}
	return "is_deleted = FALSE AND path LIKE ? ESCAPE '\\' AND path NOT LIKE ? ESCAPE '\\'",
		[]interface{}{escapedPrefix + "%", escapedPrefix + "%/%"}, nil
}
