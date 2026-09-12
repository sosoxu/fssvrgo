package database

import (
	"context"
	"fmt"
)

// PathEntry identifies a row by id and path. The cascade operations behind
// directory delete/rename only need those two columns, so the store returns
// them instead of full metadata rows.
type PathEntry struct {
	ID   string
	Path string
}

// PathUpdate is a single path rewrite: the row's new path plus the file or
// directory name derived from it.
type PathUpdate struct {
	ID   string
	Path string
	Name string
}

// DirectoryTreeStore owns the multi-row and transactional SQL behind
// directory deletion and rename. Keeping those statements here (rather than in
// the service) is what lets DirectoryManager be exercised through an in-memory
// fake, with no PostgreSQL instance in sight.
type DirectoryTreeStore struct {
	db *DB
}

func NewDirectoryTreeStore(db *DB) *DirectoryTreeStore {
	return &DirectoryTreeStore{db: db}
}

// CountChildren counts the live files and directories under path. It counts
// the whole subtree, not just direct children, which is what the non-recursive
// delete emptiness check needs.
func (s *DirectoryTreeStore) CountChildren(path string) (int, int, error) {
	prefix := escapeLikePattern(path+"/") + "%"

	var fileCount int
	if err := s.db.QueryRow(
		"SELECT COUNT(*) FROM files WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE",
		prefix,
	).Scan(&fileCount); err != nil {
		return 0, 0, fmt.Errorf("failed to count files in directory: %w", err)
	}

	var dirCount int
	if err := s.db.QueryRow(
		"SELECT COUNT(*) FROM directories WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE",
		prefix,
	).Scan(&dirCount); err != nil {
		return 0, 0, fmt.Errorf("failed to count subdirectories: %w", err)
	}

	return fileCount, dirCount, nil
}

// ListChildFiles returns live file rows under path. A limit <= 0 means "no
// limit"; a positive limit is how the recursive delete loop bounds one batch.
func (s *DirectoryTreeStore) ListChildFiles(path string, limit int) ([]PathEntry, error) {
	return s.listChildren("files", path, limit)
}

// ListChildDirectories is the directories counterpart of ListChildFiles.
func (s *DirectoryTreeStore) ListChildDirectories(path string, limit int) ([]PathEntry, error) {
	return s.listChildren("directories", path, limit)
}

func (s *DirectoryTreeStore) listChildren(table, path string, limit int) ([]PathEntry, error) {
	query := fmt.Sprintf("SELECT id, path FROM %s WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", table)
	args := []interface{}{escapeLikePattern(path+"/") + "%"}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", table, err)
	}
	defer rows.Close()

	var entries []PathEntry
	for rows.Next() {
		var e PathEntry
		if err := rows.Scan(&e.ID, &e.Path); err != nil {
			return nil, fmt.Errorf("failed to scan %s row: %w", table, err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate %s rows: %w", table, err)
	}
	return entries, nil
}

// SoftDeleteFiles marks one batch of file rows deleted in a single
// transaction, so a batch can never leave the DB half-deleted.
func (s *DirectoryTreeStore) SoftDeleteFiles(updatedAt string, entries []PathEntry) error {
	return s.softDelete("files", updatedAt, entries)
}

// SoftDeleteDirectories is the directories counterpart of SoftDeleteFiles.
func (s *DirectoryTreeStore) SoftDeleteDirectories(updatedAt string, entries []PathEntry) error {
	return s.softDelete("directories", updatedAt, entries)
}

func (s *DirectoryTreeStore) softDelete(table, updatedAt string, entries []PathEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin %s delete transaction: %w", table, err)
	}
	for _, e := range entries {
		if _, err := tx.Exec(
			fmt.Sprintf("UPDATE %s SET is_deleted = TRUE, updated_at = ? WHERE id = ?", table),
			updatedAt, e.ID,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to delete %s metadata: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit %s deletion batch: %w", table, err)
	}
	return nil
}

// RenameTree rewrites the paths of a renamed subtree — its files, its
// descendant directories, and the renamed directory row itself — in one
// transaction. Callers move storage objects only after this returns, so a
// failed commit leaves the DB untouched and no object moved.
func (s *DirectoryTreeStore) RenameTree(updatedAt string, files, dirs []PathUpdate, target PathUpdate) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin rename transaction: %w", err)
	}

	for _, u := range files {
		if _, err := tx.Exec(
			"UPDATE files SET path = ?, name = ?, updated_at = ? WHERE id = ?",
			u.Path, u.Name, updatedAt, u.ID,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update file path: %w", err)
		}
	}

	for _, u := range dirs {
		if _, err := tx.Exec(
			"UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?",
			u.Path, u.Name, updatedAt, u.ID,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update directory path: %w", err)
		}
	}

	if _, err := tx.Exec(
		"UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?",
		target.Path, target.Name, updatedAt, target.ID,
	); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update directory metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rename: %w", err)
	}
	return nil
}
