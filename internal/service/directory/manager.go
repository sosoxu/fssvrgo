package directory

import (
	"fmt"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type DirectoryManager struct {
	db *database.DB
}

func NewDirectoryManager(db *database.DB) *DirectoryManager {
	return &DirectoryManager{db: db}
}

func (dm *DirectoryManager) CreateDirectory(path string) error {
	path = utils.NormalizePath(path)
	if dm.Exists(path) {
		return fmt.Errorf("directory already exists: %s", path)
	}

	now := utils.GetCurrentTimestamp()
	name := utils.GetFileName(path)

	meta := &database.DirectoryMetadata{
		ID:        utils.GenerateUUID(),
		Path:      path,
		Name:      name,
		CreatedAt: now,
		UpdatedAt: now,
		IsDeleted: false,
	}

	return database.NewDirectoryMetadataService(dm.db).Create(meta)
}

func (dm *DirectoryManager) DeleteDirectory(path string, recursive bool) error {
	path = utils.NormalizePath(path)
	if !dm.Exists(path) {
		return fmt.Errorf("directory not found: %s", path)
	}

	if !recursive {
		prefix := path + "/%"
		var fileCount int
		err := dm.db.QueryRow("SELECT COUNT(*) FROM files WHERE path LIKE ? AND is_deleted = FALSE", prefix).Scan(&fileCount)
		if err != nil {
			return fmt.Errorf("failed to check directory contents: %w", err)
		}
		if fileCount > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}
		var dirCount int
		err = dm.db.QueryRow("SELECT COUNT(*) FROM directories WHERE path LIKE ? AND is_deleted = FALSE", prefix).Scan(&dirCount)
		if err != nil {
			return fmt.Errorf("failed to check directory contents: %w", err)
		}
		if dirCount > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}

		meta, err := database.NewDirectoryMetadataService(dm.db).GetByPath(path)
		if err != nil {
			return fmt.Errorf("failed to get directory metadata: %w", err)
		}
		if meta == nil {
			return fmt.Errorf("directory not found: %s", path)
		}
		return database.NewDirectoryMetadataService(dm.db).Remove(meta.ID)
	}

	const batchSize = 500
	prefix := path + "/"

	for {
		rows, err := dm.db.Query("SELECT id FROM files WHERE path LIKE ? AND is_deleted = FALSE LIMIT ?", prefix+"%", batchSize)
		if err != nil {
			return fmt.Errorf("failed to query files: %w", err)
		}

		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan file id: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()

		if len(ids) == 0 {
			break
		}

		svc := database.NewFileMetadataService(dm.db)
		for _, id := range ids {
			if err := svc.Remove(id); err != nil {
				return fmt.Errorf("failed to delete file metadata: %w", err)
			}
		}
	}

	for {
		rows, err := dm.db.Query("SELECT id FROM directories WHERE path LIKE ? AND is_deleted = FALSE LIMIT ?", prefix+"%", batchSize)
		if err != nil {
			return fmt.Errorf("failed to query directories: %w", err)
		}

		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan directory id: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()

		if len(ids) == 0 {
			break
		}

		svc := database.NewDirectoryMetadataService(dm.db)
		for _, id := range ids {
			if err := svc.Remove(id); err != nil {
				return fmt.Errorf("failed to delete directory metadata: %w", err)
			}
		}
	}

	meta, err := database.NewDirectoryMetadataService(dm.db).GetByPath(path)
	if err != nil {
		return fmt.Errorf("failed to get directory metadata: %w", err)
	}
	if meta == nil {
		return fmt.Errorf("directory not found: %s", path)
	}
	return database.NewDirectoryMetadataService(dm.db).Remove(meta.ID)
}

func (dm *DirectoryManager) RenameDirectory(oldPath, newName string) error {
	oldPath = utils.NormalizePath(oldPath)

	meta, err := dm.GetDirectoryMetadata(oldPath)
	if err != nil {
		return err
	}

	newPath := utils.NormalizePath(utils.GetDirectory(oldPath) + "/" + newName)

	if dm.Exists(newPath) {
		return fmt.Errorf("target path already exists: %s", newPath)
	}

	rows, err := dm.db.Query("SELECT id, path FROM files WHERE path LIKE ? AND is_deleted = FALSE", oldPath+"/%")
	if err != nil {
		return fmt.Errorf("failed to query child files: %w", err)
	}

	type pathEntry struct {
		id   string
		path string
	}
	var fileEntries []pathEntry
	for rows.Next() {
		var e pathEntry
		if err := rows.Scan(&e.id, &e.path); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan file path: %w", err)
		}
		fileEntries = append(fileEntries, e)
	}
	rows.Close()

	now := utils.GetCurrentTimestamp()
	for _, e := range fileEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		newItemName := utils.GetFileName(newItemPath)
		_, err := dm.db.Exec("UPDATE files SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id)
		if err != nil {
			return fmt.Errorf("failed to update file path: %w", err)
		}
	}

	dirRows, err := dm.db.Query("SELECT id, path FROM directories WHERE path LIKE ? AND is_deleted = FALSE", oldPath+"/%")
	if err != nil {
		return fmt.Errorf("failed to query child directories: %w", err)
	}

	var dirEntries []pathEntry
	for dirRows.Next() {
		var e pathEntry
		if err := dirRows.Scan(&e.id, &e.path); err != nil {
			dirRows.Close()
			return fmt.Errorf("failed to scan directory path: %w", err)
		}
		dirEntries = append(dirEntries, e)
	}
	dirRows.Close()

	for _, e := range dirEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		newItemName := utils.GetFileName(newItemPath)
		_, err := dm.db.Exec("UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id)
		if err != nil {
			return fmt.Errorf("failed to update directory path: %w", err)
		}
	}

	meta.Path = newPath
	meta.Name = newName
	meta.UpdatedAt = now

	return database.NewDirectoryMetadataService(dm.db).Update(meta)
}

func (dm *DirectoryManager) GetDirectoryMetadata(path string) (*database.DirectoryMetadata, error) {
	path = utils.NormalizePath(path)
	meta, err := database.NewDirectoryMetadataService(dm.db).GetByPath(path)
	if err != nil {
		return nil, fmt.Errorf("directory metadata not found: %w", err)
	}
	if meta == nil {
		return nil, fmt.Errorf("directory not found: %s", path)
	}
	return meta, nil
}

func (dm *DirectoryManager) Exists(path string) bool {
	path = utils.NormalizePath(path)
	exists, err := database.NewDirectoryMetadataService(dm.db).Exists(path)
	if err != nil {
		return false
	}
	return exists
}
