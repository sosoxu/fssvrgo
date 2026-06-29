package directory

import (
	"fmt"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type DirectoryManager struct {
	db    *database.DB
	store storage.StorageAdapter
}

func NewDirectoryManager(db *database.DB) *DirectoryManager {
	return &DirectoryManager{db: db}
}

// NewDirectoryManagerWithStore creates a DirectoryManager that also synchronizes
// storage objects when deleting or renaming directories.
func NewDirectoryManagerWithStore(db *database.DB, store storage.StorageAdapter) *DirectoryManager {
	return &DirectoryManager{db: db, store: store}
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

	if err := database.NewDirectoryMetadataService(dm.db).Create(meta); err != nil {
		return err
	}

	// Create the directory marker in the storage backend so that the directory
	// is visible to store-level operations (Exists/List) on both backends.
	// On MinIO this creates a 0-byte "<path>/" marker object; on LocalStorage
	// this creates the physical directory. Failure is best-effort: the DB
	// record is the source of truth and LocalStorage will also auto-create the
	// directory on first file write, so we only warn.
	if dm.store != nil {
		if err := dm.store.CreateDirectory(path); err != nil {
			logger.Warn("failed to create directory marker in storage for %s: %v", path, err)
		}
	}

	return nil
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
		rows, err := dm.db.Query("SELECT id, path FROM files WHERE path LIKE ? AND is_deleted = FALSE LIMIT ?", prefix+"%", batchSize)
		if err != nil {
			return fmt.Errorf("failed to query files: %w", err)
		}

		type fileEntry struct {
			id   string
			path string
		}
		var entries []fileEntry
		for rows.Next() {
			var e fileEntry
			if err := rows.Scan(&e.id, &e.path); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan file id: %w", err)
			}
			entries = append(entries, e)
		}
		rows.Close()

		if len(entries) == 0 {
			break
		}

		svc := database.NewFileMetadataService(dm.db)
		for _, e := range entries {
			// Remove the storage object before soft-deleting the DB record.
			if dm.store != nil {
				if err := dm.store.Remove(e.path); err != nil {
					// Log but continue - the DB record is the source of truth for metadata.
				}
			}
			if err := svc.Remove(e.id); err != nil {
				return fmt.Errorf("failed to delete file metadata: %w", err)
			}
		}
	}

	for {
		rows, err := dm.db.Query("SELECT id, path FROM directories WHERE path LIKE ? AND is_deleted = FALSE LIMIT ?", prefix+"%", batchSize)
		if err != nil {
			return fmt.Errorf("failed to query directories: %w", err)
		}

		type dirEntry struct {
			id   string
			path string
		}
		var entries []dirEntry
		for rows.Next() {
			var e dirEntry
			if err := rows.Scan(&e.id, &e.path); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan directory id: %w", err)
			}
			entries = append(entries, e)
		}
		rows.Close()

		if len(entries) == 0 {
			break
		}

		svc := database.NewDirectoryMetadataService(dm.db)
		for _, e := range entries {
			// Remove the storage directory if supported.
			if dm.store != nil {
				if err := dm.store.RemoveDirectory(e.path); err != nil {
					// Best effort - some backends treat empty dirs as no-ops.
				}
			}
			if err := svc.Remove(e.id); err != nil {
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
		// Move the storage object before updating the DB record.
		if dm.store != nil {
			if err := dm.store.Rename(e.path, newItemPath); err != nil {
				return fmt.Errorf("failed to rename storage object %s -> %s: %w", e.path, newItemPath, err)
			}
		}
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
		// Storage directories are logical; Rename on the store is best-effort for non-leaf dirs.
		_, err := dm.db.Exec("UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id)
		if err != nil {
			return fmt.Errorf("failed to update directory path: %w", err)
		}
	}

	// Move the target directory's own storage object (if it has one).
	if dm.store != nil {
		if err := dm.store.Rename(oldPath, newPath); err != nil {
			// Best-effort: directory entries may not have a physical storage object.
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
