package directory

import (
	"context"
	"fmt"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type DirectoryManager struct {
	db       *database.DB
	store    storage.StorageAdapter
	distLock distributed.DistributedLock
}

func NewDirectoryManager(db *database.DB) *DirectoryManager {
	return &DirectoryManager{db: db}
}

// NewDirectoryManagerWithStore creates a DirectoryManager that also synchronizes
// storage objects when deleting or renaming directories.
func NewDirectoryManagerWithStore(db *database.DB, store storage.StorageAdapter) *DirectoryManager {
	return &DirectoryManager{db: db, store: store}
}

// NewDirectoryManagerWithDistLock creates a DirectoryManager with a distributed
// lock so that concurrent rename/delete operations on the same directory
// (including across instances) are serialized. Pass nil to disable locking
// (e.g. in single-process tests).
func NewDirectoryManagerWithDistLock(db *database.DB, store storage.StorageAdapter, distLock distributed.DistributedLock) *DirectoryManager {
	return &DirectoryManager{db: db, store: store, distLock: distLock}
}

// lockDirectory acquires a distributed lock for directory-level operations.
// It returns a release function that must be called (typically via defer) and
// an error if the lock could not be acquired. When no distLock is configured
// the release function is a no-op.
//
// The lock is acquired with renewal so that long-running directory operations
// (e.g. recursive delete/rename of large directories) do not lose the lock
// when the initial TTL expires. This matches the behavior of the upload
// completion path (see transfer.FileTransferService.CompleteUpload).
func (dm *DirectoryManager) lockDirectory(path string) (func(), error) {
	if dm.distLock == nil {
		return func() {}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	token, cancelRenew, err := distributed.AcquireLockWithRenewal(ctx, dm.distLock, "dir:"+path, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire directory lock for %s: %w", path, err)
	}
	return func() {
		cancelRenew()
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		if err := dm.distLock.Unlock(unlockCtx, "dir:"+path, token); err != nil {
			logger.Warn("failed to release directory lock for %s: %v", path, err)
		}
	}, nil
}

// supportsDirectoryStorageOps returns true only for backends where directory-
// level storage operations (mkdir / rename dir / remove dir) are atomic and
// cheap. Object storage (MinIO/S3) has no real directories — prefixes are
// implicit, so creating a 0-byte marker is meaningless, renaming a directory
// is an N-object copy+delete (non-atomic), and RemoveDirectory duplicates the
// per-file removal already done by recursive delete. For object storage we
// therefore keep the DB record as the single source of truth and only touch
// storage at the per-file granularity.
func (dm *DirectoryManager) supportsDirectoryStorageOps() bool {
	return dm.store != nil && dm.store.StorageType() == "local"
}

func (dm *DirectoryManager) CreateDirectory(path string) error {
	path = utils.NormalizePath(path)

	release, err := dm.lockDirectory(path)
	if err != nil {
		return err
	}
	defer release()

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
	// is visible to store-level operations (Exists/List) on the local backend.
	// Object storage (MinIO/S3) has no real directories — prefixes are
	// implicit, so a 0-byte marker object would be meaningless and is skipped.
	// The DB record is the source of truth; LocalStorage will also auto-create
	// the directory on first file write, so failure here is best-effort.
	if dm.supportsDirectoryStorageOps() {
		if err := dm.store.CreateDirectory(path); err != nil {
			logger.Warn("failed to create directory marker in storage for %s: %v", path, err)
		}
	}

	return nil
}

func (dm *DirectoryManager) DeleteDirectory(path string, recursive bool) error {
	path = utils.NormalizePath(path)

	release, err := dm.lockDirectory(path)
	if err != nil {
		return err
	}
	defer release()

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

	// Recursive deletion. The distributed lock held above serializes concurrent
	// delete/rename operations on this directory across instances. Within this
	// operation we delete in batches to bound memory, and wrap each batch's
	// metadata soft-deletes in a transaction so a batch cannot leave the DB in
	// a half-deleted state. Storage object removal happens after the batch's
	// transaction commits and is best-effort (a leaked object is recoverable via
	// the periodic cleanup service; a missing DB record after commit is not).
	const batchSize = 500
	prefix := path + "/"

	for {
		// Query the next batch outside the transaction — we only need the IDs
		// to soft-delete, and holding a long read transaction for large dirs
		// would hurt concurrency on SQLite.
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

		// Soft-delete the whole batch atomically.
		tx, txErr := dm.db.BeginTx(context.Background(), nil)
		if txErr != nil {
			return fmt.Errorf("failed to begin delete transaction: %w", txErr)
		}
		for _, e := range entries {
			if _, err := tx.Exec("UPDATE files SET is_deleted = TRUE, updated_at = ? WHERE id = ?", utils.GetCurrentTimestamp(), e.id); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to delete file metadata: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit file deletion batch: %w", err)
		}

		// Storage cleanup after the DB commit. Best-effort: a failure leaves an
		// orphan object that the cleanup service can reap later.
		for _, e := range entries {
			if dm.store != nil {
				if err := dm.store.Remove(e.path); err != nil {
					logger.Warn("failed to remove storage object %s during directory delete: %v", e.path, err)
				}
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

		tx, txErr := dm.db.BeginTx(context.Background(), nil)
		if txErr != nil {
			return fmt.Errorf("failed to begin delete directory transaction: %w", txErr)
		}
		for _, e := range entries {
			if _, err := tx.Exec("UPDATE directories SET is_deleted = TRUE, updated_at = ? WHERE id = ?", utils.GetCurrentTimestamp(), e.id); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to delete directory metadata: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit directory deletion batch: %w", err)
		}

		// Storage directory-marker removal is only meaningful for the local
		// backend (it removes a physical directory). For object storage there
		// are no real directories — the per-file Remove above already deleted
		// every real object, and RemoveDirectory would just re-list the same
		// (now empty) prefix. Skip it.
		for _, e := range entries {
			if dm.supportsDirectoryStorageOps() {
				if err := dm.store.RemoveDirectory(e.path); err != nil {
					logger.Warn("failed to remove storage directory %s during delete: %v", e.path, err)
				}
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

	// Object storage has no real directories. The file DB `Path` is used
	// directly as the storage object key, so renaming a directory would
	// require an N-object copy+delete (one RTT per child, non-atomic, and
	// leaves dual copies on crash). Skipping the storage move would break
	// downloads (DB points at new path, object still at old path). We
	// therefore reject directory rename on object storage outright; callers
	// should instead copy files to the new path and delete the originals.
	//
	// A nil store (DB-only deployments / tests) is allowed: there is no
	// storage layer to keep in sync, so a DB-only path update is safe.
	if dm.store != nil && !dm.supportsDirectoryStorageOps() {
		return fmt.Errorf("directory rename is not supported for object storage: %s", oldPath)
	}

	release, err := dm.lockDirectory(oldPath)
	if err != nil {
		return err
	}
	defer release()

	meta, err := dm.GetDirectoryMetadata(oldPath)
	if err != nil {
		return err
	}

	newPath := utils.NormalizePath(utils.GetDirectory(oldPath) + "/" + newName)

	if dm.Exists(newPath) {
		return fmt.Errorf("target path already exists: %s", newPath)
	}

	// Snapshot the children to rename. The distributed lock serializes this
	// against concurrent rename/delete on the same directory, so the snapshot
	// is stable for the duration of the operation.
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

	now := utils.GetCurrentTimestamp()

	// Apply all metadata path updates atomically. If the commit fails the DB
	// remains at the old paths and no storage objects have been moved yet, so
	// the system stays consistent. Storage renames happen after commit and are
	// best-effort; a failed storage rename leaves the DB pointing at the new
	// path while the object lingers at the old path — a recoverable mismatch
	// that the cleanup service can reconcile.
	tx, err := dm.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin rename transaction: %w", err)
	}

	for _, e := range fileEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		newItemName := utils.GetFileName(newItemPath)
		if _, err := tx.Exec("UPDATE files SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update file path: %w", err)
		}
	}

	for _, e := range dirEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		newItemName := utils.GetFileName(newItemPath)
		if _, err := tx.Exec("UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update directory path: %w", err)
		}
	}

	if _, err := tx.Exec("UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?", newPath, newName, now, meta.ID); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update directory metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rename: %w", err)
	}

	// DB is now durably at the new paths. Move storage objects to match.
	for _, e := range fileEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		if dm.store != nil {
			if err := dm.store.Rename(e.path, newItemPath); err != nil {
				logger.Warn("failed to rename storage object %s -> %s: %v", e.path, newItemPath, err)
			}
		}
	}

	// Move the target directory's own storage object (if it has one).
	if dm.store != nil {
		if err := dm.store.Rename(oldPath, newPath); err != nil {
			logger.Warn("failed to rename storage directory %s -> %s: %v", oldPath, newPath, err)
		}
	}

	meta.Path = newPath
	meta.Name = newName
	meta.UpdatedAt = now

	return nil
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
