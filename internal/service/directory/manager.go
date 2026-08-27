package directory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// escapeLikePattern 转义 SQL LIKE 模式中的通配符（%、_、\），防止目录名含这些
// 字符时导致跨目录误匹配。转义后的模式需配合 ESCAPE '\\' 子句使用。
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

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
func (dm *DirectoryManager) lockDirectory(path string) (*distributed.LockLease, func(), error) {
	if dm.distLock == nil {
		return nil, func() {}, nil
	}
	lease, err := distributed.AcquireLockLease(context.Background(), dm.distLock, "dir:"+path, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire directory lock for %s: %w", path, err)
	}
	return lease, func() {
		lease.Stop()
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		if err := dm.distLock.Unlock(unlockCtx, "dir:"+path, lease.Token); err != nil {
			logger.Warn("failed to release directory lock for %s: %v", path, err)
		}
	}, nil
}

func checkDirectoryLease(lease *distributed.LockLease) error {
	if lease == nil {
		return nil
	}
	return lease.Err()
}

func (dm *DirectoryManager) lockDirectories(paths ...string) ([]*distributed.LockLease, func(), error) {
	ordered := append([]string(nil), paths...)
	sort.Strings(ordered)
	leases := make([]*distributed.LockLease, 0, len(ordered))
	releases := make([]func(), 0, len(ordered))
	for _, path := range ordered {
		lease, release, err := dm.lockDirectory(path)
		if err != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return nil, nil, err
		}
		leases = append(leases, lease)
		releases = append(releases, release)
	}
	return leases, func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}, nil
}

func checkDirectoryLeases(leases []*distributed.LockLease) error {
	for _, lease := range leases {
		if err := checkDirectoryLease(lease); err != nil {
			return err
		}
	}
	return nil
}

func (dm *DirectoryManager) acquireDirectoryNamespace(paths ...string) (*database.NamespaceLease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := dm.db.AcquireNamespaceLease(ctx, database.DirectoryNamespaceRequests(paths...), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire directory namespace lease: %w", err)
	}
	return lease, nil
}

func releaseNamespaceLease(lease *database.NamespaceLease) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		logger.Warn("failed to release directory namespace lease: %v", err)
	}
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
	namespaceLease, err := dm.acquireDirectoryNamespace(path)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(namespaceLease)

	lease, release, err := dm.lockDirectory(path)
	if err != nil {
		return err
	}
	defer release()

	metadataSvc := database.NewDirectoryMetadataService(dm.db)
	tx, err := dm.db.BeginNamespaceWrite(context.Background(), namespaceLease, path)
	if err != nil {
		return fmt.Errorf("failed to begin fenced directory create: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	existing, err := metadataSvc.GetByPathTx(tx, path)
	if err != nil {
		return fmt.Errorf("failed to check directory metadata: %w", err)
	}
	if existing != nil {
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

	if err := checkDirectoryLease(lease); err != nil {
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		return err
	}
	if err := metadataSvc.CreateTx(tx, meta); err != nil {
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit directory create: %w", err)
	}
	rollbackTx = false

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
	namespaceLease, err := dm.acquireDirectoryNamespace(path)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(namespaceLease)

	lease, release, err := dm.lockDirectory(path)
	if err != nil {
		return err
	}
	defer release()

	metadataSvc := database.NewDirectoryMetadataService(dm.db)
	meta, err := metadataSvc.GetByPath(path)
	if err != nil {
		return fmt.Errorf("failed to get directory metadata: %w", err)
	}
	if meta == nil {
		return fmt.Errorf("directory not found: %s", path)
	}

	if !recursive {
		escapedPrefix := escapeLikePattern(path + "/")
		var fileCount int
		err := dm.db.QueryRow("SELECT COUNT(*) FROM files WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", escapedPrefix+"%").Scan(&fileCount)
		if err != nil {
			return fmt.Errorf("failed to check directory contents: %w", err)
		}
		if fileCount > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}
		var dirCount int
		err = dm.db.QueryRow("SELECT COUNT(*) FROM directories WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", escapedPrefix+"%").Scan(&dirCount)
		if err != nil {
			return fmt.Errorf("failed to check directory contents: %w", err)
		}
		if dirCount > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}

		if err := checkDirectoryLease(lease); err != nil {
			return err
		}
		if err := namespaceLease.Err(); err != nil {
			return err
		}
		tx, err := dm.db.BeginNamespaceWrite(context.Background(), namespaceLease, path)
		if err != nil {
			return fmt.Errorf("failed to begin fenced directory delete: %w", err)
		}
		rollbackTx := true
		defer func() {
			if rollbackTx {
				_ = tx.Rollback()
			}
		}()
		var storageBackup storage.DirectoryRemoveBackup
		if dm.supportsDirectoryStorageOps() {
			storageBackup, err = storage.BeginRemoveDirectory(dm.store, path)
			if err != nil {
				return fmt.Errorf("failed to prepare empty directory delete: %w", err)
			}
		}
		if err := metadataSvc.RemoveTx(tx, meta.ID); err != nil {
			if storageBackup != nil {
				_ = storageBackup.Rollback()
			}
			return err
		}
		if err := namespaceLease.ValidateTx(tx); err != nil {
			if storageBackup != nil {
				_ = storageBackup.Rollback()
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			if storageBackup != nil {
				_ = storageBackup.Rollback()
			}
			return fmt.Errorf("failed to commit directory delete: %w", err)
		}
		rollbackTx = false
		if storageBackup != nil {
			if err := storageBackup.Commit(); err != nil {
				logger.Warn("failed to clean empty directory backup %s: %v", path, err)
			}
		}
		return nil
	}

	// Object storage has no atomic directory removal, so remember its live file
	// keys before the metadata transaction and remove them only after commit.
	// Local storage is removed as one tree and does not need an in-memory list.
	prefix := escapeLikePattern(path + "/")
	var objectPaths []string
	if dm.store != nil && !dm.supportsDirectoryStorageOps() {
		rows, err := dm.db.Query("SELECT path FROM files WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", prefix+"%")
		if err != nil {
			return fmt.Errorf("failed to query object paths: %w", err)
		}
		for rows.Next() {
			var objectPath string
			if err := rows.Scan(&objectPath); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan object path: %w", err)
			}
			objectPaths = append(objectPaths, objectPath)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("failed to iterate object paths: %w", err)
		}
		rows.Close()
	}

	if err := checkDirectoryLease(lease); err != nil {
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		return err
	}
	// All metadata rows transition together. A failure in any statement rolls
	// the complete tree back, so callers never observe a successful prefix and
	// a still-live suffix from an earlier batch.
	tx, err := dm.db.BeginNamespaceWrite(context.Background(), namespaceLease, path)
	if err != nil {
		return fmt.Errorf("failed to begin recursive delete transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	var storageBackup storage.DirectoryRemoveBackup
	if dm.supportsDirectoryStorageOps() {
		storageBackup, err = storage.BeginRemoveDirectory(dm.store, path)
		if err != nil {
			return fmt.Errorf("failed to prepare recursive directory delete: %w", err)
		}
	}
	rollbackStorage := func() {
		if storageBackup != nil {
			if rbErr := storageBackup.Rollback(); rbErr != nil {
				logger.Error("failed to restore directory %s after metadata failure: %v", path, rbErr)
			}
		}
	}

	now := utils.GetCurrentTimestamp()
	if _, err := tx.Exec("UPDATE files SET is_deleted = TRUE, updated_at = ? WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", now, prefix+"%"); err != nil {
		rollbackStorage()
		return fmt.Errorf("failed to delete file metadata: %w", err)
	}
	if _, err := tx.Exec("UPDATE directories SET is_deleted = TRUE, updated_at = ? WHERE (id = ? OR path LIKE ? ESCAPE '\\') AND is_deleted = FALSE", now, meta.ID, prefix+"%"); err != nil {
		rollbackStorage()
		return fmt.Errorf("failed to delete directory metadata: %w", err)
	}
	if err := checkDirectoryLease(lease); err != nil {
		rollbackStorage()
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		rollbackStorage()
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackStorage()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollbackStorage()
		return fmt.Errorf("failed to commit recursive delete: %w", err)
	}
	rollback = false

	if storageBackup != nil {
		if err := storageBackup.Commit(); err != nil {
			logger.Warn("failed to clean recursive directory backup %s: %v", path, err)
		}
	} else if dm.store != nil {
		for _, objectPath := range objectPaths {
			if err := dm.store.Remove(objectPath); err != nil {
				logger.Warn("failed to remove storage object %s during directory delete: %v", objectPath, err)
			}
		}
	}
	return nil
}

func (dm *DirectoryManager) RenameDirectory(oldPath, newName string) error {
	oldPath = utils.NormalizePath(oldPath)
	newPath := utils.NormalizePath(utils.GetDirectory(oldPath) + "/" + newName)
	if newPath == oldPath {
		return nil
	}

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
	namespaceLease, err := dm.acquireDirectoryNamespace(oldPath, newPath)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(namespaceLease)

	leases, release, err := dm.lockDirectories(oldPath, newPath)
	if err != nil {
		return err
	}
	defer release()

	tx, err := dm.db.BeginNamespaceWrite(context.Background(), namespaceLease, oldPath, newPath)
	if err != nil {
		return fmt.Errorf("failed to begin fenced directory rename: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	metadataSvc := database.NewDirectoryMetadataService(dm.db)
	meta, err := metadataSvc.GetByPathTx(tx, oldPath)
	if err != nil {
		return fmt.Errorf("failed to get source directory metadata: %w", err)
	}
	if meta == nil {
		return fmt.Errorf("directory not found: %s", oldPath)
	}

	targetMeta, err := metadataSvc.GetByPathTx(tx, newPath)
	if err != nil {
		return fmt.Errorf("failed to check target directory metadata: %w", err)
	}
	if targetMeta != nil {
		return fmt.Errorf("target path already exists: %s", newPath)
	}
	if dm.store != nil && dm.store.Exists(newPath) {
		return fmt.Errorf("target storage path already exists: %s", newPath)
	}

	// Snapshot the children to rename. The distributed lock serializes this
	// against concurrent rename/delete on the same directory, so the snapshot
	// is stable for the duration of the operation.
	rows, err := tx.Query("SELECT id, path FROM files WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", escapeLikePattern(oldPath+"/")+"%")
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
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("failed to iterate file rows: %w", err)
	}
	rows.Close()

	dirRows, err := tx.Query("SELECT id, path FROM directories WHERE path LIKE ? ESCAPE '\\' AND is_deleted = FALSE", escapeLikePattern(oldPath+"/")+"%")
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
	if err := dirRows.Err(); err != nil {
		dirRows.Close()
		return fmt.Errorf("failed to iterate directory rows: %w", err)
	}
	dirRows.Close()

	now := utils.GetCurrentTimestamp()

	if err := checkDirectoryLeases(leases); err != nil {
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		return err
	}

	// Local directory rename is one filesystem operation, so move the complete
	// tree before changing metadata. If the DB transaction or lease check fails,
	// move it back and keep the old metadata paths authoritative.
	storageMoved := false
	if dm.store != nil {
		if err := dm.store.Rename(oldPath, newPath); err != nil {
			return fmt.Errorf("failed to rename storage directory %s -> %s: %w", oldPath, newPath, err)
		}
		storageMoved = true
	}
	rollbackStorage := func() {
		if storageMoved {
			if rbErr := dm.store.Rename(newPath, oldPath); rbErr != nil {
				logger.Error("failed to rollback storage directory rename %s -> %s: %v", newPath, oldPath, rbErr)
			}
		}
	}

	for _, e := range fileEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		newItemName := utils.GetFileName(newItemPath)
		if _, err := tx.Exec("UPDATE files SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id); err != nil {
			rollbackStorage()
			return fmt.Errorf("failed to update file path: %w", err)
		}
	}

	for _, e := range dirEntries {
		newItemPath := newPath + e.path[len(oldPath):]
		newItemName := utils.GetFileName(newItemPath)
		if _, err := tx.Exec("UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?", newItemPath, newItemName, now, e.id); err != nil {
			rollbackStorage()
			return fmt.Errorf("failed to update directory path: %w", err)
		}
	}

	if _, err := tx.Exec("UPDATE directories SET path = ?, name = ?, updated_at = ? WHERE id = ?", newPath, newName, now, meta.ID); err != nil {
		rollbackStorage()
		return fmt.Errorf("failed to update directory metadata: %w", err)
	}

	if err := checkDirectoryLeases(leases); err != nil {
		rollbackStorage()
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		rollbackStorage()
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackStorage()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollbackStorage()
		return fmt.Errorf("failed to commit rename: %w", err)
	}
	rollbackTx = false

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
