package directory

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// metadataStore is the data-access seam for single-row directory metadata.
// database.DirectoryMetadataService satisfies it, and tests can supply an
// in-memory fake instead of a PostgreSQL instance.
type metadataStore interface {
	Create(meta *database.DirectoryMetadata) error
	GetByPath(path string) (*database.DirectoryMetadata, error)
	Remove(id string) error
	Exists(path string) (bool, error)
}

// treeStore is the data-access seam for the cascade operations that walk or
// rewrite a whole directory subtree. database.DirectoryTreeStore satisfies it;
// tests supply an in-memory fake so the service can be covered without a
// database.
type treeStore interface {
	CountChildren(path string) (fileCount, dirCount int, err error)
	ListChildFiles(path string, limit int) ([]database.PathEntry, error)
	ListChildDirectories(path string, limit int) ([]database.PathEntry, error)
	SoftDeleteFiles(updatedAt string, entries []database.PathEntry) error
	SoftDeleteDirectories(updatedAt string, entries []database.PathEntry) error
	RenameTree(updatedAt string, files, dirs []database.PathUpdate, target database.PathUpdate) error
}

type DirectoryManager struct {
	meta     metadataStore
	tree     treeStore
	store    storage.StorageAdapter
	distLock distributed.DistributedLock
}

func NewDirectoryManager(db *database.DB) *DirectoryManager {
	return NewDirectoryManagerWithStores(database.NewDirectoryMetadataService(db), database.NewDirectoryTreeStore(db), nil, nil)
}

// NewDirectoryManagerWithStore creates a DirectoryManager that also synchronizes
// storage objects when deleting or renaming directories.
func NewDirectoryManagerWithStore(db *database.DB, store storage.StorageAdapter) *DirectoryManager {
	return NewDirectoryManagerWithStores(database.NewDirectoryMetadataService(db), database.NewDirectoryTreeStore(db), store, nil)
}

// NewDirectoryManagerWithDistLock creates a DirectoryManager with a distributed
// lock so that concurrent rename/delete operations on the same directory
// (including across instances) are serialized. Pass nil to disable locking
// (e.g. in single-process tests).
func NewDirectoryManagerWithDistLock(db *database.DB, store storage.StorageAdapter, distLock distributed.DistributedLock) *DirectoryManager {
	return NewDirectoryManagerWithStores(database.NewDirectoryMetadataService(db), database.NewDirectoryTreeStore(db), store, distLock)
}

// NewDirectoryManagerWithStores wires explicit metadata and tree stores. Unit
// tests use it to run without a database.
func NewDirectoryManagerWithStores(meta metadataStore, tree treeStore, store storage.StorageAdapter, distLock distributed.DistributedLock) *DirectoryManager {
	return &DirectoryManager{meta: meta, tree: tree, store: store, distLock: distLock}
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

	if err := dm.meta.Create(meta); err != nil {
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
		fileCount, dirCount, err := dm.tree.CountChildren(path)
		if err != nil {
			return fmt.Errorf("failed to check directory contents: %w", err)
		}
		if fileCount > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}
		if dirCount > 0 {
			return fmt.Errorf("directory is not empty: %s", path)
		}

		meta, err := dm.meta.GetByPath(path)
		if err != nil {
			return fmt.Errorf("failed to get directory metadata: %w", err)
		}
		if meta == nil {
			return fmt.Errorf("directory not found: %s", path)
		}
		return dm.meta.Remove(meta.ID)
	}

	// Recursive deletion. The distributed lock held above serializes concurrent
	// delete/rename operations on this directory across instances. Within this
	// operation we delete in batches to bound memory, and wrap each batch's
	// metadata soft-deletes in a transaction so a batch cannot leave the DB in
	// a half-deleted state. Storage object removal happens after the batch's
	// transaction commits and is best-effort (a leaked object is recoverable via
	// the periodic cleanup service; a missing DB record after commit is not).
	const batchSize = 500

	for {
		// Fetch the next batch outside the transaction — we only need the IDs
		// to soft-delete, and holding a long read transaction for large dirs
		// would hurt concurrency on SQLite.
		entries, err := dm.tree.ListChildFiles(path, batchSize)
		if err != nil {
			return fmt.Errorf("failed to list child files: %w", err)
		}

		if len(entries) == 0 {
			break
		}

		// Soft-delete the whole batch atomically.
		if err := dm.tree.SoftDeleteFiles(utils.GetCurrentTimestamp(), entries); err != nil {
			return fmt.Errorf("failed to delete file metadata: %w", err)
		}

		// Storage cleanup after the DB commit. Best-effort: a failure leaves an
		// orphan object that the cleanup service can reap later.
		for _, e := range entries {
			if dm.store != nil {
				if err := dm.store.Remove(e.Path); err != nil {
					logger.Warn("failed to remove storage object %s during directory delete: %v", e.Path, err)
				}
			}
		}
	}

	for {
		entries, err := dm.tree.ListChildDirectories(path, batchSize)
		if err != nil {
			return fmt.Errorf("failed to list child directories: %w", err)
		}

		if len(entries) == 0 {
			break
		}

		if err := dm.tree.SoftDeleteDirectories(utils.GetCurrentTimestamp(), entries); err != nil {
			return fmt.Errorf("failed to delete directory metadata: %w", err)
		}

		// Storage directory-marker removal is only meaningful for the local
		// backend (it removes a physical directory). For object storage there
		// are no real directories — the per-file Remove above already deleted
		// every real object, and RemoveDirectory would just re-list the same
		// (now empty) prefix. Skip it.
		for _, e := range entries {
			if dm.supportsDirectoryStorageOps() {
				if err := dm.store.RemoveDirectory(e.Path); err != nil {
					logger.Warn("failed to remove storage directory %s during delete: %v", e.Path, err)
				}
			}
		}
	}

	meta, err := dm.meta.GetByPath(path)
	if err != nil {
		return fmt.Errorf("failed to get directory metadata: %w", err)
	}
	if meta == nil {
		return fmt.Errorf("directory not found: %s", path)
	}
	return dm.meta.Remove(meta.ID)
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
	fileEntries, err := dm.tree.ListChildFiles(oldPath, 0)
	if err != nil {
		return fmt.Errorf("failed to list child files: %w", err)
	}

	dirEntries, err := dm.tree.ListChildDirectories(oldPath, 0)
	if err != nil {
		return fmt.Errorf("failed to list child directories: %w", err)
	}

	now := utils.GetCurrentTimestamp()

	// Apply all metadata path updates atomically. If the commit fails the DB
	// remains at the old paths and no storage objects have been moved yet, so
	// the system stays consistent. Storage renames happen after commit and are
	// best-effort; a failed storage rename leaves the DB pointing at the new
	// path while the object lingers at the old path — a recoverable mismatch
	// that the cleanup service can reconcile.
	fileUpdates, err := rewritePaths(fileEntries, oldPath, newPath)
	if err != nil {
		return err
	}
	dirUpdates, err := rewritePaths(dirEntries, oldPath, newPath)
	if err != nil {
		return err
	}

	if err := dm.tree.RenameTree(now, fileUpdates, dirUpdates, database.PathUpdate{ID: meta.ID, Path: newPath, Name: newName}); err != nil {
		return fmt.Errorf("failed to rename directory: %w", err)
	}

	// DB is now durably at the new paths. Move storage objects to match.
	// rewritePaths validated every child's old prefix above, so the slicing
	// here cannot go out of range.
	for _, e := range fileEntries {
		newItemPath := newPath + e.Path[len(oldPath):]
		if dm.store != nil {
			if err := dm.store.Rename(e.Path, newItemPath); err != nil {
				logger.Warn("failed to rename storage object %s -> %s: %v", e.Path, newItemPath, err)
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

// rewritePaths maps the children of a renamed directory to their new paths.
// Every entry came from a query for oldPath's subtree, so a path that does not
// carry the old prefix means the store returned something unexpected — fail
// loudly instead of slicing out of range.
func rewritePaths(entries []database.PathEntry, oldPath, newPath string) ([]database.PathUpdate, error) {
	updates := make([]database.PathUpdate, 0, len(entries))
	for _, e := range entries {
		suffix, ok := strings.CutPrefix(e.Path, oldPath)
		if !ok {
			return nil, fmt.Errorf("child path %q is not under %q", e.Path, oldPath)
		}
		newItemPath := newPath + suffix
		updates = append(updates, database.PathUpdate{
			ID:   e.ID,
			Path: newItemPath,
			Name: utils.GetFileName(newItemPath),
		})
	}
	return updates, nil
}

func (dm *DirectoryManager) GetDirectoryMetadata(path string) (*database.DirectoryMetadata, error) {
	path = utils.NormalizePath(path)
	meta, err := dm.meta.GetByPath(path)
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
	exists, err := dm.meta.Exists(path)
	if err != nil {
		return false
	}
	return exists
}
