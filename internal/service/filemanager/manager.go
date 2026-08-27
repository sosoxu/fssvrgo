package filemanager

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type FileManager struct {
	storage     storage.StorageAdapter
	db          *database.DB
	initialized bool
	fileLocks   sync.Map
	distLock    distributed.DistributedLock
}

func commitReplaceBackup(backup storage.ReplaceBackup, path string) {
	if backup != nil {
		if err := backup.Commit(); err != nil {
			logger.Warn("failed to remove replacement backup for %s: %v", path, err)
		}
	}
}

func rollbackReplaceBackup(backup storage.ReplaceBackup, path string) {
	if backup != nil {
		if err := backup.Rollback(); err != nil {
			logger.Error("failed to restore replacement backup for %s: %v", path, err)
		}
	}
}

func rollbackFileWrite(adapter storage.StorageAdapter, backup storage.ReplaceBackup, path string) {
	if backup != nil {
		rollbackReplaceBackup(backup, path)
	} else if err := adapter.Remove(path); err != nil {
		logger.Error("failed to remove uncommitted file %s: %v", path, err)
	}
}

func NewFileManager(storage storage.StorageAdapter, db *database.DB) *FileManager {
	return &FileManager{
		storage:     storage,
		db:          db,
		initialized: true,
		distLock:    distributed.NewLocalDistributedLock(),
	}
}

func NewFileManagerWithDistLock(storage storage.StorageAdapter, db *database.DB, distLock distributed.DistributedLock) *FileManager {
	return &FileManager{
		storage:     storage,
		db:          db,
		initialized: true,
		distLock:    distLock,
	}
}

func (fm *FileManager) lockFile(path string) {
	val, _ := fm.fileLocks.LoadOrStore(path, &sync.Mutex{})
	mu := val.(*sync.Mutex)
	mu.Lock()
}

func (fm *FileManager) unlockFile(path string) {
	val, ok := fm.fileLocks.Load(path)
	if !ok {
		return
	}
	mu := val.(*sync.Mutex)
	mu.Unlock()
}

func (fm *FileManager) distLockFile(path string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token, err := distributed.AcquireLock(ctx, fm.distLock, "file:"+path, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return "", fmt.Errorf("failed to acquire distributed lock for %s: %w", path, err)
	}
	return token, nil
}

// distLockFileWithRenewal acquires a distributed lock and starts a background
// goroutine to periodically renew it. The returned cancel function must be
// called (typically via defer) to stop renewal when the operation completes.
// Use this for long-running operations like large file uploads.
func (fm *FileManager) distLockFileWithRenewal(ctx context.Context, path string) (*distributed.LockLease, error) {
	lease, err := distributed.AcquireLockLease(ctx, fm.distLock, "file:"+path, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire distributed lock for %s: %w", path, err)
	}
	return lease, nil
}

func (fm *FileManager) distUnlockFile(path string, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fm.distLock.Unlock(ctx, "file:"+path, token); err != nil {
		logger.Warn("failed to release distributed lock for %s: %v", path, err)
	}
}

func (fm *FileManager) acquireFileNamespace(paths ...string) (*database.NamespaceLease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := fm.db.AcquireNamespaceLease(ctx, database.FileNamespaceRequests(paths...), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire file namespace lease: %w", err)
	}
	return lease, nil
}

func releaseNamespaceLease(lease *database.NamespaceLease) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		logger.Warn("failed to release namespace lease: %v", err)
	}
}

func (fm *FileManager) UploadFile(path string, data []byte) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	namespaceLease, err := fm.acquireFileNamespace(path)
	if err != nil {
		return nil, err
	}
	defer releaseNamespaceLease(namespaceLease)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	lease, err := fm.distLockFileWithRenewal(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer fm.distUnlockFile(path, lease.Token)
	defer lease.Stop()

	fileMetadataSvc := database.NewFileMetadataService(fm.db)
	tx, err := fm.db.BeginNamespaceWrite(context.Background(), namespaceLease, path)
	if err != nil {
		return nil, fmt.Errorf("failed to begin fenced upload transaction: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	existingMeta, err := fileMetadataSvc.GetByPathTx(tx, path)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("failed to query existing metadata: %w", err)
	}
	var backup storage.ReplaceBackup
	if existingMeta != nil {
		backup, err = storage.BeginReplace(fm.storage, path)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare overwrite: %w", err)
		}
	}
	if err := fm.storage.Write(path, data); err != nil {
		rollbackReplaceBackup(backup, path)
		return nil, fmt.Errorf("failed to write file: %w", err)
	}
	if err := lease.Err(); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, err
	}
	if err := namespaceLease.Err(); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, err
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	now := utils.GetCurrentTimestamp()
	name := utils.GetFileName(path)

	var meta *database.FileMetadata
	if existingMeta != nil {
		existingMeta.Size = int64(len(data))
		existingMeta.Hash = hash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		meta = existingMeta
		if err := fileMetadataSvc.UpdateTx(tx, meta); err != nil {
			rollbackFileWrite(fm.storage, backup, path)
			return nil, fmt.Errorf("failed to update file metadata: %w", err)
		}
	} else {
		meta = &database.FileMetadata{
			ID: utils.GenerateUUID(), Path: path, Name: name, Size: int64(len(data)), Hash: hash,
			StorageType: fm.storage.StorageType(), StorageLocation: "", CreatedAt: now, UpdatedAt: now,
		}
		if err := fileMetadataSvc.CreateTx(tx, meta); err != nil {
			rollbackFileWrite(fm.storage, backup, path)
			return nil, fmt.Errorf("failed to create file metadata: %w", err)
		}
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, fmt.Errorf("failed to commit file metadata: %w", err)
	}
	rollbackTx = false
	commitReplaceBackup(backup, path)

	return meta, nil
}

// UploadFileFromReader stages the stream and hash before acquiring the final
// write fence. The PostgreSQL transaction therefore covers only the atomic
// storage replacement and metadata commit, not a slow client connection.
func (fm *FileManager) UploadFileFromReader(path string, reader io.Reader) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	hashWriter := sha256.New()
	tempPath, size, err := storage.StageReader(fm.storage, path, io.TeeReader(reader, hashWriter))
	if err != nil {
		return nil, fmt.Errorf("failed to stage upload stream: %w", err)
	}
	defer os.Remove(tempPath)
	hash := hex.EncodeToString(hashWriter.Sum(nil))

	namespaceLease, err := fm.acquireFileNamespace(path)
	if err != nil {
		return nil, err
	}
	defer releaseNamespaceLease(namespaceLease)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	lease, err := fm.distLockFileWithRenewal(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer fm.distUnlockFile(path, lease.Token)
	defer lease.Stop()

	fileMetadataSvc := database.NewFileMetadataService(fm.db)
	tx, err := fm.db.BeginNamespaceWrite(context.Background(), namespaceLease, path)
	if err != nil {
		return nil, fmt.Errorf("failed to begin fenced streaming upload transaction: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	existingMeta, err := fileMetadataSvc.GetByPathTx(tx, path)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("failed to query existing metadata: %w", err)
	}
	var backup storage.ReplaceBackup
	if existingMeta != nil {
		backup, err = storage.BeginReplace(fm.storage, path)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare overwrite: %w", err)
		}
	}

	if err := storage.CommitStagedFile(fm.storage, path, tempPath); err != nil {
		rollbackReplaceBackup(backup, path)
		return nil, fmt.Errorf("failed to commit staged upload: %w", err)
	}
	if err := lease.Err(); err != nil {
		if backup != nil {
			rollbackReplaceBackup(backup, path)
		} else {
			_ = fm.storage.Remove(path)
		}
		return nil, err
	}
	if err := namespaceLease.Err(); err != nil {
		if backup != nil {
			rollbackReplaceBackup(backup, path)
		} else {
			_ = fm.storage.Remove(path)
		}
		return nil, err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, err
	}

	now := utils.GetCurrentTimestamp()
	name := utils.GetFileName(path)

	var meta *database.FileMetadata
	if existingMeta != nil {
		existingMeta.Size = size
		existingMeta.Hash = hash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		if err := fileMetadataSvc.UpdateTx(tx, existingMeta); err != nil {
			rollbackReplaceBackup(backup, path)
			return nil, fmt.Errorf("failed to update file metadata: %w", err)
		}
		meta = existingMeta
	} else {
		meta = &database.FileMetadata{
			ID:              utils.GenerateUUID(),
			Path:            path,
			Name:            name,
			Size:            size,
			Hash:            hash,
			StorageType:     fm.storage.StorageType(),
			StorageLocation: "",
			CreatedAt:       now,
			UpdatedAt:       now,
			IsDeleted:       false,
		}
		if err := fileMetadataSvc.CreateTx(tx, meta); err != nil {
			rollbackFileWrite(fm.storage, backup, path)
			return nil, fmt.Errorf("failed to create file metadata: %w", err)
		}
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		rollbackFileWrite(fm.storage, backup, path)
		return nil, fmt.Errorf("failed to commit file metadata: %w", err)
	}
	rollbackTx = false
	commitReplaceBackup(backup, path)

	return meta, nil
}

func (fm *FileManager) DownloadFile(path string) ([]byte, error) {
	meta, release, err := fm.BeginRead(path)
	if err != nil {
		return nil, err
	}
	defer release()

	data, err := fm.storage.Read(meta.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

func (fm *FileManager) DownloadFileAt(path string, size int, offset int64) ([]byte, error) {
	meta, release, err := fm.BeginRead(path)
	if err != nil {
		return nil, err
	}
	defer release()

	data, err := fm.storage.ReadAt(meta.Path, size, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read file at offset: %w", err)
	}

	return data, nil
}

// BeginRead holds shared leases on the file's ancestor directories while the
// caller reads metadata and storage. This prevents a directory rename/delete
// from moving the object between the metadata lookup and the final read.
func (fm *FileManager) BeginRead(path string) (*database.FileMetadata, func(), error) {
	path = utils.NormalizePath(path)
	lease, err := fm.acquireFileNamespace(path)
	if err != nil {
		return nil, nil, err
	}
	release := func() { releaseNamespaceLease(lease) }
	meta, err := fm.GetFileMetadata(path)
	if err != nil {
		release()
		return nil, nil, err
	}
	return meta, release, nil
}

// DownloadFileData 读取文件内容，复用调用方已查询的 meta，避免 DownloadFile 内部
// 重复执行 GetFileMetadata 的 DB 查询。meta 必须是 GetFileMetadata 的返回值（或
// 等价的有效元数据）。
func (fm *FileManager) DownloadFileData(meta *database.FileMetadata) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is required")
	}
	path := utils.NormalizePath(meta.Path)
	data, err := fm.storage.Read(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return data, nil
}

// DownloadFileDataAt 读取文件指定 offset 起的 size 字节，复用调用方已查询的
// meta。gRPC/HTTP 下载路径在循环或分块读取时调用本方法，可消除每个 chunk 的
// 冗余 GetFileMetadata 查询。
func (fm *FileManager) DownloadFileDataAt(meta *database.FileMetadata, size int, offset int64) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is required")
	}
	path := utils.NormalizePath(meta.Path)
	data, err := fm.storage.ReadAt(path, size, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read file at offset: %w", err)
	}
	return data, nil
}

func (fm *FileManager) DeleteFile(path string) error {
	path = utils.NormalizePath(path)
	namespaceLease, err := fm.acquireFileNamespace(path)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(namespaceLease)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	lease, err := fm.distLockFileWithRenewal(context.Background(), path)
	if err != nil {
		return err
	}
	defer fm.distUnlockFile(path, lease.Token)
	defer lease.Stop()

	fileMetadataSvc := database.NewFileMetadataService(fm.db)
	tx, err := fm.db.BeginNamespaceWrite(context.Background(), namespaceLease, path)
	if err != nil {
		return fmt.Errorf("failed to begin fenced delete transaction: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	meta, err := fileMetadataSvc.GetByPathTx(tx, path)
	if err != nil {
		return err
	}

	backup, err := storage.BeginReplace(fm.storage, path)
	if err != nil {
		return fmt.Errorf("failed to prepare delete: %w", err)
	}
	if err := fm.storage.Remove(path); err != nil {
		commitReplaceBackup(backup, path)
		return fmt.Errorf("failed to delete file from storage: %w", err)
	}
	if err := lease.Err(); err != nil {
		rollbackReplaceBackup(backup, path)
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		rollbackReplaceBackup(backup, path)
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackReplaceBackup(backup, path)
		return err
	}
	if err := fileMetadataSvc.RemoveTx(tx, meta.ID); err != nil {
		rollbackReplaceBackup(backup, path)
		return fmt.Errorf("failed to delete file metadata: %w", err)
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackReplaceBackup(backup, path)
		return err
	}
	if err := tx.Commit(); err != nil {
		rollbackReplaceBackup(backup, path)
		return fmt.Errorf("failed to commit file delete: %w", err)
	}
	rollbackTx = false
	commitReplaceBackup(backup, path)

	return nil
}

func (fm *FileManager) RenameFile(oldPath, newName string) error {
	oldPath = utils.NormalizePath(oldPath)
	newPath := utils.NormalizePath(utils.GetDirectory(oldPath) + "/" + newName)
	if newPath == oldPath {
		return nil
	}
	namespaceLease, err := fm.acquireFileNamespace(oldPath, newPath)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(namespaceLease)

	paths := []string{oldPath, newPath}
	sort.Strings(paths)
	for _, path := range paths {
		fm.lockFile(path)
	}
	defer func() {
		for i := len(paths) - 1; i >= 0; i-- {
			fm.unlockFile(paths[i])
		}
	}()

	leases := make([]*distributed.LockLease, 0, len(paths))
	for _, path := range paths {
		lease, err := fm.distLockFileWithRenewal(context.Background(), path)
		if err != nil {
			for i := len(leases) - 1; i >= 0; i-- {
				leases[i].Stop()
				fm.distUnlockFile(paths[i], leases[i].Token)
			}
			return err
		}
		leases = append(leases, lease)
	}
	defer func() {
		for i := len(leases) - 1; i >= 0; i-- {
			leases[i].Stop()
			fm.distUnlockFile(paths[i], leases[i].Token)
		}
	}()

	fileMetadataSvc := database.NewFileMetadataService(fm.db)
	tx, err := fm.db.BeginNamespaceWrite(context.Background(), namespaceLease, oldPath, newPath)
	if err != nil {
		return fmt.Errorf("failed to begin fenced rename transaction: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	meta, err := fileMetadataSvc.GetByPathTx(tx, oldPath)
	if err != nil {
		return err
	}

	targetMeta, err := fileMetadataSvc.GetByPathTx(tx, newPath)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query target metadata: %w", err)
	}
	if targetMeta != nil || fm.storage.Exists(newPath) {
		return fmt.Errorf("target path already exists: %s", newPath)
	}

	if err := fm.storage.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename file in storage: %w", err)
	}
	for _, lease := range leases {
		if err := lease.Err(); err != nil {
			if rbErr := fm.storage.Rename(newPath, oldPath); rbErr != nil {
				logger.Error("failed to rollback storage rename after lease loss %s -> %s: %v", newPath, oldPath, rbErr)
			}
			return err
		}
	}
	if err := namespaceLease.Err(); err != nil {
		if rbErr := fm.storage.Rename(newPath, oldPath); rbErr != nil {
			logger.Error("failed to rollback storage rename after namespace lease loss %s -> %s: %v", newPath, oldPath, rbErr)
		}
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		if rbErr := fm.storage.Rename(newPath, oldPath); rbErr != nil {
			logger.Error("failed to rollback storage rename after fencing rejection %s -> %s: %v", newPath, oldPath, rbErr)
		}
		return err
	}

	meta.Path = newPath
	meta.Name = newName
	meta.UpdatedAt = utils.GetCurrentTimestamp()

	if err := fileMetadataSvc.UpdateTx(tx, meta); err != nil {
		// 回滚存储层重命名：若回滚也失败则记录日志，避免静默丢失文件。
		if rbErr := fm.storage.Rename(newPath, oldPath); rbErr != nil {
			logger.Error("failed to rollback storage rename %s -> %s: %v", newPath, oldPath, rbErr)
		}
		return fmt.Errorf("failed to update file metadata: %w", err)
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		if rbErr := fm.storage.Rename(newPath, oldPath); rbErr != nil {
			logger.Error("failed to rollback storage rename after fencing rejection %s -> %s: %v", newPath, oldPath, rbErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		if rbErr := fm.storage.Rename(newPath, oldPath); rbErr != nil {
			logger.Error("failed to rollback storage rename after commit failure %s -> %s: %v", newPath, oldPath, rbErr)
		}
		return fmt.Errorf("failed to commit file rename: %w", err)
	}
	rollbackTx = false

	return nil
}

func (fm *FileManager) GetFileMetadata(path string) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	meta, err := database.NewFileMetadataService(fm.db).GetByPath(path)
	if err != nil {
		return nil, fmt.Errorf("file metadata not found: %w", err)
	}
	if meta == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return meta, nil
}

func (fm *FileManager) Exists(path string) bool {
	path = utils.NormalizePath(path)
	exists, err := database.NewFileMetadataService(fm.db).Exists(path)
	if err != nil {
		return false
	}
	return exists
}

func (fm *FileManager) GetFileSize(path string) int64 {
	path = utils.NormalizePath(path)
	meta, err := database.NewFileMetadataService(fm.db).GetByPath(path)
	if err != nil || meta == nil {
		return 0
	}
	return meta.Size
}

func (fm *FileManager) CleanFileLocks() {
	fm.fileLocks.Range(func(key, value interface{}) bool {
		path := key.(string)
		mu := value.(*sync.Mutex)
		// TryLock 成功说明当前无 goroutine 持有该锁，可安全删除；
		// 失败则跳过，避免删除正在使用的锁条目导致锁逃逸。
		if !mu.TryLock() {
			return true
		}
		mu.Unlock()
		if !fm.Exists(path) {
			fm.fileLocks.Delete(key)
		}
		return true
	})
}
