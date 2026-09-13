package filemanager

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// metadataStore is the data-access seam of FileManager: everything the service
// needs to know about persisted file metadata, expressed in domain terms.
// database.FileMetadataService satisfies it, and tests can supply an in-memory
// fake instead of a PostgreSQL instance.
//
// Every method takes the caller's context so a cancelled request aborts the
// query instead of running it to completion.
type metadataStore interface {
	GetByPath(ctx context.Context, path string) (*database.FileMetadata, error)
	Create(ctx context.Context, meta *database.FileMetadata) error
	Update(ctx context.Context, meta *database.FileMetadata) error
	Remove(ctx context.Context, id string) error
	Exists(ctx context.Context, path string) (bool, error)
}

type FileManager struct {
	storage     storage.StorageAdapter
	meta        metadataStore
	initialized bool
	fileLocks   sync.Map
	distLock    distributed.DistributedLock
}

func NewFileManager(storage storage.StorageAdapter, db *database.DB) *FileManager {
	return NewFileManagerWithStore(storage, database.NewFileMetadataService(db), distributed.NewLocalDistributedLock())
}

// NewFileManagerWithStore builds a FileManager from the narrow metadata store,
// which is what unit tests use to run without a database.
func NewFileManagerWithStore(storage storage.StorageAdapter, meta metadataStore, distLock distributed.DistributedLock) *FileManager {
	return &FileManager{
		storage:     storage,
		meta:        meta,
		initialized: true,
		distLock:    distLock,
	}
}

func NewFileManagerWithDistLock(storage storage.StorageAdapter, db *database.DB, distLock distributed.DistributedLock) *FileManager {
	return NewFileManagerWithStore(storage, database.NewFileMetadataService(db), distLock)
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

func (fm *FileManager) distLockFile(ctx context.Context, path string) (string, error) {
	lockCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	token, err := distributed.AcquireLock(lockCtx, fm.distLock, "file:"+path, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return "", fmt.Errorf("failed to acquire distributed lock for %s: %w", path, err)
	}
	return token, nil
}

// distLockFileWithRenewal acquires a distributed lock and starts a background
// goroutine to periodically renew it. The returned cancel function must be
// called (typically via defer) to stop renewal when the operation completes.
// Use this for long-running operations like large file uploads.
func (fm *FileManager) distLockFileWithRenewal(ctx context.Context, path string) (string, context.CancelFunc, error) {
	token, cancel, err := distributed.AcquireLockWithRenewal(ctx, fm.distLock, "file:"+path, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return "", nil, fmt.Errorf("failed to acquire distributed lock for %s: %w", path, err)
	}
	return token, cancel, nil
}

// distUnlockFile releases a lock. The release runs on a context derived with
// WithoutCancel: if the caller's context is already cancelled (client
// disconnect, timeout) the lock must still be released, otherwise it would
// linger until its TTL expires and block other writers.
func (fm *FileManager) distUnlockFile(ctx context.Context, path string, token string) {
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := fm.distLock.Unlock(unlockCtx, "file:"+path, token); err != nil {
		logger.Warn("failed to release distributed lock for %s: %v", path, err)
	}
}

func (fm *FileManager) UploadFile(ctx context.Context, path string, data []byte) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	token, cancelRenew, err := fm.distLockFileWithRenewal(ctx, path)
	if err != nil {
		return nil, err
	}
	defer fm.distUnlockFile(ctx, path, token)
	defer cancelRenew()

	if fm.Exists(ctx, path) {
		existingMeta, err := fm.meta.GetByPath(ctx, path)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			// Log the error but continue - treat as new file
			logger.Error("Failed to query existing metadata: %v", err)
		}
		if existingMeta != nil {
			if err := fm.storage.Write(ctx, path, data); err != nil {
				return nil, fmt.Errorf("failed to overwrite file: %w", err)
			}
			hash := fmt.Sprintf("%x", sha256.Sum256(data))
			now := utils.GetCurrentTimestamp()
			existingMeta.Size = int64(len(data))
			existingMeta.Hash = hash
			existingMeta.UpdatedAt = now
			existingMeta.IsDeleted = false
			if err := fm.meta.Update(ctx, existingMeta); err != nil {
				return nil, fmt.Errorf("failed to update file metadata: %w", err)
			}
			return existingMeta, nil
		}
	}

	if err := fm.storage.Write(ctx, path, data); err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	now := utils.GetCurrentTimestamp()
	name := utils.GetFileName(path)

	meta := &database.FileMetadata{
		ID:              utils.GenerateUUID(),
		Path:            path,
		Name:            name,
		Size:            int64(len(data)),
		Hash:            hash,
		StorageType:     fm.storage.StorageType(),
		StorageLocation: "",
		CreatedAt:       now,
		UpdatedAt:       now,
		IsDeleted:       false,
	}

	if err := fm.meta.Create(ctx, meta); err != nil {
		fm.storage.Remove(ctx, path)
		return nil, fmt.Errorf("failed to create file metadata: %w", err)
	}

	return meta, nil
}

// UploadFileFromReader streams the upload into storage without holding the full
// file content in memory. It uses store.WriteFromReader and a streaming SHA-256
// (io.TeeReader) so peak memory is bounded by the copy buffer rather than the
// file size. Use this for large uploads instead of UploadFile.
//
// If the storage backend does not support streaming (WriteFromReader returns
// ErrStreamingUnsupported), the caller should fall back to UploadFile.
//
// Unlike UploadFile, this path does NOT re-hash the existing file on overwrite;
// the hash is computed once from the incoming stream.
func (fm *FileManager) UploadFileFromReader(ctx context.Context, path string, reader io.Reader) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	token, cancelRenew, err := fm.distLockFileWithRenewal(ctx, path)
	if err != nil {
		return nil, err
	}
	defer fm.distUnlockFile(ctx, path, token)
	defer cancelRenew()

	// Tee the stream through a SHA-256 writer so we compute the hash as bytes
	// flow into storage, without buffering the whole file in memory.
	hashWriter := sha256.New()
	teeReader := io.TeeReader(reader, hashWriter)

	if err := fm.storage.WriteFromReader(ctx, path, teeReader); err != nil {
		return nil, fmt.Errorf("failed to write file from reader: %w", err)
	}

	// storage.WriteFromReader does not report bytes written, so re-stat the
	// object to get its size rather than trusting the caller's size hint.
	size, err := fm.storage.GetSize(ctx, path)
	if err != nil {
		// Fall back to a best-effort unknown size rather than failing the
		// already-completed upload.
		size = 0
	}

	hash := hex.EncodeToString(hashWriter.Sum(nil))
	now := utils.GetCurrentTimestamp()
	name := utils.GetFileName(path)

	// Overwrite existing metadata if present (same overwrite semantics as
	// UploadFile), otherwise create a new record.
	existingMeta, err := fm.meta.GetByPath(ctx, path)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		logger.Error("Failed to query existing metadata: %v", err)
	}

	var meta *database.FileMetadata
	if existingMeta != nil {
		existingMeta.Size = size
		existingMeta.Hash = hash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		if err := fm.meta.Update(ctx, existingMeta); err != nil {
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
		if err := fm.meta.Create(ctx, meta); err != nil {
			fm.storage.Remove(ctx, path)
			return nil, fmt.Errorf("failed to create file metadata: %w", err)
		}
	}

	return meta, nil
}

func (fm *FileManager) DownloadFile(ctx context.Context, path string) ([]byte, error) {
	path = utils.NormalizePath(path)
	_, err := fm.GetFileMetadata(ctx, path)
	if err != nil {
		return nil, err
	}

	data, err := fm.storage.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

func (fm *FileManager) DownloadFileAt(ctx context.Context, path string, size int, offset int64) ([]byte, error) {
	path = utils.NormalizePath(path)
	_, err := fm.GetFileMetadata(ctx, path)
	if err != nil {
		return nil, err
	}

	data, err := fm.storage.ReadAt(ctx, path, size, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read file at offset: %w", err)
	}

	return data, nil
}

// DownloadFileData 读取文件内容，复用调用方已查询的 meta，避免 DownloadFile 内部
// 重复执行 GetFileMetadata 的 DB 查询。meta 必须是 GetFileMetadata 的返回值（或
// 等价的有效元数据）。
func (fm *FileManager) DownloadFileData(ctx context.Context, meta *database.FileMetadata) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is required")
	}
	path := utils.NormalizePath(meta.Path)
	data, err := fm.storage.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return data, nil
}

// DownloadFileDataAt 读取文件指定 offset 起的 size 字节，复用调用方已查询的
// meta。gRPC/HTTP 下载路径在循环或分块读取时调用本方法，可消除每个 chunk 的
// 冗余 GetFileMetadata 查询。
func (fm *FileManager) DownloadFileDataAt(ctx context.Context, meta *database.FileMetadata, size int, offset int64) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is required")
	}
	path := utils.NormalizePath(meta.Path)
	data, err := fm.storage.ReadAt(ctx, path, size, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read file at offset: %w", err)
	}
	return data, nil
}

func (fm *FileManager) DeleteFile(ctx context.Context, path string) error {
	path = utils.NormalizePath(path)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	token, err := fm.distLockFile(ctx, path)
	if err != nil {
		return err
	}
	defer fm.distUnlockFile(ctx, path, token)

	meta, err := fm.GetFileMetadata(ctx, path)
	if err != nil {
		return err
	}

	if err := fm.storage.Remove(ctx, path); err != nil {
		return fmt.Errorf("failed to delete file from storage: %w", err)
	}

	if err := fm.meta.Remove(ctx, meta.ID); err != nil {
		return fmt.Errorf("failed to delete file metadata: %w", err)
	}

	return nil
}

func (fm *FileManager) RenameFile(ctx context.Context, oldPath, newName string) error {
	oldPath = utils.NormalizePath(oldPath)
	fm.lockFile(oldPath)
	defer fm.unlockFile(oldPath)

	token, err := fm.distLockFile(ctx, oldPath)
	if err != nil {
		return err
	}
	defer fm.distUnlockFile(ctx, oldPath, token)

	meta, err := fm.GetFileMetadata(ctx, oldPath)
	if err != nil {
		return err
	}

	newPath := utils.NormalizePath(utils.GetDirectory(oldPath) + "/" + newName)

	if fm.Exists(ctx, newPath) {
		return fmt.Errorf("target path already exists: %s", newPath)
	}

	if err := fm.storage.Rename(ctx, oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename file in storage: %w", err)
	}

	meta.Path = newPath
	meta.Name = newName
	meta.UpdatedAt = utils.GetCurrentTimestamp()

	if err := fm.meta.Update(ctx, meta); err != nil {
		// 回滚存储层重命名：若回滚也失败则记录日志，避免静默丢失文件。
		if rbErr := fm.storage.Rename(ctx, newPath, oldPath); rbErr != nil {
			logger.Error("failed to rollback storage rename %s -> %s: %v", newPath, oldPath, rbErr)
		}
		return fmt.Errorf("failed to update file metadata: %w", err)
	}

	return nil
}

func (fm *FileManager) GetFileMetadata(ctx context.Context, path string) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	meta, err := fm.meta.GetByPath(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("file metadata not found: %w", err)
	}
	if meta == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return meta, nil
}

func (fm *FileManager) Exists(ctx context.Context, path string) bool {
	path = utils.NormalizePath(path)
	exists, err := fm.meta.Exists(ctx, path)
	if err != nil {
		return false
	}
	return exists
}

func (fm *FileManager) GetFileSize(ctx context.Context, path string) int64 {
	path = utils.NormalizePath(path)
	meta, err := fm.meta.GetByPath(ctx, path)
	if err != nil || meta == nil {
		return 0
	}
	return meta.Size
}

// CleanFileLocks drops lock entries for paths that no longer exist. ctx is the
// janitor's lifetime context (not a request context); it bounds the existence
// checks performed while iterating.
func (fm *FileManager) CleanFileLocks(ctx context.Context) {
	fm.fileLocks.Range(func(key, value interface{}) bool {
		path := key.(string)
		mu := value.(*sync.Mutex)
		// TryLock 成功说明当前无 goroutine 持有该锁，可安全删除；
		// 失败则跳过，避免删除正在使用的锁条目导致锁逃逸。
		if !mu.TryLock() {
			return true
		}
		mu.Unlock()
		if !fm.Exists(ctx, path) {
			fm.fileLocks.Delete(key)
		}
		return true
	})
}
