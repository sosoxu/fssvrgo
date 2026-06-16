package filemanager

import (
	"context"
	"crypto/sha256"
	"fmt"
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

func (fm *FileManager) distUnlockFile(path string, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fm.distLock.Unlock(ctx, "file:"+path, token); err != nil {
		logger.Warn("failed to release distributed lock for %s: %v", path, err)
	}
}

func (fm *FileManager) UploadFile(path string, data []byte) (*database.FileMetadata, error) {
	path = utils.NormalizePath(path)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	token, err := fm.distLockFile(path)
	if err != nil {
		return nil, err
	}
	defer fm.distUnlockFile(path, token)

	if fm.Exists(path) {
		existingMeta, _ := database.NewFileMetadataService(fm.db).GetByPath(path)
		if existingMeta != nil {
			if err := fm.storage.Write(path, data); err != nil {
				return nil, fmt.Errorf("failed to overwrite file: %w", err)
			}
			hash := fmt.Sprintf("%x", sha256.Sum256(data))
			now := utils.GetCurrentTimestamp()
			existingMeta.Size = int64(len(data))
			existingMeta.Hash = hash
			existingMeta.UpdatedAt = now
			existingMeta.IsDeleted = false
			if err := database.NewFileMetadataService(fm.db).Update(existingMeta); err != nil {
				return nil, fmt.Errorf("failed to update file metadata: %w", err)
			}
			return existingMeta, nil
		}
	}

	if err := fm.storage.Write(path, data); err != nil {
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

	if err := database.NewFileMetadataService(fm.db).Create(meta); err != nil {
		fm.storage.Remove(path)
		return nil, fmt.Errorf("failed to create file metadata: %w", err)
	}

	return meta, nil
}

func (fm *FileManager) DownloadFile(path string) ([]byte, error) {
	path = utils.NormalizePath(path)
	_, err := fm.GetFileMetadata(path)
	if err != nil {
		return nil, err
	}

	data, err := fm.storage.Read(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

func (fm *FileManager) DownloadFileAt(path string, size int, offset int64) ([]byte, error) {
	path = utils.NormalizePath(path)
	_, err := fm.GetFileMetadata(path)
	if err != nil {
		return nil, err
	}

	data, err := fm.storage.ReadAt(path, size, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read file at offset: %w", err)
	}

	return data, nil
}

func (fm *FileManager) DeleteFile(path string) error {
	path = utils.NormalizePath(path)
	fm.lockFile(path)
	defer fm.unlockFile(path)

	token, err := fm.distLockFile(path)
	if err != nil {
		return err
	}
	defer fm.distUnlockFile(path, token)

	meta, err := fm.GetFileMetadata(path)
	if err != nil {
		return err
	}

	if err := fm.storage.Remove(path); err != nil {
		return fmt.Errorf("failed to delete file from storage: %w", err)
	}

	if err := database.NewFileMetadataService(fm.db).Remove(meta.ID); err != nil {
		return fmt.Errorf("failed to delete file metadata: %w", err)
	}

	return nil
}

func (fm *FileManager) RenameFile(oldPath, newName string) error {
	oldPath = utils.NormalizePath(oldPath)
	fm.lockFile(oldPath)
	defer fm.unlockFile(oldPath)

	token, err := fm.distLockFile(oldPath)
	if err != nil {
		return err
	}
	defer fm.distUnlockFile(oldPath, token)

	meta, err := fm.GetFileMetadata(oldPath)
	if err != nil {
		return err
	}

	newPath := utils.NormalizePath(utils.GetDirectory(oldPath) + "/" + newName)

	if fm.Exists(newPath) {
		return fmt.Errorf("target path already exists: %s", newPath)
	}

	if err := fm.storage.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename file in storage: %w", err)
	}

	meta.Path = newPath
	meta.Name = newName
	meta.UpdatedAt = utils.GetCurrentTimestamp()

	if err := database.NewFileMetadataService(fm.db).Update(meta); err != nil {
		fm.storage.Rename(newPath, oldPath)
		return fmt.Errorf("failed to update file metadata: %w", err)
	}

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
	fm.fileLocks.Range(func(key, _ interface{}) bool {
		path := key.(string)
		if !fm.Exists(path) {
			fm.fileLocks.Delete(key)
		}
		return true
	})
}
