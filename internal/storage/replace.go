package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ReplaceBackup allows a service-layer metadata transaction to restore the
// previous local object if the database update fails after an atomic replace.
type ReplaceBackup interface {
	Commit() error
	Rollback() error
}

// ReplaceBackupStorage is intentionally optional. Object storage needs a
// different versioned-object strategy and is not treated as transactional by
// this local-filesystem mechanism.
type ReplaceBackupStorage interface {
	BeginReplace(path string) (ReplaceBackup, error)
}

// DirectoryRemoveBackup provides the same commit/rollback boundary for a
// local directory tree that ReplaceBackup provides for one file.
type DirectoryRemoveBackup interface {
	Commit() error
	Rollback() error
}

type DirectoryRemoveBackupStorage interface {
	BeginRemoveDirectory(path string) (DirectoryRemoveBackup, error)
}

type localReplaceBackup struct {
	targetPath string
	backupPath string
	existed    bool
}

func (ls *LocalStorage) BeginReplace(path string) (ReplaceBackup, error) {
	if err := ls.validatePath(path); err != nil {
		return nil, err
	}
	target := ls.getFullPath(path)
	if _, err := os.Stat(target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &localReplaceBackup{targetPath: target}, nil
		}
		return nil, fmt.Errorf("failed to inspect replacement target: %w", err)
	}
	placeholder, err := os.CreateTemp(filepath.Dir(target), ".fssvr-backup-*")
	if err != nil {
		return nil, fmt.Errorf("failed to allocate replacement backup: %w", err)
	}
	backupPath := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		os.Remove(backupPath)
		return nil, err
	}
	if err := os.Remove(backupPath); err != nil {
		return nil, err
	}
	if err := os.Link(target, backupPath); err != nil {
		return nil, fmt.Errorf("failed to preserve replacement target: %w", err)
	}
	return &localReplaceBackup{targetPath: target, backupPath: backupPath, existed: true}, nil
}

func (b *localReplaceBackup) Commit() error {
	if !b.existed || b.backupPath == "" {
		return nil
	}
	err := os.Remove(b.backupPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (b *localReplaceBackup) Rollback() error {
	if !b.existed {
		err := os.Remove(b.targetPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.Rename(b.backupPath, b.targetPath); err != nil {
		return fmt.Errorf("failed to restore replacement backup: %w", err)
	}
	return nil
}

func BeginReplace(store StorageAdapter, path string) (ReplaceBackup, error) {
	replacer, ok := store.(ReplaceBackupStorage)
	if !ok {
		return nil, nil
	}
	return replacer.BeginReplace(path)
}

type localDirectoryRemoveBackup struct {
	targetPath string
	backupPath string
	existed    bool
}

func (ls *LocalStorage) BeginRemoveDirectory(path string) (DirectoryRemoveBackup, error) {
	if err := ls.validatePath(path); err != nil {
		return nil, err
	}
	target := ls.getFullPath(path)
	if _, err := os.Stat(target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &localDirectoryRemoveBackup{targetPath: target}, nil
		}
		return nil, fmt.Errorf("failed to inspect directory removal target: %w", err)
	}
	placeholder, err := os.MkdirTemp(filepath.Dir(target), ".fssvr-dir-backup-*")
	if err != nil {
		return nil, fmt.Errorf("failed to allocate directory removal backup: %w", err)
	}
	if err := os.Remove(placeholder); err != nil {
		return nil, err
	}
	if err := os.Rename(target, placeholder); err != nil {
		return nil, fmt.Errorf("failed to preserve directory removal target: %w", err)
	}
	return &localDirectoryRemoveBackup{targetPath: target, backupPath: placeholder, existed: true}, nil
}

func (b *localDirectoryRemoveBackup) Commit() error {
	if !b.existed || b.backupPath == "" {
		return nil
	}
	return os.RemoveAll(b.backupPath)
}

func (b *localDirectoryRemoveBackup) Rollback() error {
	if !b.existed {
		return nil
	}
	if err := os.Rename(b.backupPath, b.targetPath); err != nil {
		return fmt.Errorf("failed to restore removed directory: %w", err)
	}
	return nil
}

func BeginRemoveDirectory(store StorageAdapter, path string) (DirectoryRemoveBackup, error) {
	remover, ok := store.(DirectoryRemoveBackupStorage)
	if !ok {
		return nil, nil
	}
	return remover.BeginRemoveDirectory(path)
}
