package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type StorageAdapter interface {
	Write(path string, data []byte) error
	WriteAt(path string, data []byte, offset int64) error
	WriteFromTempFile(path string, tempFilePath string) error
	WriteFromReader(path string, reader io.Reader) error
	Read(path string) ([]byte, error)
	ReadAt(path string, size int, offset int64) ([]byte, error)
	OpenReader(path string) (io.ReadCloser, error)
	Remove(path string) error
	Exists(path string) bool
	List(directory string) ([]string, error)
	GetSize(path string) (int64, error)
	Rename(oldPath, newPath string) error
	CreateDirectory(path string) error
	RemoveDirectory(path string) error
	StorageType() string
	ValidatePath(path string) error
}

type LocalStorage struct {
	rootDir   string
	pathLocks sync.Map
}

func NewLocalStorage(rootDir string) *LocalStorage {
	return &LocalStorage{
		rootDir: rootDir,
	}
}

func (ls *LocalStorage) RootDir() string {
	return ls.rootDir
}

func (ls *LocalStorage) StorageType() string {
	return "local"
}

func (ls *LocalStorage) getFullPath(path string) string {
	return filepath.Join(ls.rootDir, path)
}

func (ls *LocalStorage) ValidatePath(path string) error {
	fullPath := ls.getFullPath(path)
	absRoot, err := filepath.Abs(ls.rootDir)
	if err != nil {
		return fmt.Errorf("invalid root directory: %w", err)
	}
	absFull, err := filepath.Abs(fullPath)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	if !strings.HasPrefix(absFull, absRoot) {
		return fmt.Errorf("path traversal detected: %s", path)
	}
	return nil
}

func (ls *LocalStorage) validatePath(path string) error {
	return ls.ValidatePath(path)
}

func (ls *LocalStorage) getLock(path string) *sync.Mutex {
	val, _ := ls.pathLocks.LoadOrStore(path, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func (ls *LocalStorage) ensureDirectoryExists(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return os.MkdirAll(dirPath, 0755)
	}
	return nil
}

func (ls *LocalStorage) Write(path string, data []byte) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	dir := filepath.Dir(fullPath)
	if err := ls.ensureDirectoryExists(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

func (ls *LocalStorage) WriteAt(path string, data []byte, offset int64) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	dir := filepath.Dir(fullPath)
	if err := ls.ensureDirectoryExists(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write at offset: %w", err)
	}
	return nil
}

func (ls *LocalStorage) WriteFromTempFile(path string, tempFilePath string) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	absTemp, err := filepath.Abs(tempFilePath)
	if err != nil {
		return fmt.Errorf("invalid temp file path: %w", err)
	}
	if !strings.HasPrefix(absTemp, os.TempDir()) && !strings.HasPrefix(absTemp, "/tmp/") {
		return fmt.Errorf("temp file must be in system temp directory: %s", tempFilePath)
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	dir := filepath.Dir(fullPath)
	if err := ls.ensureDirectoryExists(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	err = os.Rename(tempFilePath, fullPath)
	if err == nil {
		return nil
	}

	src, err := os.Open(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dst.Close()

	buf := make([]byte, 256*1024)
	if _, err := io.CopyBuffer(dst, src, buf); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	if err := os.Remove(tempFilePath); err != nil {
		return fmt.Errorf("failed to remove temp file: %w", err)
	}
	return nil
}

func (ls *LocalStorage) Read(path string) ([]byte, error) {
	if err := ls.validatePath(path); err != nil {
		return nil, err
	}
	fullPath := ls.getFullPath(path)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return data, nil
}

func (ls *LocalStorage) OpenReader(path string) (io.ReadCloser, error) {
	if err := ls.validatePath(path); err != nil {
		return nil, err
	}
	fullPath := ls.getFullPath(path)
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return f, nil
}

func (ls *LocalStorage) WriteFromReader(path string, reader io.Reader) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	dir := filepath.Dir(fullPath)
	if err := ls.ensureDirectoryExists(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 256*1024)
	if _, err := io.CopyBuffer(f, reader, buf); err != nil {
		return fmt.Errorf("failed to write from reader: %w", err)
	}
	return nil
}

func (ls *LocalStorage) ReadAt(path string, size int, offset int64) ([]byte, error) {
	if err := ls.validatePath(path); err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, fmt.Errorf("invalid read size: %d", size)
	}
	if offset < 0 {
		return nil, fmt.Errorf("invalid read offset: %d", offset)
	}
	fullPath := ls.getFullPath(path)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	buf := make([]byte, size)
	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read at offset: %w", err)
	}
	return buf[:n], nil
}

func (ls *LocalStorage) Remove(path string) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	if err := os.Remove(fullPath); err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}
	ls.pathLocks.Delete(path)
	return nil
}

func (ls *LocalStorage) CleanPathLocks() {
	ls.pathLocks.Range(func(key, _ interface{}) bool {
		path := key.(string)
		fullPath := ls.getFullPath(path)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			ls.pathLocks.Delete(key)
		}
		return true
	})
}

func (ls *LocalStorage) Exists(path string) bool {
	if err := ls.validatePath(path); err != nil {
		return false
	}
	fullPath := ls.getFullPath(path)
	_, err := os.Stat(fullPath)
	return err == nil
}

func (ls *LocalStorage) List(directory string) ([]string, error) {
	if err := ls.validatePath(directory); err != nil {
		return nil, err
	}
	fullPath := ls.getFullPath(directory)
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}

	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, nil
}

func (ls *LocalStorage) GetSize(path string) (int64, error) {
	if err := ls.validatePath(path); err != nil {
		return 0, err
	}
	fullPath := ls.getFullPath(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to get file size: %w", err)
	}
	return info.Size(), nil
}

func (ls *LocalStorage) Rename(oldPath, newPath string) error {
	if err := ls.validatePath(oldPath); err != nil {
		return err
	}
	if err := ls.validatePath(newPath); err != nil {
		return err
	}

	if oldPath == newPath {
		return nil
	}

	oldMu := ls.getLock(oldPath)
	newMu := ls.getLock(newPath)

	if oldPath < newPath {
		oldMu.Lock()
		newMu.Lock()
	} else {
		newMu.Lock()
		oldMu.Lock()
	}
	defer oldMu.Unlock()
	defer newMu.Unlock()

	fullOldPath := ls.getFullPath(oldPath)
	fullNewPath := ls.getFullPath(newPath)
	dir := filepath.Dir(fullNewPath)
	if err := ls.ensureDirectoryExists(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := os.Rename(fullOldPath, fullNewPath); err != nil {
		return fmt.Errorf("failed to rename: %w", err)
	}

	ls.pathLocks.Delete(oldPath)
	return nil
}

func (ls *LocalStorage) CreateDirectory(path string) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func (ls *LocalStorage) RemoveDirectory(path string) error {
	if err := ls.validatePath(path); err != nil {
		return err
	}
	mu := ls.getLock(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := ls.getFullPath(path)
	if err := os.RemoveAll(fullPath); err != nil {
		return fmt.Errorf("failed to remove directory: %w", err)
	}
	ls.pathLocks.Delete(path)
	return nil
}
