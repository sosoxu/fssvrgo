package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

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

// ValidatePath reports whether path stays inside the storage root, resolving
// symlinks. It is an implementation detail: StorageAdapter does not expose it,
// because every operation rejects escaping paths on its own. Exported only for
// backend-specific tests.
func (ls *LocalStorage) ValidatePath(path string) error {
	fullPath := ls.getFullPath(path)
	absRoot, err := filepath.Abs(ls.rootDir)
	if err != nil {
		return fmt.Errorf("invalid root directory: %w", err)
	}
	// Resolve symlinks in root to prevent a symlinked root from bypassing checks.
	if resolvedRoot, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolvedRoot
	}
	absFull, err := filepath.Abs(fullPath)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	// Resolve symlinks on the target path. If the target doesn't exist yet
	// (e.g., a new file upload), resolve the deepest existing ancestor and
	// re-append the remaining components to detect symlinks that escape root.
	resolved, err := filepath.EvalSymlinks(absFull)
	if err != nil {
		resolved = resolveExistingAncestor(absFull)
	}
	absFull = filepath.Clean(resolved)
	if !strings.HasPrefix(absFull, absRoot+string(filepath.Separator)) && absFull != absRoot {
		return fmt.Errorf("path traversal detected: %s", path)
	}
	return nil
}

// resolveExistingAncestor walks up the path until it finds an existing
// component, resolves its symlinks, and re-appends the non-existent tail.
// This is used by ValidatePath to detect symlink-based traversal attacks on
// paths that do not exist yet (e.g., new file uploads).
func resolveExistingAncestor(path string) string {
	dir := path
	tail := ""
	for {
		if resolved, err := filepath.EvalSymlinks(dir); err == nil {
			if tail == "" {
				return resolved
			}
			return filepath.Join(resolved, tail)
		}
		tail = filepath.Join(filepath.Base(dir), tail)
		parent := filepath.Dir(dir)
		if parent == dir {
			return path // reached filesystem root without finding existing dir
		}
		dir = parent
	}
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

// WriteAt performs an in-place random write. It is a local-filesystem
// capability and deliberately not part of StorageAdapter (object storage
// cannot provide it without a full read-modify-write). Used by tests that
// exercise local storage semantics.
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
	if err := validateTempFilePath(tempFilePath); err != nil {
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

	err := os.Rename(tempFilePath, fullPath)
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
	// 不在此处删除 pathLocks 中的锁条目：若删除后另一个 goroutine 通过
	// LoadOrStore 存入新 mutex，会导致两个 goroutine 持有不同 mutex 却操作
	// 同一路径（锁逃逸）。锁条目由 CleanPathLocks 在确认无占用时回收。
	return nil
}

func (ls *LocalStorage) CleanPathLocks() {
	ls.pathLocks.Range(func(key, value interface{}) bool {
		path := key.(string)
		mu := value.(*sync.Mutex)
		// TryLock 成功说明当前无 goroutine 持有该锁，可安全删除；
		// 失败则跳过，等下次清理。避免删除正在使用的锁条目导致锁逃逸。
		if !mu.TryLock() {
			return true
		}
		mu.Unlock()
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

// ListObjects walks the storage root and returns every stored object as a
// slash-separated path relative to the root (for example "dir/file.txt").
//
// It is the enumeration primitive used by the metadata/storage reconciler. It
// is deliberately not part of StorageAdapter: the adapter stays byte-oriented,
// and backends that cannot enumerate simply do not implement this method.
func (ls *LocalStorage) ListObjects(ctx context.Context) ([]string, error) {
	var objects []string
	err := filepath.WalkDir(ls.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(ls.rootDir, path)
		if relErr != nil {
			return relErr
		}
		objects = append(objects, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			// An empty or not-yet-created storage root enumerates to nothing.
			return nil, nil
		}
		return nil, fmt.Errorf("failed to enumerate storage root: %w", err)
	}
	return objects, nil
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

	// 不在此处删除 oldPath 的锁条目，避免锁逃逸（见 Remove 注释）。
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
	// 不在此处删除 pathLocks 中的锁条目，避免锁逃逸（见 Remove 注释）。
	return nil
}
