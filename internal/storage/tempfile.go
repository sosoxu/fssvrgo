package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var trustedTempRoots sync.Map

type stagingFileStorage interface {
	CreateStagingFile(path string) (*os.File, error)
	CommitStagingFile(path, tempPath string) error
}

func StageReader(store StorageAdapter, path string, reader io.Reader) (string, int64, error) {
	var (
		file *os.File
		err  error
	)
	if staging, ok := store.(stagingFileStorage); ok {
		file, err = staging.CreateStagingFile(path)
	} else {
		file, err = os.CreateTemp("", ".fssvr-stage-*")
	}
	if err != nil {
		return "", 0, err
	}
	tempPath := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}
	written, err := io.CopyBuffer(file, reader, make([]byte, 256*1024))
	if err != nil {
		cleanup()
		return "", 0, err
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return "", 0, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return "", 0, err
	}
	return tempPath, written, nil
}

func CommitStagedFile(store StorageAdapter, path, tempPath string) error {
	if staging, ok := store.(stagingFileStorage); ok {
		return staging.CommitStagingFile(path, tempPath)
	}
	return store.WriteFromTempFile(path, tempPath)
}

// RegisterTrustedTempDir adds an application-controlled directory to the set
// of roots accepted by WriteFromTempFile. This is required when upload temp
// files live on a shared volume rather than under os.TempDir().
func RegisterTrustedTempDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("trusted temp directory cannot be empty")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("invalid trusted temp directory: %w", err)
	}
	trustedTempRoots.Store(filepath.Clean(abs), struct{}{})
	return nil
}

func pathWithinRoot(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || filepath.IsAbs(rel) {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// validateTempFilePath ensures tempFilePath points inside the system temp
// directory. This prevents a caller from uploading arbitrary local files
// (e.g. /etc/shadow) into the storage backend.
//
// Both LocalStorage and MinIOStorage enforce this check so the two backends
// behave identically; previously only LocalStorage validated the temp path,
// which meant the MinIO backend would upload any local file referenced by an
// untrusted caller.
func validateTempFilePath(tempFilePath string) error {
	absTemp, err := filepath.Abs(tempFilePath)
	if err != nil {
		return fmt.Errorf("invalid temp file path: %w", err)
	}
	absTemp = filepath.Clean(absTemp)
	roots := []string{filepath.Clean(os.TempDir())}
	if filepath.Clean(os.TempDir()) != filepath.Clean("/tmp") {
		roots = append(roots, filepath.Clean("/tmp"))
	}
	trustedTempRoots.Range(func(key, _ interface{}) bool {
		roots = append(roots, key.(string))
		return true
	})
	for _, root := range roots {
		if pathWithinRoot(absTemp, root) {
			return nil
		}
	}
	return fmt.Errorf("temp file is outside trusted temp directories: %s", tempFilePath)
}
