package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ctxReader aborts a streaming copy when the caller's context is cancelled.
// LocalStorage copies whole files with io.CopyBuffer, which has no notion of
// context; wrapping the source is what lets a cancelled upload stop mid-copy
// instead of running to completion.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
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
	if !strings.HasPrefix(absTemp, os.TempDir()) && !strings.HasPrefix(absTemp, "/tmp/") {
		return fmt.Errorf("temp file must be in system temp directory: %s", tempFilePath)
	}
	return nil
}
