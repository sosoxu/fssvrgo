package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// uploadTempDirPrefix matches the per-process upload temp directories created
// by internal/service/transfer (os.TempDir()/fsserver-uploads-<nanos>).
const uploadTempDirPrefix = "fsserver-uploads-"

// orphanTempDirMinAge is the minimum age before a leftover upload temp
// directory may be removed at startup. Anything younger could belong to
// another fsserver instance running on the same host with in-flight uploads.
const orphanTempDirMinAge = 24 * time.Hour

// cleanupOrphanedUploadDirs removes fsserver-uploads-* directories under root
// that are older than minAge, and reports how many were removed.
//
// Age is the guard against deleting live data: a second instance starting on
// the same host shares os.TempDir(), so its in-flight temp directories must
// survive our startup cleanup.
func cleanupOrphanedUploadDirs(root string, minAge time.Duration, now time.Time) (int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read temp directory %s: %w", root, err)
	}

	removed := 0
	var failures []string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), uploadTempDirPrefix) {
			continue
		}
		fullPath := filepath.Join(root, entry.Name())
		info, statErr := entry.Info()
		if statErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", fullPath, statErr))
			continue
		}
		if now.Sub(info.ModTime()) < minAge {
			continue
		}
		if err := os.RemoveAll(fullPath); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", fullPath, err))
			continue
		}
		removed++
	}

	if len(failures) > 0 {
		return removed, fmt.Errorf("failed to clean %d orphaned temp director(ies): %s",
			len(failures), strings.Join(failures, "; "))
	}
	return removed, nil
}
