package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupOrphanedUploadDirs(t *testing.T) {
	root := t.TempDir()
	now := time.Now()

	oldDir := filepath.Join(root, uploadTempDirPrefix+"1000")
	freshDir := filepath.Join(root, uploadTempDirPrefix+"2000")
	unrelated := filepath.Join(root, "not-an-upload-dir")
	for _, dir := range []string{oldDir, freshDir, unrelated} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dir, err)
		}
	}
	// Age the "old" directory beyond the threshold.
	stale := now.Add(-2 * orphanTempDirMinAge)
	if err := os.Chtimes(oldDir, stale, stale); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	removed, err := cleanupOrphanedUploadDirs(root, orphanTempDirMinAge, now)
	if err != nil {
		t.Fatalf("cleanupOrphanedUploadDirs: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Errorf("stale upload dir should have been removed (stat err=%v)", err)
	}
	if _, err := os.Stat(freshDir); err != nil {
		t.Errorf("fresh upload dir must be kept: another instance may be uploading into it (%v)", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("unrelated directory must be kept: %v", err)
	}
}

func TestCleanupOrphanedUploadDirsMissingRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	removed, err := cleanupOrphanedUploadDirs(missing, time.Hour, time.Now())
	if err != nil {
		t.Fatalf("missing root must not be an error: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
}
