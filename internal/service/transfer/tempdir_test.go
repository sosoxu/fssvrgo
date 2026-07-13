package transfer

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSetTempDir_OverridesDefault verifies that SetTempDir changes the
// directory used for upload temp files and that the directory is created.
func TestSetTempDir_OverridesDefault(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	defaultDir := svc.TempDir()
	if defaultDir == "" {
		t.Fatal("expected non-empty default temp dir")
	}

	custom, err := os.MkdirTemp("", "transfer-customtemp-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	// Remove it so SetTempDir has to re-create it, proving MkdirAll runs.
	if err := os.RemoveAll(custom); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	if err := svc.SetTempDir(custom); err != nil {
		t.Fatalf("SetTempDir failed: %v", err)
	}
	if got := svc.TempDir(); got != custom {
		t.Errorf("expected temp dir %s, got %s", custom, got)
	}
	if _, err := os.Stat(custom); err != nil {
		t.Errorf("expected SetTempDir to create the directory, got stat err: %v", err)
	}
}

// TestSetTempDir_EmptyIsNoop verifies that passing an empty path is a no-op
// (keeps the existing temp dir), so callers can safely call it unconditionally.
func TestSetTempDir_EmptyIsNoop(t *testing.T) {
	svc, _, _ := setupTestEnv(t)
	before := svc.TempDir()
	if err := svc.SetTempDir(""); err != nil {
		t.Fatalf("SetTempDir(\"\") returned error: %v", err)
	}
	if got := svc.TempDir(); got != before {
		t.Errorf("empty SetTempDir changed dir from %s to %s", before, got)
	}
}

// TestSetTempDir_UploadUsesConfiguredDir verifies that after SetTempDir, a new
// upload session writes its temp file into the configured directory (not the
// default per-instance one). This is the property that enables cross-instance
// resume on a shared volume: the temp file lands at a path another instance
// can open.
func TestSetTempDir_UploadUsesConfiguredDir(t *testing.T) {
	svc, _, _ := setupTestEnv(t)

	custom, err := os.MkdirTemp("", "transfer-resume-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	if err := svc.SetTempDir(custom); err != nil {
		t.Fatalf("SetTempDir: %v", err)
	}

	sessionID, err := svc.CreateUploadSession("resume.txt", "resume.txt", 16, "client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession: %v", err)
	}

	// The temp file should exist in the configured directory.
	tempPath := filepath.Join(custom, sessionID+".tmp")
	if _, err := os.Stat(tempPath); err != nil {
		t.Errorf("expected temp file at %s after SetTempDir, got: %v", tempPath, err)
	}
}
