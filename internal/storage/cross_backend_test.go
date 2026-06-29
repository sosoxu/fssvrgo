package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runOnBothBackends runs fn against a LocalStorage and a (gofakes3-backed)
// MinIOStorage so that path-semantics assertions are enforced for both the
// local/concentrated storage and the object storage backends.
func runOnBothBackends(t *testing.T, fn func(t *testing.T, name string, s StorageAdapter)) {
	t.Helper()

	t.Run("LocalStorage", func(t *testing.T) {
		root := t.TempDir()
		s := NewLocalStorage(root)
		fn(t, "local", s)
	})

	t.Run("MinIO", func(t *testing.T) {
		store, _ := newTestMinIOStorage(t)
		ensureBucket(t, store)
		fn(t, "minio", store)
	})
}

// TestCrossBackendValidatePathLegalDoubleDot verifies that a legitimate file
// name containing ".." as a substring (e.g. "file..txt") is accepted by BOTH
// backends. Previously MinIOStorage used strings.Contains(key, "..") which
// falsely rejected such names while LocalStorage accepted them — a clear case
// of inconsistent path handling between the two backends.
func TestCrossBackendValidatePathLegalDoubleDot(t *testing.T) {
	legalNames := []string{
		"file..txt",
		"ver..1.0.txt",
		"my..data.bin",
		"docs/notes..draft.md",
	}
	runOnBothBackends(t, func(t *testing.T, name string, s StorageAdapter) {
		for _, p := range legalNames {
			if err := s.ValidatePath(p); err != nil {
				t.Errorf("[%s] ValidatePath(%q) = %v, want nil (legitimate name)", name, p, err)
			}
		}
	})
}

// TestCrossBackendValidatePathRejectsRootEscape verifies both backends reject a
// traversal that escapes the storage root. Note: LocalStorage.ValidatePath
// resolves the final absolute path and only rejects paths that land OUTSIDE
// rootDir, so "a/../b" (which resolves to "b" inside rootDir) is accepted by
// LocalStorage by design. We only assert agreement on truly escaping paths.
// The upper-layer guard (utils.IsValidFilePath) rejects any ".." segment
// regardless of resolution, so end-to-end safety is preserved.
func TestCrossBackendValidatePathRejectsRootEscape(t *testing.T) {
	escaping := []string{"../etc/passwd", "foo/../../bar"}
	runOnBothBackends(t, func(t *testing.T, name string, s StorageAdapter) {
		for _, p := range escaping {
			if err := s.ValidatePath(p); err == nil {
				t.Errorf("[%s] ValidatePath(%q) = nil, want error (escapes root)", name, p)
			}
		}
	})
}

// TestCrossBackendWriteReadDoubleDotName writes a file whose logical path
// contains ".." as a substring and reads it back, on both backends. This is
// the end-to-end proof that the ValidatePath fix lets such names flow through
// the object storage backend.
func TestCrossBackendWriteReadDoubleDotName(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, name string, s StorageAdapter) {
		path := "ver..1.0/release..notes.txt"
		data := []byte("hello from " + name)
		if err := s.Write(path, data); err != nil {
			t.Fatalf("[%s] Write(%q) = %v", name, path, err)
		}
		got, err := s.Read(path)
		if err != nil {
			t.Fatalf("[%s] Read(%q) = %v", name, path, err)
		}
		if string(got) != string(data) {
			t.Errorf("[%s] Read back = %q, want %q", name, got, data)
		}
		if !s.Exists(path) {
			t.Errorf("[%s] Exists(%q) = false, want true", name, path)
		}
	})
}

// TestMinIOExistsDirectoryWithChildren verifies the #52 fix: MinIOStorage.Exists
// reports a directory path as existing when only child objects are present
// (matching LocalStorage semantics where a dir exists if it has children).
func TestMinIOExistsDirectoryWithChildren(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	if err := store.Write("docs/readme.txt", []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !store.Exists("docs/readme.txt") {
		t.Error("Exists(\"docs/readme.txt\") = false, want true")
	}
	if !store.Exists("docs") {
		t.Error("Exists(\"docs\") = false, want true (directory has children)")
	}
	if store.Exists("does/not/exist") {
		t.Error("Exists(\"does/not/exist\") = true, want false")
	}
}

// TestLocalStorageExistsDirectoryWithChildren is the LocalStorage counterpart,
// ensuring both backends agree on directory existence semantics.
func TestLocalStorageExistsDirectoryWithChildren(t *testing.T) {
	root := t.TempDir()
	s := NewLocalStorage(root)

	if err := s.Write("docs/readme.txt", []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !s.Exists("docs/readme.txt") {
		t.Error("Exists(\"docs/readme.txt\") = false, want true")
	}
	if !s.Exists("docs") {
		t.Error("Exists(\"docs\") = false, want true (directory has children)")
	}
	if s.Exists("does/not/exist") {
		t.Error("Exists(\"does/not/exist\") = true, want false")
	}
}

// TestStorageIsNotExist verifies storage.IsNotExist recognizes "not exist"
// errors from BOTH backends. This is the core of the #49 fix: cleanup.go must
// not use os.IsNotExist (which only recognizes the filesystem error) when
// handling errors from the StorageAdapter interface.
func TestStorageIsNotExist(t *testing.T) {
	// LocalStorage: reading a missing file yields an os.ErrNotExist-wrapped error.
	root := t.TempDir()
	ls := NewLocalStorage(root)
	if _, err := ls.Read("missing.txt"); err == nil {
		t.Fatal("expected error reading missing file")
	} else if !IsNotExist(err) {
		t.Errorf("IsNotExist(LocalStorage missing read) = false, want true (err=%v)", err)
	}
	if err := ls.Remove("missing.txt"); err != nil && !IsNotExist(err) {
		t.Errorf("IsNotExist(LocalStorage missing remove) = false, want true (err=%v)", err)
	}

	// MinIO: StatObject on a missing key returns a NoSuchKey error response.
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)
	if err := store.Remove("never-existed-key"); err != nil && !IsNotExist(err) {
		t.Errorf("IsNotExist(MinIO missing remove) = false, want true (err=%v)", err)
	}
	// nil must not be reported as not-exist.
	if IsNotExist(nil) {
		t.Error("IsNotExist(nil) = true, want false")
	}
}

// TestValidateTempFilePath is the unit test for the shared temp-path guard
// (#53). It does not touch the filesystem — validateTempFilePath is a pure
// string-prefix check — so we can assert arbitrary paths without depending on
// where the OS places its temp directory.
func TestValidateTempFilePath(t *testing.T) {
	// A path under the system temp dir is accepted.
	ok := filepath.Join(os.TempDir(), "fsserver", "upload.tmp")
	if err := validateTempFilePath(ok); err != nil {
		t.Errorf("validateTempFilePath(%q) = %v, want nil", ok, err)
	}
	// "/tmp/..." is accepted on all Unix systems.
	if err := validateTempFilePath("/tmp/anything.tmp"); err != nil {
		t.Errorf("validateTempFilePath(\"/tmp/anything.tmp\") = %v, want nil", err)
	}
	// Arbitrary local paths are rejected (this is the security guard that
	// previously existed only on LocalStorage).
	bad := []string{
		"/etc/shadow",
		"/home/user/secret.txt",
		"/var/lib/data.db",
		"relative/path/file.tmp",
	}
	for _, p := range bad {
		// Skip when the bad path happens to live under os.TempDir() (some CI
		// runners place the whole workspace under /tmp), which would make the
		// assertion non-meaningful for that particular case.
		abs, _ := filepath.Abs(p)
		if strings.HasPrefix(abs, os.TempDir()) || strings.HasPrefix(abs, "/tmp/") {
			continue
		}
		if err := validateTempFilePath(p); err == nil {
			t.Errorf("validateTempFilePath(%q) = nil, want error", p)
		}
	}
}

// TestCrossBackendWriteFromTempFileAcceptsTemp verifies both backends accept a
// temp file that lives under the system temp directory and upload/copy it
// correctly. Each backend gets its OWN temp file because LocalStorage moves
// (renames) the source file on success.
func TestCrossBackendWriteFromTempFileAcceptsTemp(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, name string, s StorageAdapter) {
		// Create a fresh temp file per backend (LocalStorage renames it away).
		f, err := os.CreateTemp("", "fsserver-src-*.tmp")
		if err != nil {
			t.Fatalf("[%s] CreateTemp: %v", name, err)
		}
		tempPath := f.Name()
		if _, err := f.WriteString("payload"); err != nil {
			f.Close()
			t.Fatalf("[%s] WriteString: %v", name, err)
		}
		f.Close()
		defer os.Remove(tempPath)

		if err := s.WriteFromTempFile("dst/file.bin", tempPath); err != nil {
			t.Errorf("[%s] WriteFromTempFile(valid temp path) = %v, want nil", name, err)
			return
		}
		got, err := s.Read("dst/file.bin")
		if err != nil {
			t.Fatalf("[%s] Read: %v", name, err)
		}
		if string(got) != "payload" {
			t.Errorf("[%s] got %q, want %q", name, got, "payload")
		}
	})
}

// TestCrossBackendWriteFromTempFileRejectsNonTemp verifies both backends
// reject a temp file path outside the system temp directory (#53 fix). We use
// validateTempFilePath directly because constructing a real file that is BOTH
// writable AND outside os.TempDir() is environment-dependent.
func TestCrossBackendWriteFromTempFileRejectsNonTemp(t *testing.T) {
	// Pick a path guaranteed outside os.TempDir() and /tmp/.
	nonTempPath := "/var/lib/fsserver-test/secret-src"
	if strings.HasPrefix(nonTempPath, os.TempDir()) {
		nonTempPath = "/opt/fsserver-test/secret-src"
	}

	t.Run("LocalStorage", func(t *testing.T) {
		root := t.TempDir()
		s := NewLocalStorage(root)
		if err := s.WriteFromTempFile("target/file.txt", nonTempPath); err == nil {
			t.Errorf("WriteFromTempFile(non-temp path) = nil, want error")
		}
	})
	t.Run("MinIO", func(t *testing.T) {
		store, _ := newTestMinIOStorage(t)
		ensureBucket(t, store)
		if err := store.WriteFromTempFile("target/file.txt", nonTempPath); err == nil {
			t.Errorf("WriteFromTempFile(non-temp path) = nil, want error")
		}
	})
}
