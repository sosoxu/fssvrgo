package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLocalStorageWrite(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	err := ls.Write("test.txt", []byte("hello"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !ls.Exists("test.txt") {
		t.Errorf("file does not exist after write")
	}
}

func TestLocalStorageRead(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	data := []byte("read me")
	if err := ls.Write("read.txt", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := ls.Read("read.txt")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(result) != string(data) {
		t.Errorf("expected %q, got %q", string(data), string(result))
	}
}

func TestLocalStorageReadAt(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	data := []byte("0123456789abcdef")
	if err := ls.Write("readat.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := ls.ReadAt("readat.bin", 4, 8)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	expected := data[8:12]
	if string(result) != string(expected) {
		t.Errorf("expected %q, got %q", string(expected), string(result))
	}
}

func TestLocalStorageWriteFromTempFile(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	content := []byte("temp file content")
	tmpFile, err := os.CreateTemp("", "test-temp-*.tmp")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(content); err != nil {
		tmpFile.Close()
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	if err := ls.WriteFromTempFile("fromtemp.txt", tmpFile.Name()); err != nil {
		t.Fatalf("WriteFromTempFile failed: %v", err)
	}

	result, err := ls.Read("fromtemp.txt")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(result) != string(content) {
		t.Errorf("expected %q, got %q", string(content), string(result))
	}
}

func TestLocalStorageRemove(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	if err := ls.Write("remove.txt", []byte("bye")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := ls.Remove("remove.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if ls.Exists("remove.txt") {
		t.Errorf("file still exists after remove")
	}
}

func TestLocalStorageRename(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	if err := ls.Write("old.txt", []byte("rename")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := ls.Rename("old.txt", "new.txt"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if ls.Exists("old.txt") {
		t.Errorf("old file still exists after rename")
	}
	if !ls.Exists("new.txt") {
		t.Errorf("new file does not exist after rename")
	}
}

func TestLocalStorageExists(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	if ls.Exists("nope.txt") {
		t.Errorf("file should not exist")
	}

	if err := ls.Write("exists.txt", []byte("yes")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !ls.Exists("exists.txt") {
		t.Errorf("file should exist after write")
	}

	if err := ls.Remove("exists.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if ls.Exists("exists.txt") {
		t.Errorf("file should not exist after remove")
	}
}

func TestLocalStorageGetSize(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	data := []byte("exactly 13 chars")
	if err := ls.Write("size.txt", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	size, err := ls.GetSize("size.txt")
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}

	if size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}
}

func TestLocalStorageList(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	ls.Write("list/a.txt", []byte("a"))
	ls.Write("list/b.txt", []byte("b"))
	ls.Write("list/c.txt", []byte("c"))

	names, err := ls.List("list")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(names) != 3 {
		t.Errorf("expected 3 entries, got %d", len(names))
	}

	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	for _, expected := range []string{"a.txt", "b.txt", "c.txt"} {
		if !found[expected] {
			t.Errorf("expected file %s not found in listing", expected)
		}
	}
}

func TestLocalStorageCreateDirectory(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	if err := ls.CreateDirectory("newdir"); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	fullPath := filepath.Join(dir, "newdir")
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("directory does not exist: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("expected directory, got file")
	}
}

func TestLocalStorageRemoveDirectory(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	ls.CreateDirectory("rmdir")
	ls.Write("rmdir/a.txt", []byte("a"))
	ls.Write("rmdir/b.txt", []byte("b"))

	if err := ls.RemoveDirectory("rmdir"); err != nil {
		t.Fatalf("RemoveDirectory failed: %v", err)
	}

	fullPath := filepath.Join(dir, "rmdir")
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Errorf("directory still exists after RemoveDirectory")
	}
}

func TestLocalStorageConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	ls := NewLocalStorage(dir)

	var wg sync.WaitGroup
	count := 10
	errCh := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := filepath.Join("concurrent", fmt.Sprintf("file_%d.txt", idx))
			data := []byte(fmt.Sprintf("data_%d", idx))
			if err := ls.Write(name, data); err != nil {
				errCh <- err
				return
			}
			result, err := ls.Read(name)
			if err != nil {
				errCh <- err
				return
			}
			if string(result) != string(data) {
				errCh <- fmt.Errorf("content mismatch for file_%d", idx)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent access error: %v", err)
	}
}
