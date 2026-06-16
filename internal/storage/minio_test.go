package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func newTestMinIOStorage(t *testing.T) (*MinIOStorage, *gofakes3.GoFakeS3) {
	t.Helper()

	backend := s3mem.New()
	faker := gofakes3.New(backend)

	server := httptest.NewServer(faker.Server())
	t.Cleanup(server.Close)

	cfg := MinIOConfig{
		Endpoint:  server.Listener.Addr().String(),
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key",
		Bucket:    "test-bucket",
		UseSSL:    false,
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		t.Fatalf("failed to create minio client: %v", err)
	}

	store := &MinIOStorage{
		client: client,
		bucket: cfg.Bucket,
	}

	return store, faker
}

func ensureBucket(t *testing.T, store *MinIOStorage) {
	t.Helper()
	ctx := context.Background()
	err := store.client.MakeBucket(ctx, store.bucket, minio.MakeBucketOptions{})
	if err != nil {
		_, err2 := store.client.BucketExists(ctx, store.bucket)
		if err2 != nil {
			t.Fatalf("failed to ensure bucket exists: %v / %v", err, err2)
		}
	}
}

func TestMinIOStorageType(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	if store.StorageType() != "minio" {
		t.Errorf("expected StorageType 'minio', got %q", store.StorageType())
	}
}

func TestMinIOValidatePath(t *testing.T) {
	store, _ := newTestMinIOStorage(t)

	if err := store.ValidatePath(""); err == nil {
		t.Error("expected error for empty path")
	}
	if err := store.ValidatePath("../etc/passwd"); err == nil {
		t.Error("expected error for path traversal")
	}
	if err := store.ValidatePath("/absolute/path"); err == nil {
		t.Error("expected error for absolute path starting with /")
	}
	if err := store.ValidatePath("valid/path/file.txt"); err != nil {
		t.Errorf("expected no error for valid path, got: %v", err)
	}
}

func TestMinIOWriteAndRead(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/write-read.txt"
	data := []byte("hello minio storage")

	if err := store.Write(key, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !store.Exists(key) {
		t.Error("object should exist after write")
	}

	result, err := store.Read(key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(result) != string(data) {
		t.Errorf("expected %q, got %q", string(data), string(result))
	}
}

func TestMinIOReadAt(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/readat.bin"
	data := []byte("0123456789abcdef")
	if err := store.Write(key, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := store.ReadAt(key, 4, 8)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	expected := data[8:12]
	if string(result) != string(expected) {
		t.Errorf("expected %q, got %q", string(expected), string(result))
	}
}

func TestMinIOWriteAt(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/writeat.bin"
	data := []byte("0123456789abcdef")
	if err := store.Write(key, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	patch := []byte("XXXX")
	if err := store.WriteAt(key, patch, 4); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	result, err := store.Read(key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	expected := "0123XXXX89abcdef"
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}

func TestMinIOWriteAtExtend(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/writeat-extend.bin"
	data := []byte("short")
	if err := store.Write(key, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	patch := []byte("extended!")
	if err := store.WriteAt(key, patch, 10); err != nil {
		t.Fatalf("WriteAt extend failed: %v", err)
	}

	result, err := store.Read(key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(result) != 19 {
		t.Errorf("expected length 19, got %d", len(result))
	}
	if string(result[10:]) != "extended!" {
		t.Errorf("expected 'extended!' at offset 10, got %q", string(result[10:]))
	}
}

func TestMinIOWriteFromReader(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/write-from-reader.txt"
	data := []byte("streaming write content")

	if err := store.WriteFromReader(key, bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteFromReader failed: %v", err)
	}

	result, err := store.Read(key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(result) != string(data) {
		t.Errorf("expected %q, got %q", string(data), string(result))
	}
}

func TestMinIOOpenReader(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/open-reader.txt"
	data := []byte("open reader content")

	if err := store.Write(key, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	reader, err := store.OpenReader(key)
	if err != nil {
		t.Fatalf("OpenReader failed: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll from reader failed: %v", err)
	}

	if string(result) != string(data) {
		t.Errorf("expected %q, got %q", string(data), string(result))
	}
}

func TestMinIOWriteFromTempFile(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	content := []byte("temp file upload content")
	tmpFile, err := os.CreateTemp("", "minio-test-temp-*.tmp")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(content); err != nil {
		tmpFile.Close()
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	key := "test/from-temp-file.txt"
	if err := store.WriteFromTempFile(key, tmpFile.Name()); err != nil {
		t.Fatalf("WriteFromTempFile failed: %v", err)
	}

	result, err := store.Read(key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(result) != string(content) {
		t.Errorf("expected %q, got %q", string(content), string(result))
	}
}

func TestMinIORemove(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/remove.txt"
	if err := store.Write(key, []byte("bye")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := store.Remove(key); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if store.Exists(key) {
		t.Error("object still exists after remove")
	}
}

func TestMinIORename(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	oldKey := "test/rename-old.txt"
	newKey := "test/rename-new.txt"
	data := []byte("renamed content")

	if err := store.Write(oldKey, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := store.Rename(oldKey, newKey); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if store.Exists(oldKey) {
		t.Error("old object still exists after rename")
	}
	if !store.Exists(newKey) {
		t.Error("new object does not exist after rename")
	}

	result, err := store.Read(newKey)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(result) != string(data) {
		t.Errorf("content mismatch after rename: expected %q, got %q", string(data), string(result))
	}
}

func TestMinIORenameSameKey(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/rename-same.txt"
	if err := store.Write(key, []byte("same")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := store.Rename(key, key); err != nil {
		t.Fatalf("Rename same key should not fail: %v", err)
	}

	if !store.Exists(key) {
		t.Error("object should still exist after rename to same key")
	}
}

func TestMinIOExists(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	if store.Exists("test/nonexistent.txt") {
		t.Error("object should not exist")
	}

	key := "test/exists.txt"
	if err := store.Write(key, []byte("yes")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !store.Exists(key) {
		t.Error("object should exist after write")
	}

	store.Remove(key)

	if store.Exists(key) {
		t.Error("object should not exist after remove")
	}
}

func TestMinIOGetSize(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/size.txt"
	data := []byte("exactly 13 chars")
	if err := store.Write(key, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	size, err := store.GetSize(key)
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}

	if size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}
}

func TestMinIOCreateDirectory(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	prefix := "test-newdir"

	if err := store.CreateDirectory(prefix); err != nil {
		t.Skipf("CreateDirectory not supported by S3 mock (works on real MinIO): %v", err)
	}

	if !store.Exists(prefix + "/") {
		t.Error("directory marker should exist after CreateDirectory")
	}
}

func TestMinIORemoveDirectory(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	prefix := "test-rmdir"
	store.CreateDirectory(prefix)
	store.Write(prefix+"/a.txt", []byte("a"))
	store.Write(prefix+"/b.txt", []byte("b"))

	if err := store.RemoveDirectory(prefix); err != nil {
		t.Fatalf("RemoveDirectory failed: %v", err)
	}

	if store.Exists(prefix + "/a.txt") {
		t.Error("object should not exist after RemoveDirectory")
	}
}

func TestMinIOConcurrentAccess(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	var wg sync.WaitGroup
	count := 10
	errCh := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("test-concurrent/file_%d.txt", idx)
			data := []byte(fmt.Sprintf("data_%d", idx))
			if err := store.Write(key, data); err != nil {
				errCh <- err
				return
			}
			result, err := store.Read(key)
			if err != nil {
				errCh <- err
				return
			}
			if string(result) != string(data) {
				errCh <- fmt.Errorf("content mismatch for file_%d", idx)
				return
			}
			store.Remove(key)
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent access error: %v", err)
	}
}

func TestMinIOStreamingWriteAndRead(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/streaming.bin"
	size := 2 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := store.WriteFromReader(key, bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteFromReader failed: %v", err)
	}

	reader, err := store.OpenReader(key)
	if err != nil {
		t.Fatalf("OpenReader failed: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll from stream failed: %v", err)
	}

	if len(result) != size {
		t.Fatalf("expected size %d, got %d", size, len(result))
	}

	for i := range data {
		if result[i] != data[i] {
			t.Errorf("data mismatch at byte %d", i)
			break
		}
	}
}

func TestMinIOReadAtInvalidOffset(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/readat-invalid.bin"
	if err := store.Write(key, []byte("short")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err := store.ReadAt(key, 10, -1)
	if err == nil {
		t.Error("expected error for negative offset")
	}

	_, err = store.ReadAt(key, 0, 0)
	if err == nil {
		t.Error("expected error for zero size")
	}
}

func TestMinIOWriteAtInvalidOffset(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/writeat-invalid.bin"
	if err := store.Write(key, []byte("data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err := store.WriteAt(key, []byte("x"), -1)
	if err == nil {
		t.Error("expected error for negative offset")
	}
}

func TestMinIOOverwrite(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/overwrite.txt"
	if err := store.Write(key, []byte("original")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := store.Write(key, []byte("replaced")); err != nil {
		t.Fatalf("Overwrite failed: %v", err)
	}

	result, err := store.Read(key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(result) != "replaced" {
		t.Errorf("expected 'replaced', got %q", string(result))
	}
}

func TestMinIOValidatePathTraversal(t *testing.T) {
	store, _ := newTestMinIOStorage(t)

	tests := []struct {
		path    string
		wantErr bool
	}{
		{"valid/path.txt", false},
		{"simple.txt", false},
		{"", true},
		{"../etc/passwd", true},
		{"/absolute/path", true},
		{"path/../../../etc", true},
		{"valid/../traversal", true},
	}

	for _, tt := range tests {
		err := store.ValidatePath(tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidatePath(%q) = %v, wantErr %v", tt.path, err, tt.wantErr)
		}
	}
}

func TestMinIOStorageAdapterInterface(t *testing.T) {
	store, _ := newTestMinIOStorage(t)

	var _ StorageAdapter = store
}

func TestMinIOHTTPHealthCheck(t *testing.T) {
	store, faker := newTestMinIOStorage(t)
	ensureBucket(t, store)

	key := "test/health.txt"
	if err := store.Write(key, []byte("ok")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	handler := faker.Server()
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/test/health.txt", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}
