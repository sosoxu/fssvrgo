package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func newRealMinIOStorage(t *testing.T) *MinIOStorage {
	t.Helper()

	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	if secretKey == "" {
		secretKey = "minioadmin"
	}

	bucket := fmt.Sprintf("integration-test-%d", time.Now().UnixNano())

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("failed to create minio client: %v", err)
	}

	ctx := context.Background()
	err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Fatalf("failed to create bucket %s: %v", bucket, err)
	}

	t.Cleanup(func() {
		objectsCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
		for obj := range objectsCh {
			if obj.Err == nil {
				client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
			}
		}
		client.RemoveBucket(ctx, bucket)
	})

	return &MinIOStorage{
		client: client,
		bucket: bucket,
	}
}

func TestRealMinIOStorageType(t *testing.T) {
	store := newRealMinIOStorage(t)
	if store.StorageType() != "minio" {
		t.Errorf("expected 'minio', got %q", store.StorageType())
	}
}

func TestRealMinIOWriteAndRead(t *testing.T) {
	store := newRealMinIOStorage(t)

	key := "test/write-read.txt"
	data := []byte("hello real minio storage")

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

func TestRealMinIOReadAt(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOWriteAt(t *testing.T) {
	store := newRealMinIOStorage(t)

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

	if string(result) != "0123XXXX89abcdef" {
		t.Errorf("expected '0123XXXX89abcdef', got %q", string(result))
	}
}

func TestRealMinIOWriteAtExtend(t *testing.T) {
	store := newRealMinIOStorage(t)

	key := "test/writeat-extend.bin"
	if err := store.Write(key, []byte("short")); err != nil {
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

func TestRealMinIOWriteFromReader(t *testing.T) {
	store := newRealMinIOStorage(t)

	key := "test/write-from-reader.txt"
	data := []byte("streaming write content to real minio")

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

func TestRealMinIOOpenReader(t *testing.T) {
	store := newRealMinIOStorage(t)

	key := "test/open-reader.txt"
	data := []byte("open reader content on real minio")

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
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(result) != string(data) {
		t.Errorf("expected %q, got %q", string(data), string(result))
	}
}

func TestRealMinIOWriteFromTempFile(t *testing.T) {
	store := newRealMinIOStorage(t)

	content := []byte("temp file upload to real minio")
	tmpFile, err := os.CreateTemp("", "minio-real-test-*.tmp")
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

func TestRealMinIORemove(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIORename(t *testing.T) {
	store := newRealMinIOStorage(t)

	oldKey := "test/rename-old.txt"
	newKey := "test/rename-new.txt"
	data := []byte("renamed on real minio")

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
		t.Errorf("content mismatch: expected %q, got %q", string(data), string(result))
	}
}

func TestRealMinIORenameSameKey(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOExists(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOGetSize(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOCreateDirectory(t *testing.T) {
	store := newRealMinIOStorage(t)

	prefix := "test-newdir"

	if err := store.CreateDirectory(prefix); err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	if !store.Exists(prefix + "/") {
		t.Error("directory marker should exist after CreateDirectory")
	}
}

func TestRealMinIORemoveDirectory(t *testing.T) {
	store := newRealMinIOStorage(t)

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
	if store.Exists(prefix + "/") {
		t.Error("directory marker should not exist after RemoveDirectory")
	}
}

func TestRealMinIOList(t *testing.T) {
	store := newRealMinIOStorage(t)

	prefix := "test-list"
	store.Write(prefix+"/a.txt", []byte("a"))
	store.Write(prefix+"/b.txt", []byte("b"))
	store.Write(prefix+"/c.txt", []byte("c"))

	names, err := store.List(prefix)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(names) != 3 {
		t.Errorf("expected 3 entries, got %d: %v", len(names), names)
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

func TestRealMinIOConcurrentAccess(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOStreamingLargeFile(t *testing.T) {
	store := newRealMinIOStorage(t)

	key := "test/streaming-large.bin"
	size := 5 * 1024 * 1024
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
		t.Fatalf("ReadAll failed: %v", err)
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

func TestRealMinIOOverwrite(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOReadAtInvalidOffset(t *testing.T) {
	store := newRealMinIOStorage(t)

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

func TestRealMinIOWriteAtInvalidOffset(t *testing.T) {
	store := newRealMinIOStorage(t)

	key := "test/writeat-invalid.bin"
	if err := store.Write(key, []byte("data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err := store.WriteAt(key, []byte("x"), -1)
	if err == nil {
		t.Error("expected error for negative offset")
	}
}

func TestRealMinIOValidatePathTraversal(t *testing.T) {
	store := newRealMinIOStorage(t)

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
