package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOStorage struct {
	client   *minio.Client
	bucket   string
	pathLocks sync.Map
}

type MinIOConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

func NewMinIOStorage(cfg MinIOConfig) (*MinIOStorage, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("failed to create bucket %s: %w", cfg.Bucket, err)
		}
	}

	return &MinIOStorage{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

func (ms *MinIOStorage) StorageType() string {
	return "minio"
}

func (ms *MinIOStorage) ValidatePath(objectKey string) error {
	if objectKey == "" {
		return fmt.Errorf("object key cannot be empty")
	}
	if strings.Contains(objectKey, "..") {
		return fmt.Errorf("path traversal detected: %s", objectKey)
	}
	if strings.HasPrefix(objectKey, "/") {
		return fmt.Errorf("object key must not start with /: %s", objectKey)
	}
	return nil
}

func (ms *MinIOStorage) validatePath(objectKey string) error {
	return ms.ValidatePath(objectKey)
}

func (ms *MinIOStorage) getLock(objectKey string) *sync.Mutex {
	val, _ := ms.pathLocks.LoadOrStore(objectKey, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func (ms *MinIOStorage) normalizeKey(objectKey string) string {
	return path.Clean(objectKey)
}

func (ms *MinIOStorage) Write(objectKey string, data []byte) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}
	mu := ms.getLock(objectKey)
	mu.Lock()
	defer mu.Unlock()

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()
	reader := bytes.NewReader(data)
	_, err := ms.client.PutObject(ctx, ms.bucket, key, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to write object: %w", err)
	}
	return nil
}

func (ms *MinIOStorage) WriteAt(objectKey string, data []byte, offset int64) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}
	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}

	// Check existing object size to prevent OOM on large files
	key := ms.normalizeKey(objectKey)
	ctx := context.Background()
	info, err := ms.client.StatObject(ctx, ms.bucket, key, minio.StatObjectOptions{})
	if err == nil && info.Size > 512*1024*1024 { // 512MB limit for WriteAt
		return fmt.Errorf("WriteAt not supported for large objects (size: %d bytes), use WriteFromReader instead", info.Size)
	}

	mu := ms.getLock(objectKey)
	mu.Lock()
	defer mu.Unlock()

	obj, err := ms.client.GetObject(ctx, ms.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object for WriteAt: %w", err)
	}
	defer obj.Close()

	existing, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("failed to read existing object: %w", err)
	}

	endOffset := offset + int64(len(data))
	if endOffset > int64(len(existing)) {
		extended := make([]byte, endOffset)
		copy(extended, existing)
		copy(extended[offset:], data)
		existing = extended
	} else {
		copy(existing[offset:], data)
	}

	reader := bytes.NewReader(existing)
	_, err = ms.client.PutObject(ctx, ms.bucket, key, reader, int64(len(existing)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to write object at offset: %w", err)
	}
	return nil
}

func (ms *MinIOStorage) WriteFromTempFile(objectKey string, tempFilePath string) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}

	mu := ms.getLock(objectKey)
	mu.Lock()
	defer ms.pathLocks.Delete(objectKey)
	defer mu.Unlock()

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	_, err := ms.client.FPutObject(ctx, ms.bucket, key, tempFilePath, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to upload from temp file: %w", err)
	}
	return nil
}

func (ms *MinIOStorage) WriteFromReader(objectKey string, reader io.Reader) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}
	mu := ms.getLock(objectKey)
	mu.Lock()
	defer mu.Unlock()

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	var size int64 = -1
	if seeker, ok := reader.(io.Seeker); ok {
		current, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get current seek position: %w", err)
		}
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to seek to end: %w", err)
		}
		size = end - current
		if _, err := seeker.Seek(current, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek back: %w", err)
		}
	}

	_, err := ms.client.PutObject(ctx, ms.bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to write from reader: %w", err)
	}
	return nil
}

func (ms *MinIOStorage) Read(objectKey string) ([]byte, error) {
	if err := ms.validatePath(objectKey); err != nil {
		return nil, err
	}

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	obj, err := ms.client.GetObject(ctx, ms.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object: %w", err)
	}
	return data, nil
}

func (ms *MinIOStorage) ReadAt(objectKey string, size int, offset int64) ([]byte, error) {
	if err := ms.validatePath(objectKey); err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, fmt.Errorf("invalid read size: %d", size)
	}
	if offset < 0 {
		return nil, fmt.Errorf("invalid read offset: %d", offset)
	}

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+int64(size)-1); err != nil {
		return nil, fmt.Errorf("failed to set range: %w", err)
	}

	obj, err := ms.client.GetObject(ctx, ms.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get object range: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(io.LimitReader(obj, int64(size)))
	if err != nil {
		return nil, fmt.Errorf("failed to read object range: %w", err)
	}
	return data, nil
}

func (ms *MinIOStorage) OpenReader(objectKey string) (io.ReadCloser, error) {
	if err := ms.validatePath(objectKey); err != nil {
		return nil, err
	}

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	obj, err := ms.client.GetObject(ctx, ms.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	return obj, nil
}

func (ms *MinIOStorage) Remove(objectKey string) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}
	mu := ms.getLock(objectKey)
	mu.Lock()
	defer mu.Unlock()

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	err := ms.client.RemoveObject(ctx, ms.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove object: %w", err)
	}
	ms.pathLocks.Delete(objectKey)
	return nil
}

func (ms *MinIOStorage) Exists(objectKey string) bool {
	if err := ms.validatePath(objectKey); err != nil {
		return false
	}

	key := ms.normalizeKey(objectKey)
	if strings.HasSuffix(objectKey, "/") && !strings.HasSuffix(key, "/") {
		key = key + "/"
	}
	ctx := context.Background()

	_, err := ms.client.StatObject(ctx, ms.bucket, key, minio.StatObjectOptions{})
	return err == nil
}

func (ms *MinIOStorage) List(prefix string) ([]string, error) {
	if err := ms.validatePath(prefix); err != nil {
		return nil, err
	}

	key := ms.normalizeKey(prefix)
	ctx := context.Background()

	var names []string
	objectCh := ms.client.ListObjects(ctx, ms.bucket, minio.ListObjectsOptions{
		Prefix:    key + "/",
		Recursive: false,
	})
	for obj := range objectCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", obj.Err)
		}
		relPath := strings.TrimPrefix(obj.Key, key+"/")
		if relPath != "" {
			parts := strings.SplitN(relPath, "/", 2)
			name := parts[0]
			found := false
			for _, n := range names {
				if n == name {
					found = true
					break
				}
			}
			if !found {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

func (ms *MinIOStorage) GetSize(objectKey string) (int64, error) {
	if err := ms.validatePath(objectKey); err != nil {
		return 0, err
	}

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	info, err := ms.client.StatObject(ctx, ms.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to stat object: %w", err)
	}
	return info.Size, nil
}

func (ms *MinIOStorage) Rename(oldKey, newKey string) error {
	if err := ms.validatePath(oldKey); err != nil {
		return err
	}
	if err := ms.validatePath(newKey); err != nil {
		return err
	}

	if oldKey == newKey {
		return nil
	}

	oldMu := ms.getLock(oldKey)
	newMu := ms.getLock(newKey)

	if oldKey < newKey {
		oldMu.Lock()
		newMu.Lock()
	} else {
		newMu.Lock()
		oldMu.Lock()
	}
	defer oldMu.Unlock()
	defer newMu.Unlock()

	src := ms.normalizeKey(oldKey)
	dst := ms.normalizeKey(newKey)
	ctx := context.Background()

	srcObj, err := ms.client.GetObject(ctx, ms.bucket, src, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get source object: %w", err)
	}
	defer srcObj.Close()

	info, err := ms.client.StatObject(ctx, ms.bucket, src, minio.StatObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to stat source object: %w", err)
	}

	_, err = ms.client.PutObject(ctx, ms.bucket, dst, srcObj, info.Size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to copy object: %w", err)
	}

	if err := ms.client.RemoveObject(ctx, ms.bucket, src, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("failed to remove source object after rename: %w", err)
	}

	ms.pathLocks.Delete(oldKey)
	return nil
}

func (ms *MinIOStorage) CreateDirectory(prefix string) error {
	if err := ms.validatePath(prefix); err != nil {
		return err
	}

	key := ms.normalizeKey(prefix)
	if !strings.HasSuffix(key, "/") {
		key = key + "/"
	}

	ctx := context.Background()
	emptyBody := bytes.NewReader(make([]byte, 0))
	_, err := ms.client.PutObject(ctx, ms.bucket, key, emptyBody, 0, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to create directory marker: %w", err)
	}
	return nil
}

func (ms *MinIOStorage) RemoveDirectory(prefix string) error {
	if err := ms.validatePath(prefix); err != nil {
		return err
	}

	key := ms.normalizeKey(prefix)
	if !strings.HasSuffix(key, "/") {
		key = key + "/"
	}

	ctx := context.Background()

	objectsCh := ms.client.ListObjects(ctx, ms.bucket, minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			return fmt.Errorf("failed to list objects for removal: %w", obj.Err)
		}
		if err := ms.client.RemoveObject(ctx, ms.bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to remove object %s: %w", obj.Key, err)
		}
	}

	ms.pathLocks.Delete(prefix)
	return nil
}

func (ms *MinIOStorage) CleanPathLocks() {
	ms.pathLocks.Range(func(key, _ interface{}) bool {
		objectKey := key.(string)
		if !ms.Exists(objectKey) {
			ms.pathLocks.Delete(key)
		}
		return true
	})
}
