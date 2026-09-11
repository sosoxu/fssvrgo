package storage

import (
	"bytes"
	"context"
	"errors"
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

// ValidatePath reports whether objectKey stays inside the bucket namespace.
// Implementation detail: StorageAdapter does not expose it, because every
// operation rejects escaping keys on its own. Exported for backend tests.
func (ms *MinIOStorage) ValidatePath(objectKey string) error {
	if objectKey == "" {
		return fmt.Errorf("object key cannot be empty")
	}
	if strings.HasPrefix(objectKey, "/") {
		return fmt.Errorf("object key must not start with /: %s", objectKey)
	}
	// Inspect each path segment so that legitimate names containing ".." as a
	// substring (e.g. "file..txt", "ver..1.0") are not falsely rejected as a
	// traversal. Only a complete ".." segment denotes a parent-directory
	// traversal. A trailing "/" is allowed because it is used as a directory
	// marker by CreateDirectory/RemoveDirectory.
	segments := strings.Split(strings.TrimSuffix(objectKey, "/"), "/")
	for _, part := range segments {
		if part == ".." {
			return fmt.Errorf("path traversal detected: %s", objectKey)
		}
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

// ErrWriteAtUnsupported 表示对象存储不支持原地随机写。对象存储没有字节级
// 随机写语义，read-modify-write 会把整个对象读入内存（OOM 风险）且非原子；
// 调用方应使用 multipart upload 或整体 Write 覆盖。
var ErrWriteAtUnsupported = errors.New("storage: WriteAt is not supported on object storage; use multipart upload or Write instead")

// WriteAt always fails with ErrWriteAtUnsupported: object storage cannot do
// in-place random writes. The method is kept as an explicit, tested statement
// of that limitation even though StorageAdapter no longer carries it.
func (ms *MinIOStorage) WriteAt(objectKey string, data []byte, offset int64) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}
	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}
	// 对象存储不支持原地随机写：read-modify-write 会把整个对象读入内存
	// （OOM 风险）且读-改-写非原子。调用方应使用 multipart upload 或整体 Write 覆盖。
	return ErrWriteAtUnsupported
}

func (ms *MinIOStorage) WriteFromTempFile(objectKey string, tempFilePath string) error {
	if err := ms.validatePath(objectKey); err != nil {
		return err
	}
	// Validate that the temp file is inside the system temp directory, mirroring
	// LocalStorage. Without this, the MinIO backend would happily upload any
	// local file (e.g. /etc/shadow) referenced by an untrusted caller.
	if err := validateTempFilePath(tempFilePath); err != nil {
		return err
	}

	mu := ms.getLock(objectKey)
	mu.Lock()
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
	// 不在此处删除 pathLocks 中的锁条目，避免锁逃逸（见 local.go Remove 注释）。
	return nil
}

// Exists reports whether objectKey (an object, or a directory-like prefix that
// has children) exists. Backend failures such as network errors are returned
// rather than being collapsed into "absent".
func (ms *MinIOStorage) Exists(objectKey string) (bool, error) {
	if err := ms.validatePath(objectKey); err != nil {
		return false, err
	}

	key := ms.normalizeKey(objectKey)
	ctx := context.Background()

	// Try exact object match first (covers both leaf objects and explicit
	// directory markers written as "<key>/").
	lookupKey := key
	if strings.HasSuffix(objectKey, "/") && !strings.HasSuffix(key, "/") {
		lookupKey = key + "/"
	}
	if _, err := ms.client.StatObject(ctx, ms.bucket, lookupKey, minio.StatObjectOptions{}); err == nil {
		return true, nil
	} else if !IsNotExist(err) {
		return false, fmt.Errorf("failed to stat object %s: %w", objectKey, err)
	}

	// No exact object exists. For a non-marker key (not ending with "/"), fall
	// back to checking whether any child object exists under the "<key>/"
	// prefix. This makes Exists("foo") consistent with LocalStorage, where a
	// directory is reported as existing whenever it contains children.
	if !strings.HasSuffix(key, "/") {
		listCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		objectCh := ms.client.ListObjects(listCtx, ms.bucket, minio.ListObjectsOptions{
			Prefix:    key + "/",
			Recursive: false,
		})
		for obj := range objectCh {
			if obj.Err != nil {
				if IsNotExist(obj.Err) {
					return false, nil
				}
				return false, fmt.Errorf("failed to list objects under %s: %w", objectKey, obj.Err)
			}
			return true, nil
		}
		return false, nil
	}
	return false, nil
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

// ListObjects returns every object in the bucket as a slash-separated key
// relative to the bucket root. Directory markers (keys ending in "/") are
// skipped so the result matches the path space used by the metadata tables.
//
// Like LocalStorage.ListObjects it is an optional capability used by the
// reconciler, not part of StorageAdapter.
func (ms *MinIOStorage) ListObjects(ctx context.Context) ([]string, error) {
	var objects []string
	objectCh := ms.client.ListObjects(ctx, ms.bucket, minio.ListObjectsOptions{Recursive: true})
	for obj := range objectCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", obj.Err)
		}
		if strings.HasSuffix(obj.Key, "/") {
			continue
		}
		objects = append(objects, strings.TrimPrefix(obj.Key, "/"))
	}
	return objects, nil
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

	// 不在此处删除 oldKey 的锁条目，避免锁逃逸（见 local.go Remove 注释）。
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

	// 不在此处删除 prefix 的锁条目，避免锁逃逸（见 local.go Remove 注释）。
	return nil
}

func (ms *MinIOStorage) CleanPathLocks() {
	ms.pathLocks.Range(func(key, value interface{}) bool {
		objectKey := key.(string)
		mu := value.(*sync.Mutex)
		// TryLock 成功说明当前无 goroutine 持有该锁，可安全删除；
		// 失败则跳过，避免删除正在使用的锁条目导致锁逃逸。
		if !mu.TryLock() {
			return true
		}
		mu.Unlock()
		exists, err := ms.Exists(objectKey)
		if err != nil {
			// Keep the lock entry when existence cannot be determined; dropping
			// it here could let a concurrent writer race on the same key.
			return true
		}
		if !exists {
			ms.pathLocks.Delete(key)
		}
		return true
	})
}
