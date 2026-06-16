package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func newBenchMinIOStorage(b *testing.B) *MinIOStorage {
	b.Helper()

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

	bucket := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		b.Fatalf("failed to create minio client: %v", err)
	}

	ctx := context.Background()
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	b.Cleanup(func() {
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

func newBenchLocalStorage(b *testing.B) *LocalStorage {
	b.Helper()
	dir := b.TempDir()
	return NewLocalStorage(dir)
}

func generateBenchData(size int64) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func benchWrite(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench/write/%d/%d", size, i)
		if err := store.Write(key, data); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func benchRead(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	key := fmt.Sprintf("bench/read/%d", size)
	if err := store.Write(key, data); err != nil {
		b.Fatalf("Write setup failed: %v", err)
	}

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := store.Read(key); err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

func benchWriteFromReader(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench/writer/%d/%d", size, i)
		if err := store.WriteFromReader(key, bytes.NewReader(data)); err != nil {
			b.Fatalf("WriteFromReader failed: %v", err)
		}
	}
}

func benchOpenReader(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	key := fmt.Sprintf("bench/openreader/%d", size)
	if err := store.Write(key, data); err != nil {
		b.Fatalf("Write setup failed: %v", err)
	}

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := store.OpenReader(key)
		if err != nil {
			b.Fatalf("OpenReader failed: %v", err)
		}
		buf := make([]byte, len(data))
		if _, err := reader.Read(buf); err != nil && err.Error() != "EOF" {
			b.Fatalf("Read from reader failed: %v", err)
		}
		reader.Close()
	}
}

func benchReadAt(b *testing.B, store StorageAdapter, size int64, chunkSize int, offset int64) {
	data := generateBenchData(size)
	key := fmt.Sprintf("bench/readat/%d", size)
	if err := store.Write(key, data); err != nil {
		b.Fatalf("Write setup failed: %v", err)
	}

	b.SetBytes(int64(chunkSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := store.ReadAt(key, chunkSize, offset); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

func benchConcurrentWrite(b *testing.B, store StorageAdapter, size int64, concurrency int) {
	data := generateBenchData(size)
	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		errCh := make(chan error, concurrency)
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("bench/concwrite/%d/%d/%d", size, i, idx)
				if err := store.Write(key, data); err != nil {
					errCh <- err
				}
			}(j)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			b.Fatalf("Concurrent Write failed: %v", err)
		}
	}
}

func benchConcurrentRead(b *testing.B, store StorageAdapter, size int64, concurrency int) {
	data := generateBenchData(size)
	key := fmt.Sprintf("bench/concread/%d", size)
	if err := store.Write(key, data); err != nil {
		b.Fatalf("Write setup failed: %v", err)
	}

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		errCh := make(chan error, concurrency)
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := store.Read(key); err != nil {
					errCh <- err
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			b.Fatalf("Concurrent Read failed: %v", err)
		}
	}
}

func benchRename(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oldKey := fmt.Sprintf("bench/rename/old/%d/%d", size, i)
		newKey := fmt.Sprintf("bench/rename/new/%d/%d", size, i)
		if err := store.Write(oldKey, data); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := store.Rename(oldKey, newKey); err != nil {
			b.Fatalf("Rename failed: %v", err)
		}
	}
}

func benchRemove(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench/remove/%d/%d", size, i)
		if err := store.Write(key, data); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := store.Remove(key); err != nil {
			b.Fatalf("Remove failed: %v", err)
		}
	}
}

func benchExists(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	key := fmt.Sprintf("bench/exists/%d", size)
	if err := store.Write(key, data); err != nil {
		b.Fatalf("Write setup failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !store.Exists(key) {
			b.Fatal("Exists should return true")
		}
	}
}

func benchGetSize(b *testing.B, store StorageAdapter, size int64) {
	data := generateBenchData(size)
	key := fmt.Sprintf("bench/getsize/%d", size)
	if err := store.Write(key, data); err != nil {
		b.Fatalf("Write setup failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := store.GetSize(key); err != nil {
			b.Fatalf("GetSize failed: %v", err)
		}
	}
}

var benchSizes = []struct {
	size int64
	name string
}{
	{1 * 1024, "1KB"},
	{64 * 1024, "64KB"},
	{256 * 1024, "256KB"},
	{1 * 1024 * 1024, "1MB"},
	{4 * 1024 * 1024, "4MB"},
	{16 * 1024 * 1024, "16MB"},
}

func BenchmarkLocal_Write(b *testing.B) {
	store := newBenchLocalStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchWrite(b, store, bs.size) })
	}
}

func BenchmarkMinIO_Write(b *testing.B) {
	store := newBenchMinIOStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchWrite(b, store, bs.size) })
	}
}

func BenchmarkLocal_Read(b *testing.B) {
	store := newBenchLocalStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchRead(b, store, bs.size) })
	}
}

func BenchmarkMinIO_Read(b *testing.B) {
	store := newBenchMinIOStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchRead(b, store, bs.size) })
	}
}

func BenchmarkLocal_WriteFromReader(b *testing.B) {
	store := newBenchLocalStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchWriteFromReader(b, store, bs.size) })
	}
}

func BenchmarkMinIO_WriteFromReader(b *testing.B) {
	store := newBenchMinIOStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchWriteFromReader(b, store, bs.size) })
	}
}

func BenchmarkLocal_OpenReader(b *testing.B) {
	store := newBenchLocalStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchOpenReader(b, store, bs.size) })
	}
}

func BenchmarkMinIO_OpenReader(b *testing.B) {
	store := newBenchMinIOStorage(b)
	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) { benchOpenReader(b, store, bs.size) })
	}
}

func BenchmarkLocal_ReadAt(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB_4K_at_512K", func(b *testing.B) { benchReadAt(b, store, 1<<20, 4<<10, 512<<10) })
	b.Run("16MB_1M_at_8M", func(b *testing.B) { benchReadAt(b, store, 16<<20, 1<<20, 8<<20) })
}

func BenchmarkMinIO_ReadAt(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB_4K_at_512K", func(b *testing.B) { benchReadAt(b, store, 1<<20, 4<<10, 512<<10) })
	b.Run("16MB_1M_at_8M", func(b *testing.B) { benchReadAt(b, store, 16<<20, 1<<20, 8<<20) })
}

func BenchmarkLocal_ConcurrentWrite(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB_x4", func(b *testing.B) { benchConcurrentWrite(b, store, 1<<20, 4) })
	b.Run("1MB_x8", func(b *testing.B) { benchConcurrentWrite(b, store, 1<<20, 8) })
	b.Run("4MB_x4", func(b *testing.B) { benchConcurrentWrite(b, store, 4<<20, 4) })
}

func BenchmarkMinIO_ConcurrentWrite(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB_x4", func(b *testing.B) { benchConcurrentWrite(b, store, 1<<20, 4) })
	b.Run("1MB_x8", func(b *testing.B) { benchConcurrentWrite(b, store, 1<<20, 8) })
	b.Run("4MB_x4", func(b *testing.B) { benchConcurrentWrite(b, store, 4<<20, 4) })
}

func BenchmarkLocal_ConcurrentRead(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB_x4", func(b *testing.B) { benchConcurrentRead(b, store, 1<<20, 4) })
	b.Run("1MB_x8", func(b *testing.B) { benchConcurrentRead(b, store, 1<<20, 8) })
	b.Run("4MB_x4", func(b *testing.B) { benchConcurrentRead(b, store, 4<<20, 4) })
}

func BenchmarkMinIO_ConcurrentRead(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB_x4", func(b *testing.B) { benchConcurrentRead(b, store, 1<<20, 4) })
	b.Run("1MB_x8", func(b *testing.B) { benchConcurrentRead(b, store, 1<<20, 8) })
	b.Run("4MB_x4", func(b *testing.B) { benchConcurrentRead(b, store, 4<<20, 4) })
}

func BenchmarkLocal_Rename(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB", func(b *testing.B) { benchRename(b, store, 1<<20) })
	b.Run("4MB", func(b *testing.B) { benchRename(b, store, 4<<20) })
}

func BenchmarkMinIO_Rename(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB", func(b *testing.B) { benchRename(b, store, 1<<20) })
	b.Run("4MB", func(b *testing.B) { benchRename(b, store, 4<<20) })
}

func BenchmarkLocal_Remove(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB", func(b *testing.B) { benchRemove(b, store, 1<<20) })
}

func BenchmarkMinIO_Remove(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB", func(b *testing.B) { benchRemove(b, store, 1<<20) })
}

func BenchmarkLocal_Exists(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB", func(b *testing.B) { benchExists(b, store, 1<<20) })
}

func BenchmarkMinIO_Exists(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB", func(b *testing.B) { benchExists(b, store, 1<<20) })
}

func BenchmarkLocal_GetSize(b *testing.B) {
	store := newBenchLocalStorage(b)
	b.Run("1MB", func(b *testing.B) { benchGetSize(b, store, 1<<20) })
}

func BenchmarkMinIO_GetSize(b *testing.B) {
	store := newBenchMinIOStorage(b)
	b.Run("1MB", func(b *testing.B) { benchGetSize(b, store, 1<<20) })
}
