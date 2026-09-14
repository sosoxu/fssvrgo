package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// Bottleneck breakdown test.
//
// The end-to-end performance suite (TestPostgreSQLRedis_Performance) reports the
// throughput of each protocol/operation but does not explain *where* the time
// goes. This test peels the upload path apart layer by layer so the dominant
// cost can be attributed:
//
//	upload (redis) = in-proc mutex
//	               + Redis distributed lock (SETNX + renewal + Lua DEL)
//	               + PG metadata existence check (SELECT COUNT)
//	               + storage write (local FS)
//	               + SHA-256 over the payload
//	               + PG metadata upsert (SELECT + INSERT/UPDATE)
//
// It is OPT-IN: only runs when FSS_BENCH_ENABLED=1, and skips when PostgreSQL
// or Redis is unavailable (see NewPerfCluster).
func TestPostgreSQLRedis_BottleneckBreakdown(t *testing.T) {
	if os.Getenv("FSS_BENCH_ENABLED") != "1" {
		t.Skip("skipped: set FSS_BENCH_ENABLED=1 to run the bottleneck breakdown test")
	}

	sizes := []struct {
		bytes int64
		iters int
	}{
		{1 * 1024, 50},
		{64 * 1024, 30},
		{1024 * 1024, 20},
		{10 * 1024 * 1024, 15},
	}

	cluster := NewPerfCluster(t)
	defer cluster.Cleanup()

	store := cluster.SharedStore
	db := cluster.SharedDB
	metaSvc := database.NewFileMetadataService(db)
	redisLock := cluster.RedisManager.GetLock()

	// Two FileManagers over the same storage/db: one backed by the in-process
	// lock, one by the Redis distributed lock. The delta between their upload
	// paths is the pure cost of the distributed lock.
	localFM := filemanager.NewFileManager(store, db)
	redisFM := filemanager.NewFileManagerWithDistLock(store, db, redisLock)

	ctx := context.Background()

	var results []layerResult
	record := func(layer string, sizeInt int64, d time.Duration, note string) {
		results = append(results, layerResult{
			Layer:   layer,
			Size:    formatSize(sizeInt),
			SizeInt: sizeInt,
			Avg:     d,
			Note:    note,
		})
	}

	for _, sc := range sizes {
		data := generatePerfData(int(sc.bytes))
		sz := formatSize(sc.bytes)
		t.Logf("=== File Size: %s (iters=%d) ===", sz, sc.iters)

		// --- 1) Local filesystem floor -------------------------------------
		rawPath := fmt.Sprintf("/bn/raw/%s/file.dat", sz)
		if err := store.Write(rawPath, data); err != nil {
			t.Fatalf("seed raw write failed: %v", err)
		}
		// Seed a metadata row so the SELECT-based layers below have a hit.
		if err := metaSvc.Create(&database.FileMetadata{
			ID:          utils.GenerateUUID(),
			Path:        rawPath,
			Name:        "file.dat",
			Size:        sc.bytes,
			Hash:        fmt.Sprintf("%x", sha256.Sum256(data)),
			StorageType: store.StorageType(),
			CreatedAt:   utils.GetCurrentTimestamp(),
			UpdatedAt:   utils.GetCurrentTimestamp(),
		}); err != nil {
			t.Fatalf("seed metadata failed: %v", err)
		}

		d := measureLayer(t, "storage.Write", sc.iters, func(i int) error {
			// Unique path per iteration: a real upload never overwrites the same
			// file twice, and reusing one path would measure page-cache overwrite.
			return store.Write(fmt.Sprintf("/bn/raw/%s/%d.dat", sz, i), data)
		})
		record("storage.Write", sc.bytes, d, "本地文件系统整块写入下限")

		d = measureLayer(t, "storage.Read", sc.iters, func(int) error {
			_, err := store.Read(rawPath)
			return err
		})
		record("storage.Read", sc.bytes, d, "本地文件系统整块读取下限")

		d = measureLayer(t, "storage.WriteFromReader", sc.iters, func(i int) error {
			return store.WriteFromReader(fmt.Sprintf("/bn/stream/%s/%d.dat", sz, i), bytes.NewReader(data))
		})
		record("storage.WriteFromReader", sc.bytes, d, "流式写入（io.Copy 缓冲）")

		d = measureLayer(t, "sha256.Sum256", sc.iters, func(int) error {
			_ = sha256.Sum256(data)
			return nil
		})
		record("sha256.Sum256", sc.bytes, d, "纯 CPU 哈希成本")

		// --- 2) PostgreSQL metadata ----------------------------------------
		d = measureLayer(t, "meta.Exists", sc.iters, func(int) error {
			_, err := metaSvc.Exists(rawPath)
			return err
		})
		record("PG metadata Exists", sc.bytes, d, "上传前一次 SELECT COUNT")

		d = measureLayer(t, "meta.GetByPath", sc.iters, func(int) error {
			_, err := metaSvc.GetByPath(rawPath)
			return err
		})
		record("PG metadata GetByPath", sc.bytes, d, "下载/流式上传的单行查询")

		d = measureLayer(t, "meta.Create", sc.iters, func(i int) error {
			return metaSvc.Create(&database.FileMetadata{
				ID:          utils.GenerateUUID(),
				Path:        fmt.Sprintf("/bn/meta/%s/%d.dat", sz, i),
				Name:        fmt.Sprintf("%d.dat", i),
				Size:        sc.bytes,
				Hash:        "benchmark-hash",
				StorageType: store.StorageType(),
				CreatedAt:   utils.GetCurrentTimestamp(),
				UpdatedAt:   utils.GetCurrentTimestamp(),
			})
		})
		record("PG metadata Create", sc.bytes, d, "SELECT(已删) + INSERT")

		// --- 3) Redis distributed lock -------------------------------------
		lockKey := fmt.Sprintf("bn:lock:%s", sz)
		d = measureLayer(t, "redis.Lock+Unlock", sc.iters, func(int) error {
			token, err := redisLock.Lock(ctx, lockKey, 10*time.Second)
			if err != nil {
				return err
			}
			return redisLock.Unlock(ctx, lockKey, token)
		})
		record("Redis Lock/Unlock", sc.bytes, d, "SETNX + Lua(DEL)，2 次 RTT")

		d = measureLayer(t, "redis.LockWithRenewal", sc.iters, func(int) error {
			token, cancel, err := distributed.AcquireLockWithRenewal(
				ctx, redisLock, lockKey, 10*time.Second, 30, 50*time.Millisecond)
			if err != nil {
				return err
			}
			cancel()
			return redisLock.Unlock(ctx, lockKey, token)
		})
		record("Redis Lock+续约+Unlock", sc.bytes, d, "UploadFile 实际使用的锁路径")

		// --- 4) Full upload path, local lock vs Redis lock -----------------
		// Run the two variants interleaved (local, redis, local, redis, ...) so
		// that disk-cache / GC drift affects both equally and the difference
		// isolates the distributed-lock cost.
		if _, err := localFM.UploadFile(fmt.Sprintf("/bn/warm/%s/local.dat", sz), data); err != nil {
			t.Fatalf("local warm-up failed: %v", err)
		}
		if _, err := redisFM.UploadFile(fmt.Sprintf("/bn/warm/%s/redis.dat", sz), data); err != nil {
			t.Fatalf("redis warm-up failed: %v", err)
		}
		var localTotal, redisTotal time.Duration
		for i := 0; i < sc.iters; i++ {
			// Alternate which variant runs first each iteration: the kernel may
			// write back the previous file while the next operation runs, and
			// always measuring one variant second would bias the comparison.
			localPath := fmt.Sprintf("/bn/local/%s/%d.dat", sz, i)
			redisPath := fmt.Sprintf("/bn/redis/%s/%d.dat", sz, i)
			runLocal := func() {
				start := time.Now()
				if _, err := localFM.UploadFile(localPath, data); err != nil {
					t.Fatalf("local upload %d failed: %v", i, err)
				}
				localTotal += time.Since(start)
			}
			runRedis := func() {
				start := time.Now()
				if _, err := redisFM.UploadFile(redisPath, data); err != nil {
					t.Fatalf("redis upload %d failed: %v", i, err)
				}
				redisTotal += time.Since(start)
			}
			if i%2 == 0 {
				runLocal()
				runRedis()
			} else {
				runRedis()
				runLocal()
			}
		}
		d = localTotal / time.Duration(sc.iters)
		record("UploadFile (进程内锁)", sc.bytes, d, "无 Redis：锁+Exists+写+哈希+Create")
		d = redisTotal / time.Duration(sc.iters)
		record("UploadFile (Redis 锁)", sc.bytes, d, "生产上传路径")

		// --- 5) Download path (meta lookup + read, no locks) ---------------
		dlPath := fmt.Sprintf("/bn/download/%s/file.dat", sz)
		if _, err := redisFM.UploadFile(dlPath, data); err != nil {
			t.Fatalf("download setup failed: %v", err)
		}
		d = measureLayer(t, "DownloadFile", sc.iters, func(int) error {
			got, err := localFM.DownloadFile(dlPath)
			if err != nil {
				return err
			}
			if len(got) != int(sc.bytes) {
				return fmt.Errorf("size mismatch: got %d", len(got))
			}
			return nil
		})
		record("DownloadFile", sc.bytes, d, "GetByPath + storage.Read")

		cluster.CleanFiles()
	}

	logBottleneckBreakdown(t, sizes, results)
}

type layerResult struct {
	Layer   string
	Size    string
	SizeInt int64
	Avg     time.Duration
	Note    string
}

// measureLayer runs fn iters times (after one untimed warm-up) and returns the
// average duration. The warm-up keeps connection setup / cold caches out of the
// measurement.
func measureLayer(t *testing.T, label string, iters int, fn func(i int) error) time.Duration {
	t.Helper()
	if err := fn(0); err != nil {
		t.Fatalf("%s warm-up failed: %v", label, err)
	}
	var total time.Duration
	for i := 0; i < iters; i++ {
		start := time.Now()
		if err := fn(i + 1); err != nil {
			t.Fatalf("%s iteration %d failed: %v", label, i, err)
		}
		total += time.Since(start)
	}
	return total / time.Duration(iters)
}

func logBottleneckBreakdown(t *testing.T, sizes []struct {
	bytes int64
	iters int
}, results []layerResult) {
	t.Helper()

	sep := strings.Repeat("-", 92)

	t.Log("\n========== Layer Breakdown (avg per operation) ==========")
	t.Log(sep)
	t.Logf("%-10s %-26s %-14s %s", "Size", "Layer", "Avg Time", "Note")
	t.Log(sep)

	sorted := append([]layerResult(nil), results...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].SizeInt != sorted[j].SizeInt {
			return sorted[i].SizeInt < sorted[j].SizeInt
		}
		return false // keep insertion order within a size
	})
	for _, r := range sorted {
		t.Logf("%-10s %-26s %-14v %s", r.Size, r.Layer, r.Avg.Round(time.Microsecond), r.Note)
	}
	t.Log(sep)

	// Attribution: how much of a full Redis-backed upload each layer explains.
	t.Log("\n========== Upload Cost Attribution ==========")
	t.Log("完整上传 = storage.Write + sha256 + PG Exists + Redis 锁(含续约) + PG Create + 其余")
	t.Log(sep)
	t.Logf("%-10s %-13s %-13s %-13s %-13s %-13s %-13s %-13s",
		"Size", "storage.Write", "sha256", "PG Exists", "Redis Lock", "PG Create", "Sum", "Full Upload")
	t.Log(sep)

	for _, sc := range sizes {
		sz := formatSize(sc.bytes)
		get := func(layer string) time.Duration {
			for _, r := range results {
				if r.SizeInt == sc.bytes && r.Layer == layer {
					return r.Avg
				}
			}
			return 0
		}
		writeT := get("storage.Write")
		shaT := get("sha256.Sum256")
		existsT := get("PG metadata Exists")
		lockT := get("Redis Lock+续约+Unlock")
		createT := get("PG metadata Create")
		sum := writeT + shaT + existsT + lockT + createT
		full := get("UploadFile (Redis 锁)")
		localFull := get("UploadFile (进程内锁)")

		t.Logf("%-10s %-13v %-13v %-13v %-13v %-13v %-13v %-13v",
			sz,
			writeT.Round(time.Microsecond),
			shaT.Round(time.Microsecond),
			existsT.Round(time.Microsecond),
			lockT.Round(time.Microsecond),
			createT.Round(time.Microsecond),
			sum.Round(time.Microsecond),
			full.Round(time.Microsecond))

		// Isolate the distributed-lock premium: (redis upload - local upload).
		premium := full - localFull
		t.Logf("           -> 分布式锁溢价(Redis 上传 - 进程内上传): %v；组件之和占比: %.0f%%",
			premium.Round(time.Microsecond),
			percent(sum, full))
		if sc.bytes <= 64*1024 {
			t.Logf("           -> 固定协调开销(SELECT+锁+哈希+INSERT) 占完整上传: %.0f%%",
				percent(existsT+lockT+createT+shaT, full))
		}
	}
	t.Log(sep)
}

func percent(part, total time.Duration) float64 {
	if total <= 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}
