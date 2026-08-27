package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/storage"
)

// This test is intentionally opt-in because the 1G case writes several GB
// when concurrency is enabled. It exercises the same PostgreSQL-backed
// FileManager path used by the service while streaming payloads from a small
// repeating pattern.

type fileIOPerfSize struct {
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
}

var fileIOPerfSizes = []fileIOPerfSize{
	{Name: "1K", Bytes: 1 << 10},
	{Name: "1M", Bytes: 1 << 20},
	{Name: "100M", Bytes: 100 << 20},
	{Name: "1G", Bytes: 1 << 30},
}

var fileIOPerfOperations = []string{
	"sequential_write",
	"sequential_read",
	"concurrent_write",
	"concurrent_read",
}

type fileIOPerfMeasurement struct {
	Size          string             `json:"size"`
	SizeBytes     int64              `json:"size_bytes"`
	Operation     string             `json:"operation"`
	Concurrency   int                `json:"concurrency"`
	WarmupRuns    int                `json:"warmup_runs"`
	SampleRuns    int                `json:"sample_runs"`
	ErrorCount    int                `json:"error_count"`
	ErrorRate     float64            `json:"error_rate"`
	P50DurationMS float64            `json:"p50_duration_ms"`
	P95DurationMS float64            `json:"p95_duration_ms"`
	P99DurationMS float64            `json:"p99_duration_ms"`
	P50MiBPerSec  float64            `json:"p50_mib_per_sec"`
	P95MiBPerSec  float64            `json:"p95_mib_per_sec"`
	P99MiBPerSec  float64            `json:"p99_mib_per_sec"`
	Samples       []fileIOPerfSample `json:"samples"`
}

type fileIOPerfSample struct {
	Run         int     `json:"run"`
	DurationMS  float64 `json:"duration_ms"`
	MiBPerSec   float64 `json:"mib_per_sec"`
	Transferred int64   `json:"transferred_bytes"`
	Error       string  `json:"error,omitempty"`
}

type fileIOPerfReport struct {
	GeneratedAt  time.Time               `json:"generated_at"`
	GoVersion    string                  `json:"go_version"`
	OS           string                  `json:"os"`
	Arch         string                  `json:"arch"`
	GOMAXPROCS   int                     `json:"gomaxprocs"`
	Concurrency  int                     `json:"concurrency"`
	WarmupRuns   int                     `json:"warmup_runs"`
	SampleRuns   int                     `json:"sample_runs"`
	Backend      string                  `json:"backend"`
	Protocol     string                  `json:"protocol"`
	Redis        string                  `json:"redis"`
	Encryption   string                  `json:"encryption"`
	Measurements []fileIOPerfMeasurement `json:"measurements"`
}

type repeatingReader struct {
	remaining int64
	pattern   []byte
	index     int
}

func newRepeatingReader(size int64) *repeatingReader {
	return newRepeatingReaderAt(size, 0)
}

func newRepeatingReaderAt(size, offset int64) *repeatingReader {
	pattern := []byte("fssvr-performance-pattern-2026-io-")
	return &repeatingReader{
		remaining: size,
		pattern:   pattern,
		index:     int(offset % int64(len(pattern))),
	}
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	written := 0
	for written < len(p) {
		n := copy(p[written:], r.pattern[r.index:])
		written += n
		r.index = (r.index + n) % len(r.pattern)
	}
	r.remaining -= int64(len(p))
	return len(p), nil
}

func TestFileIOPerformance(t *testing.T) {
	if os.Getenv("FSS_PERF_ENABLE") != "1" {
		t.Skip("set FSS_PERF_ENABLE=1 to run the 1K/1M/100M/1G file performance suite")
	}

	concurrency := perfConcurrency(t)
	warmupRuns := perfRunCount(t, "FSS_PERF_WARMUPS", 0, true)
	sampleRuns := perfRunCount(t, "FSS_PERF_SAMPLES", 3, false)
	sizes := selectedFileIOPerfSizes(t)
	operations := selectedFileIOPerfOperations(t)
	env := setupPerfEnv(t)
	report := fileIOPerfReport{
		GeneratedAt: time.Now().UTC(),
		GoVersion:   runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
		Concurrency: concurrency,
		WarmupRuns:  warmupRuns,
		SampleRuns:  sampleRuns,
		Backend:     "PostgreSQL + LocalStorage",
		Protocol:    "in-process FileManager",
		Redis:       "disabled",
		Encryption:  "disabled",
	}

	for _, size := range sizes {
		size := size
		t.Run(size.Name, func(t *testing.T) {
			sequentialWritePath := func(run int) string {
				return filepath.ToSlash(filepath.Join("perf", size.Name, fmt.Sprintf("sequential-write-%d.bin", run)))
			}
			if operations["sequential_write"] {
				writeMeasurement := measureFileIO(t, size, "sequential_write", 1, warmupRuns, sampleRuns, func(run int) error {
					_, err := env.fm.UploadFileFromReader(sequentialWritePath(run), newRepeatingReader(size.Bytes))
					return err
				})
				report.Measurements = append(report.Measurements, writeMeasurement)
				verifySequentialWriteFiles(t, env.storage, size, warmupRuns, sampleRuns)
			}

			if operations["sequential_read"] {
				readPath := sequentialWritePath(warmupRuns)
				if !operations["sequential_write"] {
					readPath = filepath.ToSlash(filepath.Join("perf", size.Name, "sequential-read-source.bin"))
					if _, err := env.fm.UploadFileFromReader(readPath, newRepeatingReader(size.Bytes)); err != nil {
						t.Fatalf("prepare sequential read source: %v", err)
					}
				}
				readMeasurement := measureFileIO(t, size, "sequential_read", 1, warmupRuns, sampleRuns, func(_ int) error {
					return streamFileRead(env, readPath, size.Bytes)
				})
				report.Measurements = append(report.Measurements, readMeasurement)
			}

			if operations["concurrent_write"] {
				concurrentWriteMeasurement := measureFileIO(t, size, "concurrent_write", concurrency, warmupRuns, sampleRuns, func(run int) error {
					return concurrentFileWrites(env, size, concurrency, run)
				})
				report.Measurements = append(report.Measurements, concurrentWriteMeasurement)
				verifyConcurrentWriteFiles(t, env.storage, size, concurrency, warmupRuns, sampleRuns)
			}

			if operations["concurrent_read"] {
				concurrentReadPath := filepath.ToSlash(filepath.Join("perf", size.Name, "concurrent-read-source.bin"))
				if _, err := env.fm.UploadFileFromReader(concurrentReadPath, newRepeatingReader(size.Bytes)); err != nil {
					t.Fatalf("prepare concurrent read source: %v", err)
				}
				concurrentReadMeasurement := measureFileIO(t, size, "concurrent_read", concurrency, warmupRuns, sampleRuns, func(_ int) error {
					return concurrentFileReads(env, concurrentReadPath, size.Bytes, concurrency)
				})
				report.Measurements = append(report.Measurements, concurrentReadMeasurement)
			}
		})
	}

	if reportPath := os.Getenv("FSS_PERF_REPORT"); reportPath != "" {
		if err := writeFileIOPerfReport(reportPath, report); err != nil {
			t.Fatalf("write performance report: %v", err)
		}
		t.Logf("performance report written to %s", reportPath)
	}
}

func perfConcurrency(t *testing.T) int {
	t.Helper()
	value := 4
	if raw := os.Getenv("FSS_PERF_CONCURRENCY"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 128 {
			t.Fatalf("FSS_PERF_CONCURRENCY must be between 1 and 128, got %q", raw)
		}
		value = parsed
	}
	return value
}

func perfRunCount(t *testing.T, key string, fallback int, allowZero bool) int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	minimum := 1
	if allowZero {
		minimum = 0
	}
	if err != nil || parsed < minimum || parsed > 100 {
		t.Fatalf("%s must be between %d and 100, got %q", key, minimum, raw)
	}
	return parsed
}

func selectedFileIOPerfSizes(t *testing.T) []fileIOPerfSize {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("FSS_PERF_SIZES"))
	if raw == "" {
		return fileIOPerfSizes
	}
	allowed := make(map[string]fileIOPerfSize, len(fileIOPerfSizes))
	for _, size := range fileIOPerfSizes {
		allowed[size.Name] = size
	}
	var selected []fileIOPerfSize
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(strings.ToUpper(name))
		size, ok := allowed[name]
		if !ok {
			t.Fatalf("unknown FSS_PERF_SIZES value %q; choose from 1K, 1M, 100M, 1G", name)
		}
		selected = append(selected, size)
	}
	if len(selected) == 0 {
		t.Fatal("FSS_PERF_SIZES selected no file sizes")
	}
	return selected
}

func selectedFileIOPerfOperations(t *testing.T) map[string]bool {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("FSS_PERF_OPERATIONS"))
	selected := make(map[string]bool, len(fileIOPerfOperations))
	if raw == "" {
		for _, operation := range fileIOPerfOperations {
			selected[operation] = true
		}
		return selected
	}
	allowed := make(map[string]bool, len(fileIOPerfOperations))
	for _, operation := range fileIOPerfOperations {
		allowed[operation] = true
	}
	for _, operation := range strings.Split(raw, ",") {
		operation = strings.TrimSpace(strings.ToLower(operation))
		if !allowed[operation] {
			t.Fatalf("unknown FSS_PERF_OPERATIONS value %q; choose from %s", operation, strings.Join(fileIOPerfOperations, ", "))
		}
		selected[operation] = true
	}
	return selected
}

func measureFileIO(t *testing.T, size fileIOPerfSize, operation string, concurrency, warmupRuns, sampleRuns int, fn func(run int) error) fileIOPerfMeasurement {
	t.Helper()
	measurement := fileIOPerfMeasurement{
		Size:        size.Name,
		SizeBytes:   size.Bytes,
		Operation:   operation,
		Concurrency: concurrency,
		WarmupRuns:  warmupRuns,
		SampleRuns:  sampleRuns,
	}
	for run := 0; run < warmupRuns; run++ {
		if err := fn(run); err != nil {
			t.Fatalf("%s warmup %d for %s failed: %v", operation, run+1, size.Name, err)
		}
	}
	durations := make([]float64, 0, sampleRuns)
	throughputs := make([]float64, 0, sampleRuns)
	for sampleIndex := 0; sampleIndex < sampleRuns; sampleIndex++ {
		run := warmupRuns + sampleIndex
		start := time.Now()
		err := fn(run)
		duration := time.Since(start)
		if duration <= 0 {
			duration = time.Nanosecond
		}
		transferred := size.Bytes * int64(concurrency)
		throughput := float64(transferred) / duration.Seconds() / (1024 * 1024)
		sample := fileIOPerfSample{
			Run:         sampleIndex + 1,
			DurationMS:  float64(duration.Microseconds()) / 1000,
			MiBPerSec:   throughput,
			Transferred: transferred,
		}
		if err != nil {
			sample.Error = err.Error()
			measurement.ErrorCount++
		}
		measurement.Samples = append(measurement.Samples, sample)
		durations = append(durations, sample.DurationMS)
		throughputs = append(throughputs, sample.MiBPerSec)
		t.Logf("size=%s operation=%s concurrency=%d sample=%d/%d duration=%s throughput=%.2f MiB/s error=%v", size.Name, operation, concurrency, sampleIndex+1, sampleRuns, duration.Round(time.Microsecond), throughput, err)
	}
	measurement.ErrorRate = float64(measurement.ErrorCount) / float64(sampleRuns)
	measurement.P50DurationMS = percentile(durations, 0.50)
	measurement.P95DurationMS = percentile(durations, 0.95)
	measurement.P99DurationMS = percentile(durations, 0.99)
	measurement.P50MiBPerSec = percentile(throughputs, 0.50)
	measurement.P95MiBPerSec = percentile(throughputs, 0.95)
	measurement.P99MiBPerSec = percentile(throughputs, 0.99)
	if measurement.ErrorCount > 0 {
		t.Errorf("%s for %s failed in %d/%d samples", operation, size.Name, measurement.ErrorCount, sampleRuns)
	}
	return measurement
}

func percentile(values []float64, quantile float64) float64 {
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	index := int(math.Ceil(float64(len(ordered))*quantile)) - 1
	return ordered[index]
}

func streamFileRead(env *boundaryEnv, path string, expectedSize int64) error {
	meta, release, err := env.fm.BeginRead(path)
	if err != nil {
		return err
	}
	defer release()
	reader, err := env.storage.OpenReader(meta.Path)
	if err != nil {
		return err
	}
	defer reader.Close()
	read, err := io.CopyBuffer(io.Discard, reader, make([]byte, 256*1024))
	if err != nil {
		return err
	}
	if read != expectedSize {
		return fmt.Errorf("read %d bytes, expected %d", read, expectedSize)
	}
	return nil
}

func concurrentFileWrites(env *boundaryEnv, size fileIOPerfSize, concurrency, run int) error {
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			path := concurrentWritePath(size, run, index)
			if _, err := env.fm.UploadFileFromReader(path, newRepeatingReader(size.Bytes)); err != nil {
				errs <- fmt.Errorf("worker %d: %w", index, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func verifySequentialWriteFiles(t *testing.T, store *storage.LocalStorage, size fileIOPerfSize, warmupRuns, sampleRuns int) {
	t.Helper()
	for run := 0; run < warmupRuns+sampleRuns; run++ {
		path := filepath.ToSlash(filepath.Join("perf", size.Name, fmt.Sprintf("sequential-write-%d.bin", run)))
		verifyFileSamples(t, store, path, size.Bytes)
	}
}

func verifyConcurrentWriteFiles(t *testing.T, store *storage.LocalStorage, size fileIOPerfSize, concurrency, warmupRuns, sampleRuns int) {
	t.Helper()
	for run := 0; run < warmupRuns+sampleRuns; run++ {
		for worker := 0; worker < concurrency; worker++ {
			verifyFileSamples(t, store, concurrentWritePath(size, run, worker), size.Bytes)
		}
	}
}

func concurrentWritePath(size fileIOPerfSize, run, worker int) string {
	return filepath.ToSlash(filepath.Join("perf", size.Name, fmt.Sprintf("concurrent-write-%d-%d.bin", run, worker)))
}

func concurrentFileReads(env *boundaryEnv, path string, expectedSize int64, concurrency int) error {
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if err := streamFileRead(env, path, expectedSize); err != nil {
				errs <- fmt.Errorf("worker %d: %w", index, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func verifyFileSamples(t *testing.T, store *storage.LocalStorage, path string, size int64) {
	t.Helper()
	if err := verifyFileSamplesData(store, path, size); err != nil {
		t.Fatalf("verify %s: %v", path, err)
	}
}

func verifyFileSamplesData(store *storage.LocalStorage, path string, size int64) error {
	sampleSize := int64(64 * 1024)
	if size < sampleSize {
		sampleSize = size
	}
	for _, offset := range []int64{0, size - sampleSize} {
		data, err := store.ReadAt(path, int(sampleSize), offset)
		if err != nil {
			return err
		}
		expected := newRepeatingReaderAt(int64(len(data)), offset)
		want := make([]byte, len(data))
		_, _ = io.ReadFull(expected, want)
		for i := range data {
			if data[i] != want[i] {
				return fmt.Errorf("sample mismatch at offset %d", offset+int64(i))
			}
		}
	}
	return nil
}

func writeFileIOPerfReport(path string, report fileIOPerfReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}
