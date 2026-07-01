package tests

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	grpcserver "github.com/sosoxu/fssvrgo/internal/api/grpc"
	httpserver "github.com/sosoxu/fssvrgo/internal/api/http"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
	pb "github.com/sosoxu/fssvrgo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ====================================================================================
// Massive small-file performance test.
//   - Database:   PostgreSQL (shared)
//   - Lock:       Redis distributed lock (consistency lock)
//   - Storage:    Local filesystem (centralized) or MinIO object storage,
//                 selected via FSS_BENCH_STORAGE (default "local", or "minio")
//   - Protocols:  HTTP (REST single-shot) and RPC (real gRPC streaming over TCP)
//
// The test writes N x 1KB files then reads them back, for each protocol.
// It is OPT-IN: only runs when env FSS_BENCH_ENABLED=1 (so `go test ./...` and CI
// stay unaffected). Count / concurrency are configurable via env vars.
// ====================================================================================

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envStr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// ---- result structures -----------------------------------------------------------

type latencyStats struct {
	MinUS  int64   `json:"min_us"`
	MaxUS  int64   `json:"max_us"`
	AvgUS  int64   `json:"avg_us"`
	P50US  int64   `json:"p50_us"`
	P95US  int64   `json:"p95_us"`
	P99US  int64   `json:"p99_us"`
	Sample int     `json:"sample"`
}

type phaseResult struct {
	Name         string       `json:"name"`
	Protocol     string       `json:"protocol"`
	Operation    string       `json:"operation"`
	FileCount    int          `json:"file_count"`
	FileSize     int          `json:"file_size"`
	Concurrency  int          `json:"concurrency"`
	DurationSec  float64      `json:"duration_sec"`
	Success      int64        `json:"success"`
	Fail         int64        `json:"fail"`
	FilesPerSec  float64      `json:"files_per_sec"`
	MBPerSec     float64      `json:"mb_per_sec"`
	Latency      latencyStats `json:"latency"`
	Errors       []string     `json:"errors,omitempty"`
}

type benchReport struct {
	GeneratedAt  string         `json:"generated_at"`
	Environment  map[string]any `json:"environment"`
	Config       map[string]any `json:"config"`
	Phases       []phaseResult  `json:"phases"`
	Summary      map[string]any `json:"summary"`
}

// ---- cluster setup ---------------------------------------------------------------

type massiveCluster struct {
	httpBaseURL  string
	grpcAddr     string
	httpServer   *httpserver.Server
	grpcServer   *grpcserver.Server
	fm           *filemanager.FileManager
	transferSvc  *transfer.FileTransferService
	store        storage.StorageAdapter
	dbObj        *database.Database
	qdb          *database.DB
	redisMgr     *distributed.RedisManager
	storageDir   string
	tempDir      string
	httpClient   *http.Client
	grpcConn     *grpc.ClientConn
	grpcClient   pb.FileServiceClient
	storageType  string // "local" or "minio"
	minioBucket  string // populated when storageType == "minio"
}

func setupMassiveCluster(t *testing.T, concurrency int) *massiveCluster {
	t.Helper()
	_ = logger.Initialize("", "error")

	tempDir, err := os.MkdirTemp("", "fsserver-massive-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	storageDir := filepath.Join(tempDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("create storage dir: %v", err)
	}

	// PostgreSQL (shared database)
	dbCfg := config.DatabaseConfig{
		Type:     "postgresql",
		Host:     envStr("FSS_BENCH_PG_HOST", "localhost"),
		Port:     envInt("FSS_BENCH_PG_PORT", 5432),
		Name:     envStr("FSS_BENCH_PG_NAME", "fsserver"),
		User:     envStr("FSS_BENCH_PG_USER", "fsserver"),
		Password: envStr("FSS_BENCH_PG_PASS", "fsserver123"),
		SSLMode:  "disable",
		PoolSize: concurrency + 32,
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		os.RemoveAll(tempDir)
		t.Skipf("PostgreSQL not available: %v", err)
	}
	qdb := dbObj.GetQueryDB()
	resetMassiveDB(qdb)

	mm := database.NewMigrationManager(qdb)
	mm.Register(database.Migration{Version: 1, Name: "initial_schema", Up: func() error { return database.InitTables(qdb) }})
	if err := mm.RunMigrations(); err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("migrations: %v", err)
	}

	// Storage backend: local (centralized) or MinIO (object storage), selected
	// via FSS_BENCH_STORAGE. Local is the default; "minio" connects to a real
	// MinIO server configured by FSS_BENCH_MINIO_* env vars.
	storageType := envStr("FSS_BENCH_STORAGE", "local")
	var store storage.StorageAdapter
	var minioBucket string
	switch storageType {
	case "minio":
		minioCfg := storage.MinIOConfig{
			Endpoint:  envStr("FSS_BENCH_MINIO_ENDPOINT", "localhost:9000"),
			AccessKey: envStr("FSS_BENCH_MINIO_ACCESS_KEY", "minioadmin"),
			SecretKey: envStr("FSS_BENCH_MINIO_SECRET_KEY", "minioadmin"),
			Bucket:    fmt.Sprintf("fssbench-%d", time.Now().UnixNano()),
			UseSSL:    false,
		}
		minioBucket = minioCfg.Bucket
		ms, mErr := storage.NewMinIOStorage(minioCfg)
		if mErr != nil {
			dbObj.Close()
			os.RemoveAll(tempDir)
			t.Skipf("MinIO not available: %v", mErr)
		}
		store = ms
	default:
		storageType = "local"
		store = storage.NewLocalStorage(storageDir)
	}

	// Redis distributed lock + session store (consistency lock)
	redisAddr := envStr("FSS_BENCH_REDIS_ADDR", "localhost:6379")
	redisMgr, err := distributed.NewRedisManager(redisAddr, "", 0, concurrency+32)
	if err != nil {
		dbObj.Close()
		os.RemoveAll(tempDir)
		t.Skipf("Redis not available: %v", err)
	}
	flushRedis(redisMgr)

	distLock := redisMgr.GetLock()
	sessionStore := redisMgr.GetSessionStore()

	fm := filemanager.NewFileManagerWithDistLock(store, qdb, distLock)
	dirSvc := directory.NewDirectoryManager(qdb)
	flSvc := filelist.NewFileListService(qdb)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "") // auth disabled -> gRPC interceptor passes with no API key
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferServiceWithRedis(store, qdb, sessionStore, distLock)

	// HTTP server (tuned transport client). Size the concurrency semaphore
	// (workers*4) so the server's backpressure guard does not reject requests
	// with 503 at the intended offered load — we want to measure true
	// throughput, not the rejection rate.
	httpCfg := config.ServerConfig{
		HTTPPort:           0,
		Workers:            concurrency,
		MaxUploadSizeMB:    2048,
		MaxChunkSizeMB:     128,
		MaxPageSize:        1000,
		CORSAllowedOrigins: "*",
	}
	httpSrv := httpserver.NewServer(httpCfg, config.TLSConfig{}, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, store, cacheSvc, nil, qdb)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		cleanupMassiveCluster(nil, dbObj, redisMgr, tempDir)
		t.Fatalf("http listen: %v", err)
	}
	httpPort := ln.Addr().(*net.TCPAddr).Port
	go httpSrv.Serve(ln)
	httpBaseURL := fmt.Sprintf("http://localhost:%d", httpPort)
	waitForHTTP(httpBaseURL)

	transport := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
		// small files: avoid unnecessary buffering
		DisableCompression: true,
	}
	httpClient := &http.Client{Transport: transport, Timeout: 60 * time.Second}

	// gRPC server (real RPC over TCP). Pick a free port, then Start().
	grpcPort := freePort(t)
	grpcCfg := config.ServerConfig{GRPCPort: grpcPort}
	grpcSrv := grpcserver.NewServer(grpcCfg, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, nil)
	go func() { _ = grpcSrv.Start() }()
	grpcAddr := fmt.Sprintf("localhost:%d", grpcPort)
	waitForGRPC(grpcAddr)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cleanupMassiveCluster(httpSrv, dbObj, redisMgr, tempDir)
		t.Fatalf("grpc dial: %v", err)
	}
	grpcClient := pb.NewFileServiceClient(conn)

	return &massiveCluster{
		httpBaseURL: httpBaseURL,
		grpcAddr:    grpcAddr,
		httpServer:  httpSrv,
		grpcServer:  grpcSrv,
		fm:          fm,
		transferSvc: transferSvc,
		store:       store,
		dbObj:       dbObj,
		qdb:         qdb,
		redisMgr:    redisMgr,
		storageDir:  storageDir,
		tempDir:     tempDir,
		httpClient:  httpClient,
		grpcConn:    conn,
		grpcClient:  grpcClient,
		storageType: storageType,
		minioBucket: minioBucket,
	}
}

func cleanupMassiveCluster(httpSrv *httpserver.Server, dbObj *database.Database, redisMgr *distributed.RedisManager, tempDir string) {
	if httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		httpSrv.Shutdown(ctx)
		cancel()
	}
	if redisMgr != nil {
		flushRedis(redisMgr)
		redisMgr.Close()
	}
	if dbObj != nil {
		resetMassiveDB(dbObj.GetQueryDB())
		dbObj.Close()
	}
	if tempDir != "" {
		os.RemoveAll(tempDir)
	}
}

func (c *massiveCluster) shutdown() {
	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}
	cleanupMassiveCluster(c.httpServer, c.dbObj, c.redisMgr, c.tempDir)
}

func (c *massiveCluster) cleanState() {
	resetMassiveDB(c.qdb)
	flushRedis(c.redisMgr)
	if c.storageType == "minio" {
		// MinIO: empty the bucket so each phase starts clean. CleanPathLocks
		// is a no-op for MinIO (objects removed via API), but call it for
		// parity with local cleanup of the in-memory path-lock map.
		if ms, ok := c.store.(*storage.MinIOStorage); ok {
			ms.CleanPathLocks()
		}
	} else {
		removeDirContents(c.storageDir)
		if ls, ok := c.store.(*storage.LocalStorage); ok {
			ls.CleanPathLocks()
		}
	}
}

func resetMassiveDB(qdb *database.DB) {
	if qdb == nil {
		return
	}
	_, _ = qdb.Exec("DELETE FROM transfer_tasks")
	_, _ = qdb.Exec("DELETE FROM audit_log")
	_, _ = qdb.Exec("DELETE FROM api_keys")
	_, _ = qdb.Exec("DELETE FROM files")
	_, _ = qdb.Exec("DELETE FROM directories")
	_, _ = qdb.Exec("DELETE FROM schema_migrations")
}

func flushRedis(m *distributed.RedisManager) {
	if m == nil {
		return
	}
	ctx := context.Background()
	c := m.GetClient()
	keys, _ := c.Keys(ctx, "fsserver:*").Result()
	if len(keys) > 0 {
		c.Del(ctx, keys...)
	}
}

func removeDirContents(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		os.RemoveAll(filepath.Join(dir, e.Name()))
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func waitForHTTP(base string) {
	for i := 0; i < 200; i++ {
		resp, err := http.Get(base + "/api/v1/health")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForGRPC(addr string) {
	for i := 0; i < 200; i++ {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// ---- path helpers ----------------------------------------------------------------

// shardPath produces a sharded path so no single directory holds 500K entries.
// e.g. shardPath("/bench/http", 1234) -> "/bench/http/001/001234"
func shardPath(prefix string, idx int) string {
	return fmt.Sprintf("%s/%03d/%06d", prefix, idx/1000, idx)
}

// ---- statistics ------------------------------------------------------------------

func computeLatency(lat []int64) latencyStats {
	s := latencyStats{Sample: len(lat)}
	if len(lat) == 0 {
		return s
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	var sum int64
	for _, v := range lat {
		sum += v
		if v < s.MinUS || s.MinUS == 0 {
			s.MinUS = v
		}
		if v > s.MaxUS {
			s.MaxUS = v
		}
	}
	s.AvgUS = sum / int64(len(lat))
	pct := func(p float64) int64 {
		idx := int(float64(len(lat)-1) * p)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(lat) {
			idx = len(lat) - 1
		}
		return lat[idx]
	}
	s.P50US = pct(0.50)
	s.P95US = pct(0.95)
	s.P99US = pct(0.99)
	return s
}

type errorRecorder struct {
	mu     sync.Mutex
	first  []string
	sample map[string]int64
}

func newErrorRecorder() *errorRecorder { return &errorRecorder{sample: map[string]int64{}} }

func (e *errorRecorder) record(err string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sample[err]++
	if len(e.first) < 10 {
		e.first = append(e.first, err)
	}
}

func (e *errorRecorder) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.first))
	out = append(out, e.first...)
	return out
}

// progressLogger periodically logs throughput while a phase is running.
type progressLogger struct {
	name     string
	total    int
	done     *atomic.Int64
	stopCh   chan struct{}
}

func startProgress(name string, total int, done *atomic.Int64) *progressLogger {
	p := &progressLogger{name: name, total: total, done: done, stopCh: make(chan struct{})}
	start := time.Now()
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-t.C:
				d := done.Load()
				elapsed := time.Since(start).Seconds()
				if elapsed > 0 {
					rate := float64(d) / elapsed
					fmt.Printf("[progress] %-16s %d/%d (%.1f%%) rate=%.0f files/s elapsed=%.1fs\n",
						name, d, total, float64(d)/float64(total)*100, rate, elapsed)
				}
			}
		}
	}()
	return p
}

func (p *progressLogger) stop() { close(p.stopCh) }

// ---- HTTP operations -------------------------------------------------------------

func httpUploadFile(client *http.Client, baseURL, path string, data []byte) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return err
	}
	part.Write(data)
	w.WriteField("path", path)
	w.Close()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/files", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("http upload status %d", resp.StatusCode)
	}
	return nil
}

func httpDownloadFileOK(client *http.Client, baseURL, path string) (int, error) {
	resp, err := client.Get(baseURL + "/api/v1/files" + path)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	n, _ := io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return int(n), fmt.Errorf("http download status %d", resp.StatusCode)
	}
	return int(n), nil
}

// ---- gRPC operations -------------------------------------------------------------

func grpcUploadFile(ctx context.Context, client pb.FileServiceClient, path, name, hash string, data []byte) error {
	stream, err := client.UploadFile(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&pb.UploadRequest{
		Data: &pb.UploadRequest_Metadata{
			Metadata: &pb.UploadMetadata{
				Path: path, Name: name, TotalSize: int64(len(data)), Hash: hash,
			},
		},
	}); err != nil {
		return err
	}
	if err := stream.Send(&pb.UploadRequest{
		Data: &pb.UploadRequest_Chunk{
			Chunk: &pb.UploadChunk{Data: data, Offset: 0},
		},
	}); err != nil {
		return err
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return err
	}
	return nil
}

func grpcDownloadFile(ctx context.Context, client pb.FileServiceClient, path string) (int, error) {
	stream, err := client.DownloadFile(ctx, &pb.DownloadRequest{Path: path, ChunkSize: 4096})
	if err != nil {
		return 0, err
	}
	total := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
		total += len(msg.GetData())
	}
	return total, nil
}

// ---- phase runners ---------------------------------------------------------------

func runHTTPWritePhase(t *testing.T, c *massiveCluster, count, concurrency int, data []byte, prefix string) phaseResult {
	return runPhase(t, "HTTP Write", "HTTP", "write", count, concurrency, len(data), func(idx int) error {
		path := shardPath(prefix, idx)
		return httpUploadFile(c.httpClient, c.httpBaseURL, path, data)
	})
}

func runHTTPReadPhase(t *testing.T, c *massiveCluster, count, concurrency int, size int, prefix string) phaseResult {
	return runPhase(t, "HTTP Read", "HTTP", "read", count, concurrency, size, func(idx int) error {
		path := shardPath(prefix, idx)
		n, err := httpDownloadFileOK(c.httpClient, c.httpBaseURL, path)
		if err != nil {
			return err
		}
		if n != size {
			return fmt.Errorf("size mismatch: got %d want %d", n, size)
		}
		return nil
	})
}

func runGRPCWritePhase(t *testing.T, c *massiveCluster, count, concurrency int, data []byte, hash, prefix string) phaseResult {
	return runPhase(t, "gRPC Write", "gRPC", "write", count, concurrency, len(data), func(idx int) error {
		path := shardPath(prefix, idx)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return grpcUploadFile(ctx, c.grpcClient, path, filepath.Base(path), hash, data)
	})
}

func runGRPCReadPhase(t *testing.T, c *massiveCluster, count, concurrency int, size int, prefix string) phaseResult {
	return runPhase(t, "gRPC Read", "gRPC", "read", count, concurrency, size, func(idx int) error {
		path := shardPath(prefix, idx)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := grpcDownloadFile(ctx, c.grpcClient, path)
		if err != nil {
			return err
		}
		if n != size {
			return fmt.Errorf("size mismatch: got %d want %d", n, size)
		}
		return nil
	})
}

// runPhase executes `count` operations across `concurrency` workers, recording
// per-op latency for successful operations.
func runPhase(t *testing.T, name, protocol, operation string, count, concurrency, size int, op func(idx int) error) phaseResult {
	t.Helper()
	t.Logf(">>> START %-10s [%s] files=%d size=%dB concurrency=%d", name, protocol, count, size, concurrency)
	if concurrency > count {
		concurrency = count
	}

	latencies := make([]int64, count)
	var latIdx int64
	var success, fail atomic.Int64
	rec := newErrorRecorder()
	var done atomic.Int64

	jobs := make(chan int, concurrency*2)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			for idx := range jobs {
				start := time.Now()
				err := op(idx)
				dur := time.Since(start)
				if err != nil {
					fail.Add(1)
					rec.record(err.Error())
				} else {
					success.Add(1)
					i := atomic.AddInt64(&latIdx, 1) - 1
					if int(i) < count {
						latencies[i] = dur.Microseconds()
					}
				}
				done.Add(1)
			}
		}()
	}

	p := startProgress(name, count, &done)
	start := time.Now()
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	p.stop()

	validLat := latencies[:int(latIdx)]
	res := phaseResult{
		Name:        name,
		Protocol:    protocol,
		Operation:   operation,
		FileCount:   count,
		FileSize:    size,
		Concurrency: concurrency,
		DurationSec: elapsed.Seconds(),
		Success:     success.Load(),
		Fail:        fail.Load(),
		Latency:     computeLatency(validLat),
		Errors:      rec.snapshot(),
	}
	if res.DurationSec > 0 {
		res.FilesPerSec = float64(res.Success) / res.DurationSec
		res.MBPerSec = float64(res.Success) * float64(size) / 1024 / 1024 / res.DurationSec
	}
	t.Logf("<<< DONE  %-10s [%s] dur=%s success=%d fail=%d rate=%.0f files/s %.2f MB/s lat(p50/p95/p99)=%d/%d/%dus",
		name, protocol, elapsed.Round(time.Millisecond), res.Success, res.Fail, res.FilesPerSec, res.MBPerSec,
		res.Latency.P50US, res.Latency.P95US, res.Latency.P99US)
	return res
}

// ---- main test -------------------------------------------------------------------

func TestMassiveSmallFiles_Performance(t *testing.T) {
	if os.Getenv("FSS_BENCH_ENABLED") != "1" {
		t.Skip("skipped: set FSS_BENCH_ENABLED=1 to run the 500K small-file performance test")
	}

	fileCount := envInt("FSS_BENCH_COUNT", 500000)
	fileSize := envInt("FSS_BENCH_FILE_SIZE", 1024) // 1KB
	httpConcurrency := envInt("FSS_BENCH_HTTP_CONCURRENCY", 100)
	grpcConcurrency := envInt("FSS_BENCH_GRPC_CONCURRENCY", 100)

	// single 1KB payload reused for every file (content is irrelevant to perf)
	data := make([]byte, fileSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand read: %v", err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	t.Logf("=== Massive small-file performance test ===")
	t.Logf("files=%d size=%dB (%s) storage=%s db=postgresql lock=redis",
		fileCount, fileSize, formatSize(int64(fileSize)), envStr("FSS_BENCH_STORAGE", "local"))
	t.Logf("http_concurrency=%d grpc_concurrency=%d", httpConcurrency, grpcConcurrency)
	t.Logf("go version=%s GOMAXPROCS=%d", runtime.Version(), runtime.GOMAXPROCS(0))

	c := setupMassiveCluster(t, max(httpConcurrency, grpcConcurrency))
	defer c.shutdown()

	var phases []phaseResult

	// ---- HTTP: write then read ----
	c.cleanState()
	phases = append(phases, runHTTPWritePhase(t, c, fileCount, httpConcurrency, data, "/bench/http"))
	phases = append(phases, runHTTPReadPhase(t, c, fileCount, httpConcurrency, fileSize, "/bench/http"))

	// ---- gRPC (RPC): write then read ----
	c.cleanState()
	phases = append(phases, runGRPCWritePhase(t, c, fileCount, grpcConcurrency, data, hash, "/bench/grpc"))
	phases = append(phases, runGRPCReadPhase(t, c, fileCount, grpcConcurrency, fileSize, "/bench/grpc"))

	// ---- build & emit report ----
	report := benchReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Environment: map[string]any{
			"go_version":  runtime.Version(),
			"gomaxprocs":  runtime.GOMAXPROCS(0),
			"os":          runtime.GOOS + "/" + runtime.GOARCH,
			"storage":     envStr("FSS_BENCH_STORAGE", "local"),
			"database":    "postgresql",
			"lock":        "redis (distributed SET NX + Lua unlock)",
		},
		Config: map[string]any{
			"file_count":        fileCount,
			"file_size":         fileSize,
			"http_concurrency":  httpConcurrency,
			"grpc_concurrency":  grpcConcurrency,
		},
		Phases: phases,
	}

	// summary: write throughput, read throughput, http vs grpc
	httpWrite := findPhase(phases, "HTTP", "write")
	httpRead := findPhase(phases, "HTTP", "read")
	grpcWrite := findPhase(phases, "gRPC", "write")
	grpcRead := findPhase(phases, "gRPC", "read")
	report.Summary = map[string]any{
		"http_write_files_per_sec": httpWrite.FilesPerSec,
		"http_read_files_per_sec":  httpRead.FilesPerSec,
		"grpc_write_files_per_sec": grpcWrite.FilesPerSec,
		"grpc_read_files_per_sec":  grpcRead.FilesPerSec,
		"total_files_written":      fileCount * 2,
		"total_files_read":         fileCount * 2,
		"total_errors":             httpWrite.Fail + httpRead.Fail + grpcWrite.Fail + grpcRead.Fail,
	}

	// write JSON for downstream report generation
	outPath := envStr("FSS_BENCH_OUT", filepath.Join(getRepoRoot(), "massive_small_files_results.json"))
	if raw, err := json.MarshalIndent(report, "", "  "); err == nil {
		if err := os.WriteFile(outPath, raw, 0644); err == nil {
			t.Logf("wrote JSON results to %s", outPath)
		} else {
			t.Logf("warn: write json: %v", err)
		}
	}

	// human-readable table
	t.Log("\n" + renderPhaseTable(phases))
	t.Log(renderComparison(httpWrite, grpcWrite, "Write"))
	t.Log(renderComparison(httpRead, grpcRead, "Read"))

	// fail fast the test if error rate is unacceptable
	totalFail := httpWrite.Fail + httpRead.Fail + grpcWrite.Fail + grpcRead.Fail
	if totalFail > 0 {
		t.Logf("NOTE: %d total failures across phases (see errors per phase)", totalFail)
	}
}

func findPhase(phases []phaseResult, proto, op string) phaseResult {
	for _, p := range phases {
		if p.Protocol == proto && p.Operation == op {
			return p
		}
	}
	return phaseResult{}
}

func getRepoRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

func renderPhaseTable(phases []phaseResult) string {
	var b strings.Builder
	b.WriteString(strings.Repeat("=", 132) + "\n")
	b.WriteString(fmt.Sprintf("%-12s %-6s %-6s %-10s %-12s %-10s %-14s %-18s %s\n",
		"Phase", "Proto", "Op", "Files", "Duration", "Errors", "files/s", "MB/s", "Lat p50/p95/p99 (us)"))
	b.WriteString(strings.Repeat("-", 132) + "\n")
	for _, p := range phases {
		b.WriteString(fmt.Sprintf("%-12s %-6s %-6s %-10d %-12s %-10d %-14.1f %-18.2f %d/%d/%d\n",
			p.Name, p.Protocol, p.Operation, p.FileCount,
			fmt.Sprintf("%.2fs", p.DurationSec), p.Fail, p.FilesPerSec, p.MBPerSec,
			p.Latency.P50US, p.Latency.P95US, p.Latency.P99US))
	}
	b.WriteString(strings.Repeat("=", 132))
	return b.String()
}

func renderComparison(a, b phaseResult, op string) string {
	if a.FilesPerSec == 0 || b.FilesPerSec == 0 {
		return fmt.Sprintf("--- %s: missing data (a=%.1f b=%.1f) ---", op, a.FilesPerSec, b.FilesPerSec)
	}
	faster := "HTTP"
	ratio := a.FilesPerSec / b.FilesPerSec
	if b.FilesPerSec > a.FilesPerSec {
		faster = "gRPC"
		ratio = b.FilesPerSec / a.FilesPerSec
	}
	return fmt.Sprintf("--- %s: HTTP=%.1f files/s, gRPC=%.1f files/s -> %s %.2fx faster ---",
		op, a.FilesPerSec, b.FilesPerSec, faster, ratio)
}
