package testutil

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	httpserver "github.com/sosoxu/fssvrgo/internal/api/http"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/cache"
	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/pgtest"
	"github.com/sosoxu/fssvrgo/internal/service/apikey"
	"github.com/sosoxu/fssvrgo/internal/service/auditlog"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type TestServer struct {
	BaseURL     string
	DB          *database.DB
	Store       storage.StorageAdapter
	FM          *filemanager.FileManager
	DirSvc      *directory.DirectoryManager
	FlSvc       *filelist.FileListService
	TransferSvc *transfer.FileTransferService
	CryptoSvc   *crypto.CryptoService
	cleanup     func()
	server      *httpserver.Server
}

func NewTestServer() (*TestServer, error) {
	return newTestServer(false)
}

// NewTestServerWithCrypto is NewTestServer with the crypto service enabled
// under a freshly generated key, for the encrypted upload/download paths.
func NewTestServerWithCrypto() (*TestServer, error) {
	return newTestServer(true)
}

func newTestServer(enableCrypto bool) (*TestServer, error) {
	tempDir, err := os.MkdirTemp("", "fsserver-test-*")
	if err != nil {
		return nil, err
	}

	storageDir := filepath.Join(tempDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		os.RemoveAll(tempDir)
		return nil, err
	}

	_ = logger.Initialize("", "error")

	// Each test server runs in its own PostgreSQL schema so integration tests
	// stay isolated from one another.
	dbCfg := pgtest.Config()
	schema := pgtest.NewSchemaName()
	if err := pgtest.Create(dbCfg, schema); err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("create test schema: %w", err)
	}
	dropSchema := func() { _ = pgtest.Drop(pgtest.Config(), schema) }
	dbCfg.SearchPath = schema

	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
		dropSchema()
		os.RemoveAll(tempDir)
		return nil, err
	}

	qdb := dbObj.GetQueryDB()

	migrationMgr := database.NewMigrationManager(qdb)
	migrationMgr.Register(database.Migration{
		Version: 1,
		Name:    "initial_schema",
		Up: func() error {
			return database.InitTables(qdb)
		},
	})
	if err := migrationMgr.RunMigrations(); err != nil {
		dbObj.Close()
		dropSchema()
		os.RemoveAll(tempDir)
		return nil, err
	}

	store := storage.NewLocalStorage(storageDir)

	fm := filemanager.NewFileManager(store, qdb)
	dirSvc := directory.NewDirectoryManager(qdb)
	flSvc := filelist.NewFileListServiceFromDB(qdb)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	if enableCrypto {
		if err := cryptoSvc.Init(cryptoSvc.GenerateKey()); err != nil {
			dbObj.Close()
			dropSchema()
			os.RemoveAll(tempDir)
			return nil, fmt.Errorf("enable crypto: %w", err)
		}
	}
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferService(store, qdb)
	// Mirror main.go: the transfer service encrypts on completion and decrypts
	// download sessions only when it holds the crypto service.
	transferSvc.SetCryptoService(cryptoSvc)

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		dbObj.Close()
		dropSchema()
		os.RemoveAll(tempDir)
		return nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	serverCfg := config.ServerConfig{
		HTTPPort:           port,
		MaxUploadSizeMB:    1024,
		MaxChunkSizeMB:     64,
		MaxPageSize:        1000,
		CORSAllowedOrigins: "*",
	}

	srv := httpserver.NewServer(httpserver.Deps{
		Config:      serverCfg,
		Files:       fm,
		Directories: dirSvc,
		Lists:       flSvc,
		Transfers:   transferSvc,
		Auth:        authSvc,
		Crypto:      cryptoSvc,
		Storage:     store,
		Cache:       cacheSvc,
		Audit: auditlog.NewService(
			database.NewAuditLogService(qdb),
			database.NewAuditWriter(qdb, 100, time.Second),
		),
		ApiKeys: apikey.NewService(database.NewApiKeyService(qdb), authSvc.GenerateApiKey),
		DB:      qdb,
	})

	go srv.ListenAndServe()

	baseURL := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(baseURL + "/api/v1/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	ts := &TestServer{
		BaseURL:     baseURL,
		DB:          qdb,
		Store:       store,
		FM:          fm,
		DirSvc:      dirSvc,
		FlSvc:       flSvc,
		TransferSvc: transferSvc,
		CryptoSvc:   cryptoSvc,
		server:      srv,
		cleanup: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv.Shutdown(ctx)
			dbObj.Close()
			dropSchema()
			os.RemoveAll(tempDir)
		},
	}

	return ts, nil
}

func (ts *TestServer) Cleanup() {
	if ts.cleanup != nil {
		ts.cleanup()
		ts.cleanup = nil
	}
}
