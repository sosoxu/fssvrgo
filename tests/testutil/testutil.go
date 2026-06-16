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
	cleanup     func()
	server      *httpserver.Server
}

func NewTestServer() (*TestServer, error) {
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

	dbPath := filepath.Join(tempDir, "test.db")
	dbCfg := config.DatabaseConfig{
		Type: "sqlite",
		Path: dbPath,
	}
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(dbCfg); err != nil {
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
		os.RemoveAll(tempDir)
		return nil, err
	}

	store := storage.NewLocalStorage(storageDir)

	fm := filemanager.NewFileManager(store, qdb)
	dirSvc := directory.NewDirectoryManager(qdb)
	flSvc := filelist.NewFileListService(qdb)
	authSvc := auth.NewAuthService()
	authSvc.Init(false, "")
	cryptoSvc := crypto.NewCryptoService()
	cacheSvc := cache.NewCache(300, 1000)
	transferSvc := transfer.NewFileTransferService(store, qdb)

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		dbObj.Close()
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

	srv := httpserver.NewServer(serverCfg, config.TLSConfig{}, fm, dirSvc, flSvc, transferSvc, authSvc, cryptoSvc, store, cacheSvc, nil, qdb)

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
		server:      srv,
		cleanup: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv.Shutdown(ctx)
			dbObj.Close()
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
