package transfer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type UploadSession struct {
	SessionID              string
	FileID                 string
	FilePath               string
	FileName               string
	TotalSize              int64
	UploadedSize           int64
	Hash                   string
	ClientID               string
	Status                 string
	CreatedAt              string
	UpdatedAt              string
	CompletionFileHash     string
	CompletionCreatedAt    string
	CompletionHashProvided bool
	CompletionHashVerified bool
	chunkCount             int64
	tempFile               *os.File
	tempFileMu             sync.Mutex
	hashWriter             hash.Hash
	hashMu                 sync.Mutex
	lastOffset             int64
	hashValid              int32
	closed                 int32
}

type DownloadSession struct {
	SessionID           string
	FileID              string
	FilePath            string
	TotalSize           int64
	DownloadedSize      int64
	ClientID            string
	Status              string
	CreatedAt           string
	UpdatedAt           string
	NamespaceLeaseToken string
	Encrypted           bool
	chunkCount          int64
	decryptedTempPath   string
	decryptedFile       *os.File
	namespaceMu         sync.Mutex
	namespaceLease      *database.NamespaceLease
	readMu              sync.RWMutex
	progressMu          sync.Mutex
	prepareMu           sync.Mutex
}

type FileTransferService struct {
	storage           storage.StorageAdapter
	db                *database.DB
	uploadSessions    sync.Map
	downloadSessions  sync.Map
	multipartSessions sync.Map
	cleanupRunning    int32
	cleanupCancel     context.CancelFunc
	mu                sync.Mutex
	tempDir           string
	sessionStore      distributed.SessionStore
	distLock          distributed.DistributedLock
	cryptoSvc         *crypto.CryptoService
	maxSessions       int   // maximum concurrent upload sessions (DoS protection)
	sessionCount      int64 // current number of active upload sessions (atomic)
}

func commitStorageBackup(backup storage.ReplaceBackup, path string) {
	if backup != nil {
		if err := backup.Commit(); err != nil {
			logger.Warn("failed to remove replacement backup for %s: %v", path, err)
		}
	}
}

func rollbackStorageBackup(backup storage.ReplaceBackup, path string) {
	if backup != nil {
		if err := backup.Rollback(); err != nil {
			logger.Error("failed to restore replacement backup for %s: %v", path, err)
		}
	}
}

func rollbackStorageWrite(store storage.StorageAdapter, backup storage.ReplaceBackup, path string) {
	if backup != nil {
		rollbackStorageBackup(backup, path)
		return
	}
	if err := store.Remove(path); err != nil {
		logger.Error("failed to remove uncommitted storage object %s: %v", path, err)
	}
}

func transferResultExpiry() string {
	return utils.FormatTimestamp(time.Now().UTC().Add(24 * time.Hour))
}

func completeUploadResultFromLedger(result *database.TransferSessionResult) *CompleteUploadResult {
	return &CompleteUploadResult{
		FileID: result.FileID, FilePath: result.FilePath, FileName: result.FileName,
		FileHash: result.FileHash, CreatedAt: result.FileCreatedAt,
		HashProvided: result.HashProvided, HashVerified: result.HashVerified,
		UploadedSize: result.UploadedSize, StorageType: result.StorageType,
	}
}

func (s *FileTransferService) getTransferResult(sessionID string) (*database.TransferSessionResult, error) {
	return database.NewTransferSessionResultService(s.db).Get(sessionID)
}

func (s *FileTransferService) acquireFileNamespace(path string) (*database.NamespaceLease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := s.db.AcquireNamespaceLease(ctx, database.FileNamespaceRequests(path), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire upload namespace lease: %w", err)
	}
	return lease, nil
}

func (s *FileTransferService) resumeFileNamespace(path, token string) (*database.NamespaceLease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := s.db.ResumeNamespaceLease(ctx, token, database.FileNamespaceRequests(path), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to resume download namespace lease: %w", err)
	}
	return lease, nil
}

func (s *FileTransferService) acquireDownloadOperation(sessionID string, mode database.NamespaceLockMode) (*database.NamespaceLease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := s.db.AcquireNamespaceLease(ctx, database.DownloadSessionNamespaceRequests(sessionID, mode), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire download session lease: %w", err)
	}
	return lease, nil
}

func releaseNamespaceLease(lease *database.NamespaceLease) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		logger.Warn("failed to release namespace lease: %v", err)
	}
}

func NewFileTransferService(storageAdapter storage.StorageAdapter, db *database.DB) *FileTransferService {
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fsserver-uploads-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		logger.Error("failed to create temp directory: %v", err)
	}

	return &FileTransferService{
		storage:      storageAdapter,
		db:           db,
		tempDir:      tempDir,
		sessionStore: distributed.NewMemorySessionStore(),
		distLock:     distributed.NewLocalDistributedLock(),
		maxSessions:  1000,
	}
}

func NewFileTransferServiceWithRedis(storageAdapter storage.StorageAdapter, db *database.DB, sessionStore distributed.SessionStore, distLock distributed.DistributedLock) *FileTransferService {
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fsserver-uploads-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		logger.Error("failed to create temp directory: %v", err)
	}

	return &FileTransferService{
		storage:      storageAdapter,
		db:           db,
		tempDir:      tempDir,
		sessionStore: sessionStore,
		distLock:     distLock,
		maxSessions:  1000,
	}
}

// acquireSessionSlot atomically increments the session counter and returns
// false if the maximum concurrent session limit has been reached.
func (s *FileTransferService) acquireSessionSlot() bool {
	count := atomic.AddInt64(&s.sessionCount, 1)
	if count > int64(s.maxSessions) {
		atomic.AddInt64(&s.sessionCount, -1)
		return false
	}
	return true
}

// releaseSessionSlot decrements the session counter when a session ends.
func (s *FileTransferService) releaseSessionSlot() {
	atomic.AddInt64(&s.sessionCount, -1)
}

func (s *FileTransferService) SetCryptoService(cryptoSvc *crypto.CryptoService) {
	s.cryptoSvc = cryptoSvc
}

// SetTempDir overrides the directory used to store upload temp files
// (<tempDir>/<sessionID>.tmp). By default each instance uses a private
// os.TempDir()/fsserver-uploads-<nanos> directory, which means upload bytes
// are bound to the instance that created the session and cannot be resumed on
// another instance.
//
// For multi-instance deployments that need cross-instance upload resume, set
// this to a shared volume (e.g. an NFS mount) mounted at the same path on
// every instance. Every instance must use the same path so that a session
// created on one instance can find its temp file on another. This must be
// called before any upload sessions are created (i.e. at startup).
func (s *FileTransferService) SetTempDir(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create configured temp directory %s: %w", dir, err)
	}
	if err := storage.RegisterTrustedTempDir(dir); err != nil {
		return err
	}
	s.tempDir = dir
	return nil
}

// TempDir returns the directory currently used for upload temp files. Exposed
// for diagnostics and tests.
func (s *FileTransferService) TempDir() string {
	return s.tempDir
}

func (s *FileTransferService) loadUploadSession(ctx context.Context, sessionID string, refresh bool) (*UploadSession, error) {
	if !refresh {
		if val, ok := s.uploadSessions.Load(sessionID); ok {
			return val.(*UploadSession), nil
		}
	}

	var stored UploadSession
	if err := s.sessionStore.Get(ctx, "upload", sessionID, &stored); err != nil {
		return nil, fmt.Errorf("upload session not found: %s", sessionID)
	}
	if val, ok := s.uploadSessions.Load(sessionID); ok {
		session := val.(*UploadSession)
		session.FileID = stored.FileID
		session.FilePath = stored.FilePath
		session.FileName = stored.FileName
		session.TotalSize = stored.TotalSize
		atomic.StoreInt64(&session.UploadedSize, stored.UploadedSize)
		session.Hash = stored.Hash
		session.ClientID = stored.ClientID
		session.Status = stored.Status
		session.CreatedAt = stored.CreatedAt
		session.UpdatedAt = stored.UpdatedAt
		session.CompletionFileHash = stored.CompletionFileHash
		session.CompletionCreatedAt = stored.CompletionCreatedAt
		session.CompletionHashProvided = stored.CompletionHashProvided
		session.CompletionHashVerified = stored.CompletionHashVerified
		return session, nil
	}
	if stored.Status != "active" {
		actual, _ := s.uploadSessions.LoadOrStore(sessionID, &stored)
		return actual.(*UploadSession), nil
	}

	if !s.acquireSessionSlot() {
		return nil, fmt.Errorf("maximum number of concurrent upload sessions reached")
	}
	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	file, err := os.OpenFile(tempPath, os.O_RDWR, 0644)
	if err != nil {
		s.releaseSessionSlot()
		return nil, fmt.Errorf("failed to reopen upload temp file: %w", err)
	}
	stored.tempFile = file
	stored.hashValid = 0
	stored.hashWriter = nil
	stored.lastOffset = stored.UploadedSize
	actual, loaded := s.uploadSessions.LoadOrStore(sessionID, &stored)
	if loaded {
		file.Close()
		s.releaseSessionSlot()
		return actual.(*UploadSession), nil
	}
	return &stored, nil
}

func (s *FileTransferService) CreateUploadSession(filePath, fileName string, totalSize int64, clientID, hash string) (string, error) {
	if !s.acquireSessionSlot() {
		return "", fmt.Errorf("maximum number of concurrent upload sessions reached")
	}
	filePath = utils.NormalizePath(filePath)
	sessionID := utils.GenerateUUID()
	now := utils.GetCurrentTimestamp()

	session := &UploadSession{
		SessionID: sessionID,
		FilePath:  filePath,
		FileName:  fileName,
		TotalSize: totalSize,
		Hash:      hash,
		ClientID:  clientID,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	file, err := os.Create(tempPath)
	if err != nil {
		s.releaseSessionSlot()
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	if totalSize > 0 {
		if err := file.Truncate(totalSize); err != nil {
			file.Close()
			os.Remove(tempPath)
			s.releaseSessionSlot()
			return "", fmt.Errorf("failed to pre-allocate temp file: %w", err)
		}
	}

	session.tempFile = file

	session.hashWriter = sha256.New()
	session.hashValid = 1
	session.lastOffset = 0

	s.uploadSessions.Store(sessionID, session)

	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "upload", sessionID, session, 2*time.Hour); err != nil {
		s.uploadSessions.Delete(sessionID)
		file.Close()
		os.Remove(tempPath)
		s.releaseSessionSlot()
		return "", fmt.Errorf("failed to persist upload session: %w", err)
	}

	return sessionID, nil
}

func (s *FileTransferService) UploadChunk(sessionID string, data []byte, offset int64) error {
	lease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "upload:"+sessionID, 10*time.Second, 10, 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire session lock: %w", err)
	}
	defer lease.Stop()
	defer s.distLock.Unlock(context.Background(), "upload:"+sessionID, lease.Token)
	terminal, err := s.getTransferResult(sessionID)
	if err != nil {
		return fmt.Errorf("failed to check upload terminal state: %w", err)
	}
	if terminal != nil {
		return fmt.Errorf("upload session is %s: %s", terminal.Status, sessionID)
	}

	session, err := s.loadUploadSession(context.Background(), sessionID, true)
	if err != nil {
		return err
	}

	if atomic.LoadInt32(&session.closed) == 1 {
		return fmt.Errorf("upload session is closed: %s", sessionID)
	}
	if session.Status != "active" {
		return fmt.Errorf("upload session is not active: %s", sessionID)
	}

	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}
	if offset+int64(len(data)) > session.TotalSize {
		return fmt.Errorf("write beyond file size: offset=%d len=%d total=%d", offset, len(data), session.TotalSize)
	}
	expectedOffset := atomic.LoadInt64(&session.UploadedSize)
	if offset != expectedOffset {
		return fmt.Errorf("non-sequential upload offset: expected %d, got %d", expectedOffset, offset)
	}

	session.tempFileMu.Lock()
	if session.tempFile != nil {
		if _, err := session.tempFile.WriteAt(data, offset); err != nil {
			session.tempFileMu.Unlock()
			return fmt.Errorf("failed to write chunk: %w", err)
		}
		if err := session.tempFile.Sync(); err != nil {
			session.tempFileMu.Unlock()
			return fmt.Errorf("failed to sync chunk: %w", err)
		}
	} else {
		tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
		file, err := os.OpenFile(tempPath, os.O_WRONLY, 0644)
		if err != nil {
			session.tempFileMu.Unlock()
			return fmt.Errorf("failed to open temp file: %w", err)
		}

		if _, err := file.WriteAt(data, offset); err != nil {
			file.Close()
			session.tempFileMu.Unlock()
			return fmt.Errorf("failed to write chunk: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			session.tempFileMu.Unlock()
			return fmt.Errorf("failed to sync chunk: %w", err)
		}
		file.Close()
	}
	session.tempFileMu.Unlock()
	if err := lease.Err(); err != nil {
		return err
	}

	if session.hashWriter != nil && atomic.LoadInt32(&session.hashValid) == 1 {
		expectedOffset := atomic.LoadInt64(&session.lastOffset)
		if offset == expectedOffset {
			session.hashMu.Lock()
			session.hashWriter.Write(data)
			session.hashMu.Unlock()
			atomic.StoreInt64(&session.lastOffset, offset+int64(len(data)))
		} else {
			atomic.StoreInt32(&session.hashValid, 0)
		}
	}

	previousSize := atomic.LoadInt64(&session.UploadedSize)
	previousChunks := atomic.LoadInt64(&session.chunkCount)
	atomic.StoreInt64(&session.UploadedSize, previousSize+int64(len(data)))
	atomic.StoreInt64(&session.chunkCount, previousChunks+1)
	session.UpdatedAt = utils.GetCurrentTimestamp()
	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "upload", sessionID, session, 2*time.Hour); err != nil {
		// The bytes can be safely overwritten by a retry at the same offset. Roll
		// back the acknowledged progress and force a full hash at completion.
		atomic.StoreInt64(&session.UploadedSize, previousSize)
		atomic.StoreInt64(&session.chunkCount, previousChunks)
		atomic.StoreInt32(&session.hashValid, 0)
		return fmt.Errorf("failed to persist upload progress: %w", err)
	}

	return nil
}

// CompleteUploadResult describes the outcome of a completed upload, including
// whether a client-provided hash was present and verified. This makes the
// integrity-check semantics explicit: a client that omits the hash gets
// HashProvided=false (no verification performed), while one that supplies a
// hash gets HashProvided=true and HashVerified=true (or the call fails with
// a hash mismatch error before returning).
type CompleteUploadResult struct {
	FileID       string
	FilePath     string
	FileName     string
	FileHash     string
	CreatedAt    string
	HashProvided bool
	HashVerified bool
	UploadedSize int64
	StorageType  string
}

func (s *FileTransferService) CompleteUpload(sessionID string) (*CompleteUploadResult, error) {
	sessionLease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "upload:"+sessionID, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire upload session lock: %w", err)
	}
	defer sessionLease.Stop()
	defer s.distLock.Unlock(context.Background(), "upload:"+sessionID, sessionLease.Token)
	terminal, err := s.getTransferResult(sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to check upload terminal state: %w", err)
	}
	if terminal != nil {
		if terminal.SessionType == "upload" && terminal.Status == "completed" {
			return completeUploadResultFromLedger(terminal), nil
		}
		return nil, fmt.Errorf("upload session is %s: %s", terminal.Status, sessionID)
	}

	session, err := s.loadUploadSession(context.Background(), sessionID, true)
	if err != nil {
		return nil, err
	}
	if session.Status != "active" {
		return nil, fmt.Errorf("upload session is not active: %s", sessionID)
	}

	if atomic.LoadInt64(&session.UploadedSize) != session.TotalSize {
		return nil, fmt.Errorf("upload incomplete: expected %d bytes, got %d bytes", session.TotalSize, atomic.LoadInt64(&session.UploadedSize))
	}
	if err := sessionLease.Err(); err != nil {
		return nil, err
	}

	atomic.StoreInt32(&session.closed, 1)

	session.tempFileMu.Lock()
	if session.tempFile != nil {
		if err := session.tempFile.Sync(); err != nil {
			session.tempFile.Close()
			session.tempFile = nil
			session.tempFileMu.Unlock()
			os.Remove(filepath.Join(s.tempDir, sessionID+".tmp"))
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("failed to sync temp file: %w", err)
		}
		if err := session.tempFile.Close(); err != nil {
			session.tempFile = nil
			session.tempFileMu.Unlock()
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("failed to close temp file: %w", err)
		}
		session.tempFile = nil
	}
	session.tempFileMu.Unlock()

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")

	// Hash verification: when the client supplied a hash (session.Hash != "")
	// the computed hash MUST match, otherwise the upload is rejected. When the
	// client did not supply a hash, verification is skipped and the result
	// reports HashProvided=false so callers can tell the two cases apart.
	hashProvided := session.Hash != ""
	hashVerified := false
	var computedHash string
	if session.hashWriter != nil && atomic.LoadInt32(&session.hashValid) == 1 {
		session.hashMu.Lock()
		computedHash = hex.EncodeToString(session.hashWriter.Sum(nil))
		session.hashMu.Unlock()
	} else {
		var err error
		computedHash, err = utils.SHA256File(tempPath)
		if err != nil {
			os.Remove(tempPath)
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("failed to compute hash: %w", err)
		}
	}
	if session.Hash != "" {
		if computedHash != session.Hash {
			os.Remove(tempPath)
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("hash mismatch: expected %s, got %s", session.Hash, computedHash)
		}
		hashVerified = true
	}

	// Encrypt the temp file before writing to storage if encryption is enabled
	storageTempPath := tempPath
	storageHash := computedHash
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		encTempPath := tempPath + ".enc"
		if err := s.cryptoSvc.EncryptFile(tempPath, encTempPath); err != nil {
			os.Remove(tempPath)
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("failed to encrypt file: %w", err)
		}
		os.Remove(tempPath)

		storageTempPath = encTempPath
	}
	if err := sessionLease.Err(); err != nil {
		os.Remove(storageTempPath)
		s.uploadSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return nil, err
	}
	namespaceLease, err := s.acquireFileNamespace(session.FilePath)
	if err != nil {
		os.Remove(storageTempPath)
		s.uploadSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return nil, err
	}
	defer releaseNamespaceLease(namespaceLease)

	fileLease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "file:"+session.FilePath, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		os.Remove(storageTempPath)
		s.uploadSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return nil, fmt.Errorf("failed to acquire lock for file %s: %w", session.FilePath, err)
	}
	defer s.distLock.Unlock(context.Background(), "file:"+session.FilePath, fileLease.Token)
	defer fileLease.Stop()

	fileMetadataSvc := database.NewFileMetadataService(s.db)
	tx, err := s.db.BeginNamespaceWrite(context.Background(), namespaceLease, session.FilePath)
	if err != nil {
		os.Remove(storageTempPath)
		s.uploadSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return nil, fmt.Errorf("failed to begin fenced upload transaction: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	existingMeta, err := fileMetadataSvc.GetByPathTx(tx, session.FilePath)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("failed to query existing metadata: %w", err)
	}
	var backup storage.ReplaceBackup
	if existingMeta != nil {
		backup, err = storage.BeginReplace(s.storage, session.FilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare upload commit: %w", err)
		}
	}
	if err := s.storage.WriteFromTempFile(session.FilePath, storageTempPath); err != nil {
		rollbackStorageBackup(backup, session.FilePath)
		return nil, fmt.Errorf("failed to write file from temp: %w", err)
	}
	if err := fileLease.Err(); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return nil, err
	}
	if err := namespaceLease.Err(); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return nil, err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return nil, err
	}

	now := utils.GetCurrentTimestamp()
	var meta *database.FileMetadata
	if existingMeta != nil {
		existingMeta.Size = session.TotalSize
		existingMeta.Hash = storageHash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		if err := fileMetadataSvc.UpdateTx(tx, existingMeta); err != nil {
			rollbackStorageBackup(backup, session.FilePath)
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("failed to update file metadata: %w", err)
		}
		meta = existingMeta
	} else {
		meta = &database.FileMetadata{
			ID:              utils.GenerateUUID(),
			Path:            session.FilePath,
			Name:            session.FileName,
			Size:            session.TotalSize,
			Hash:            storageHash,
			StorageType:     s.storage.StorageType(),
			StorageLocation: "",
			CreatedAt:       now,
			UpdatedAt:       now,
			IsDeleted:       false,
		}

		if err := fileMetadataSvc.CreateTx(tx, meta); err != nil {
			rollbackStorageWrite(s.storage, backup, session.FilePath)
			s.uploadSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return nil, fmt.Errorf("failed to create file metadata: %w", err)
		}
	}
	terminal = &database.TransferSessionResult{
		SessionID: sessionID, SessionType: "upload", Status: "completed",
		FileID: meta.ID, FilePath: meta.Path, FileName: meta.Name, FileHash: meta.Hash,
		FileCreatedAt: meta.CreatedAt, HashProvided: hashProvided, HashVerified: hashVerified,
		UploadedSize: session.TotalSize, StorageType: s.storage.StorageType(),
		UpdatedAt: now, ExpiresAt: transferResultExpiry(),
	}
	if err := database.NewTransferSessionResultService(s.db).SaveTx(tx, terminal); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return nil, err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return nil, fmt.Errorf("failed to commit upload metadata transaction: %w", err)
	}
	rollbackTx = false
	commitStorageBackup(backup, session.FilePath)

	session.FileID = meta.ID
	session.Status = "completed"
	session.UpdatedAt = now
	session.CompletionFileHash = meta.Hash
	session.CompletionCreatedAt = meta.CreatedAt
	session.CompletionHashProvided = hashProvided
	session.CompletionHashVerified = hashVerified
	if err := s.sessionStore.Set(context.Background(), "upload", sessionID, session, 5*time.Minute); err != nil {
		logger.Warn("failed to persist completed upload session %s: %v", sessionID, err)
	}

	s.uploadSessions.Delete(sessionID)
	s.releaseSessionSlot()

	os.Remove(storageTempPath)

	return completeUploadResultFromLedger(terminal), nil
}

func (s *FileTransferService) AbortUpload(sessionID string) error {
	lease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "upload:"+sessionID, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire upload session lock: %w", err)
	}
	defer lease.Stop()
	defer s.distLock.Unlock(context.Background(), "upload:"+sessionID, lease.Token)
	terminal, err := s.getTransferResult(sessionID)
	if err != nil {
		return fmt.Errorf("failed to check upload terminal state: %w", err)
	}
	if terminal != nil {
		if terminal.SessionType == "upload" && terminal.Status == "aborted" {
			return nil
		}
		return fmt.Errorf("upload session is %s: %s", terminal.Status, sessionID)
	}

	session, err := s.loadUploadSession(context.Background(), sessionID, true)
	if err != nil {
		return err
	}
	if err := lease.Err(); err != nil {
		return err
	}
	now := utils.GetCurrentTimestamp()
	terminal = &database.TransferSessionResult{
		SessionID: sessionID, SessionType: "upload", Status: "aborted",
		FilePath: session.FilePath, FileName: session.FileName,
		UploadedSize: atomic.LoadInt64(&session.UploadedSize), StorageType: s.storage.StorageType(),
		UpdatedAt: now, ExpiresAt: transferResultExpiry(),
	}
	if err := database.NewTransferSessionResultService(s.db).Save(terminal); err != nil {
		return err
	}
	atomic.StoreInt32(&session.closed, 1)
	// An aborted session is no longer a resumable upload. Keep the actual
	// acknowledged size in the terminal ledger, but expose zero progress from
	// the retained session snapshot so clients cannot mistake it for resumable
	// state.
	atomic.StoreInt64(&session.UploadedSize, 0)
	session.tempFileMu.Lock()
	if session.tempFile != nil {
		session.tempFile.Close()
		session.tempFile = nil
	}
	session.tempFileMu.Unlock()
	session.Status = "aborted"
	session.UpdatedAt = now
	if err := s.sessionStore.Set(context.Background(), "upload", sessionID, session, 5*time.Minute); err != nil {
		logger.Warn("failed to persist aborted upload session %s: %v", sessionID, err)
	}
	s.uploadSessions.Delete(sessionID)
	s.releaseSessionSlot()

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	os.Remove(tempPath)

	return nil
}

func (s *FileTransferService) GetUploadSession(sessionID string) (*UploadSession, error) {
	session, err := s.loadUploadSession(context.Background(), sessionID, true)
	if err != nil {
		return nil, err
	}
	if session.Status != "active" {
		return nil, fmt.Errorf("upload session is %s: %s", session.Status, sessionID)
	}
	return session, nil
}

func (s *FileTransferService) prepareDownloadFile(session *DownloadSession) error {
	session.prepareMu.Lock()
	defer session.prepareMu.Unlock()
	if !session.Encrypted || session.decryptedFile != nil {
		return nil
	}
	if s.cryptoSvc == nil || !s.cryptoSvc.IsEnabled() {
		return errors.New("download session requires the configured encryption key")
	}

	encTempPath := filepath.Join(s.tempDir, session.SessionID+".enc")
	encFile, err := os.Create(encTempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file for encrypted data: %w", err)
	}
	reader, err := s.storage.OpenReader(session.FilePath)
	if err != nil {
		_ = encFile.Close()
		_ = os.Remove(encTempPath)
		return fmt.Errorf("failed to open encrypted file from storage: %w", err)
	}
	if _, err := io.Copy(encFile, reader); err != nil {
		_ = reader.Close()
		_ = encFile.Close()
		_ = os.Remove(encTempPath)
		return fmt.Errorf("failed to stream encrypted file: %w", err)
	}
	if err := reader.Close(); err != nil {
		_ = encFile.Close()
		_ = os.Remove(encTempPath)
		return fmt.Errorf("failed to close encrypted storage reader: %w", err)
	}
	if err := encFile.Close(); err != nil {
		_ = os.Remove(encTempPath)
		return fmt.Errorf("failed to close encrypted temp file: %w", err)
	}

	decTempPath := filepath.Join(s.tempDir, session.SessionID+".dec")
	if err := s.cryptoSvc.DecryptFileStreaming(encTempPath, decTempPath); err != nil {
		_ = os.Remove(encTempPath)
		_ = os.Remove(decTempPath)
		return fmt.Errorf("failed to decrypt file: %w", err)
	}
	_ = os.Remove(encTempPath)
	decFile, err := os.Open(decTempPath)
	if err != nil {
		_ = os.Remove(decTempPath)
		return fmt.Errorf("failed to open decrypted temp file: %w", err)
	}
	session.decryptedTempPath = decTempPath
	session.decryptedFile = decFile
	if info, err := decFile.Stat(); err == nil {
		session.TotalSize = info.Size()
	}
	return nil
}

func (s *FileTransferService) CreateDownloadSession(filePath, clientID string) (string, error) {
	filePath = utils.NormalizePath(filePath)
	namespaceLease, err := s.acquireFileNamespace(filePath)
	if err != nil {
		return "", err
	}
	keepNamespaceLease := false
	defer func() {
		if !keepNamespaceLease {
			releaseNamespaceLease(namespaceLease)
		}
	}()
	meta, err := database.NewFileMetadataService(s.db).GetByPath(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to get file metadata: %w", err)
	}
	if meta == nil {
		return "", fmt.Errorf("file not found: %s", filePath)
	}

	sessionID := utils.GenerateUUID()
	now := utils.GetCurrentTimestamp()

	session := &DownloadSession{
		SessionID:           sessionID,
		FileID:              meta.ID,
		FilePath:            filePath,
		TotalSize:           meta.Size,
		ClientID:            clientID,
		Status:              "active",
		CreatedAt:           now,
		UpdatedAt:           now,
		NamespaceLeaseToken: namespaceLease.Token(),
		Encrypted:           s.cryptoSvc != nil && s.cryptoSvc.IsEnabled(),
		namespaceLease:      namespaceLease,
	}

	if err := s.prepareDownloadFile(session); err != nil {
		return "", err
	}

	s.downloadSessions.Store(sessionID, session)

	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "download", sessionID, session, 2*time.Hour); err != nil {
		s.downloadSessions.Delete(sessionID)
		if session.decryptedFile != nil {
			_ = session.decryptedFile.Close()
		}
		if session.decryptedTempPath != "" {
			_ = os.Remove(session.decryptedTempPath)
		}
		return "", fmt.Errorf("failed to persist download session: %w", err)
	}
	keepNamespaceLease = true

	return sessionID, nil
}

func (s *FileTransferService) ensureDownloadNamespaceLease(session *DownloadSession) error {
	session.namespaceMu.Lock()
	defer session.namespaceMu.Unlock()
	if session.namespaceLease != nil {
		return session.namespaceLease.Err()
	}
	var lease *database.NamespaceLease
	var err error
	if session.NamespaceLeaseToken != "" {
		lease, err = s.resumeFileNamespace(session.FilePath, session.NamespaceLeaseToken)
		if errors.Is(err, database.ErrNamespaceLeaseGone) {
			lease, err = s.acquireFileNamespace(session.FilePath)
		}
	} else {
		// Backward compatibility for sessions persisted before lease tokens were stored.
		lease, err = s.acquireFileNamespace(session.FilePath)
	}
	if err != nil {
		return err
	}
	session.namespaceLease = lease
	session.NamespaceLeaseToken = lease.Token()
	return nil
}

func releaseDownloadNamespaceLease(session *DownloadSession) {
	session.namespaceMu.Lock()
	lease := session.namespaceLease
	session.namespaceLease = nil
	session.namespaceMu.Unlock()
	if lease != nil {
		releaseNamespaceLease(lease)
	}
}

func (s *FileTransferService) loadDownloadSession(sessionID string) (*DownloadSession, error) {
	if val, ok := s.downloadSessions.Load(sessionID); ok {
		return val.(*DownloadSession), nil
	}

	var stored DownloadSession
	if err := s.sessionStore.Get(context.Background(), "download", sessionID, &stored); err != nil {
		return nil, fmt.Errorf("download session not found: %s", sessionID)
	}
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		stored.Encrypted = true
	}
	actual, _ := s.downloadSessions.LoadOrStore(sessionID, &stored)
	return actual.(*DownloadSession), nil
}

func downloadSessionSnapshot(session *DownloadSession) *DownloadSession {
	session.progressMu.Lock()
	updatedAt := session.UpdatedAt
	session.progressMu.Unlock()
	return &DownloadSession{
		SessionID:           session.SessionID,
		FileID:              session.FileID,
		FilePath:            session.FilePath,
		TotalSize:           session.TotalSize,
		DownloadedSize:      atomic.LoadInt64(&session.DownloadedSize),
		ClientID:            session.ClientID,
		Status:              session.Status,
		CreatedAt:           session.CreatedAt,
		UpdatedAt:           updatedAt,
		NamespaceLeaseToken: session.NamespaceLeaseToken,
		Encrypted:           session.Encrypted,
	}
}

func (s *FileTransferService) DownloadChunk(sessionID string, size int, offset int64) ([]byte, error) {
	operationLease, err := s.acquireDownloadOperation(sessionID, database.NamespaceShared)
	if err != nil {
		return nil, err
	}
	defer releaseNamespaceLease(operationLease)

	session, err := s.loadDownloadSession(sessionID)
	if err != nil {
		return nil, err
	}
	session.readMu.RLock()
	defer session.readMu.RUnlock()
	exists, err := s.sessionStore.Exists(context.Background(), "download", sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to verify download session: %w", err)
	}
	if !exists {
		s.downloadSessions.Delete(sessionID)
		releaseDownloadNamespaceLease(session)
		return nil, fmt.Errorf("download session not found: %s", sessionID)
	}
	if err := s.ensureDownloadNamespaceLease(session); err != nil {
		return nil, err
	}
	if err := s.prepareDownloadFile(session); err != nil {
		return nil, err
	}

	if offset < 0 {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	if offset >= session.TotalSize {
		return nil, fmt.Errorf("offset beyond file size: %d >= %d", offset, session.TotalSize)
	}

	var data []byte

	// If a decrypted temp file is available, read from it instead of storage
	if session.decryptedFile != nil {
		buf := make([]byte, size)
		n, readErr := session.decryptedFile.ReadAt(buf, offset)
		if readErr != nil && readErr != io.EOF {
			return nil, fmt.Errorf("failed to read decrypted chunk: %w", readErr)
		}
		data = buf[:n]
	} else {
		data, err = s.storage.ReadAt(session.FilePath, size, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read file chunk: %w", err)
		}
	}

	atomic.AddInt64(&session.DownloadedSize, int64(len(data)))
	atomic.AddInt64(&session.chunkCount, 1)
	session.progressMu.Lock()
	session.UpdatedAt = utils.GetCurrentTimestamp()
	session.progressMu.Unlock()
	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "download", sessionID, downloadSessionSnapshot(session), 2*time.Hour); err != nil {
		logger.Warn("failed to update download session in Redis: %v", err)
	}

	return data, nil
}

func (s *FileTransferService) CompleteDownload(sessionID string) error {
	operationLease, err := s.acquireDownloadOperation(sessionID, database.NamespaceExclusive)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(operationLease)

	session, err := s.loadDownloadSession(sessionID)
	if err != nil {
		return err
	}
	session.readMu.Lock()
	defer session.readMu.Unlock()
	if err := s.ensureDownloadNamespaceLease(session); err != nil {
		return err
	}
	ctx := context.Background()
	exists, err := s.sessionStore.Exists(ctx, "download", sessionID)
	if err != nil {
		return fmt.Errorf("failed to verify download session: %w", err)
	}
	if !exists {
		s.downloadSessions.Delete(sessionID)
		releaseDownloadNamespaceLease(session)
		return fmt.Errorf("download session not found: %s", sessionID)
	}
	if err := s.sessionStore.Delete(ctx, "download", sessionID); err != nil {
		return fmt.Errorf("failed to delete completed download session: %w", err)
	}
	session.progressMu.Lock()
	session.Status = "completed"
	session.UpdatedAt = utils.GetCurrentTimestamp()
	session.progressMu.Unlock()

	// Clean up decrypted temp file if present
	if session.decryptedFile != nil {
		session.decryptedFile.Close()
	}
	if session.decryptedTempPath != "" {
		os.Remove(session.decryptedTempPath)
	}

	s.downloadSessions.Delete(sessionID)
	releaseDownloadNamespaceLease(session)

	return nil
}

func (s *FileTransferService) AbortDownload(sessionID string) error {
	operationLease, err := s.acquireDownloadOperation(sessionID, database.NamespaceExclusive)
	if err != nil {
		return err
	}
	defer releaseNamespaceLease(operationLease)

	session, err := s.loadDownloadSession(sessionID)
	if err != nil {
		return err
	}
	session.readMu.Lock()
	defer session.readMu.Unlock()
	if err := s.ensureDownloadNamespaceLease(session); err != nil {
		return err
	}
	ctx := context.Background()
	exists, err := s.sessionStore.Exists(ctx, "download", sessionID)
	if err != nil {
		return fmt.Errorf("failed to verify download session: %w", err)
	}
	if !exists {
		s.downloadSessions.Delete(sessionID)
		releaseDownloadNamespaceLease(session)
		return fmt.Errorf("download session not found: %s", sessionID)
	}
	if err := s.sessionStore.Delete(ctx, "download", sessionID); err != nil {
		return fmt.Errorf("failed to delete aborted download session: %w", err)
	}
	session.progressMu.Lock()
	session.Status = "aborted"
	session.UpdatedAt = utils.GetCurrentTimestamp()
	session.progressMu.Unlock()

	// Clean up decrypted temp file if present
	if session.decryptedFile != nil {
		session.decryptedFile.Close()
	}
	if session.decryptedTempPath != "" {
		os.Remove(session.decryptedTempPath)
	}

	s.downloadSessions.Delete(sessionID)
	releaseDownloadNamespaceLease(session)

	return nil
}

func (s *FileTransferService) GetUploadProgress(sessionID string) int64 {
	var stored UploadSession
	if err := s.sessionStore.Get(context.Background(), "upload", sessionID, &stored); err == nil {
		return stored.UploadedSize
	}
	if val, ok := s.uploadSessions.Load(sessionID); ok {
		return atomic.LoadInt64(&val.(*UploadSession).UploadedSize)
	}
	return 0
}

func (s *FileTransferService) CleanupExpiredSessions(maxAgeSeconds int) {
	expiryTime := time.Now().Add(-time.Duration(maxAgeSeconds) * time.Second)
	ctx := context.Background()
	if err := database.NewTransferSessionResultService(s.db).DeleteExpired(); err != nil {
		logger.Warn("failed to delete expired transfer session results: %v", err)
	}

	s.uploadSessions.Range(func(key, value interface{}) bool {
		sessionID := key.(string)
		session := value.(*UploadSession)
		lease, err := distributed.AcquireLockLease(ctx, s.distLock, "upload:"+sessionID, 10*time.Second, 1, 10*time.Millisecond)
		if err != nil {
			return true
		}
		var stored UploadSession
		if err := s.sessionStore.Get(ctx, "upload", sessionID, &stored); err == nil {
			session.Status = stored.Status
			session.UpdatedAt = stored.UpdatedAt
		}
		updatedAt, err := utils.ParseTimestamp(session.UpdatedAt)
		if err != nil {
			lease.Stop()
			_ = s.distLock.Unlock(ctx, "upload:"+sessionID, lease.Token)
			return true
		}
		if updatedAt.Before(expiryTime) {
			session.Status = "expired"
			atomic.StoreInt32(&session.closed, 1)
			session.tempFileMu.Lock()
			if session.tempFile != nil {
				session.tempFile.Close()
				session.tempFile = nil
			}
			session.tempFileMu.Unlock()
			tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
			os.Remove(tempPath)
			s.uploadSessions.Delete(key)
			s.releaseSessionSlot()
			s.sessionStore.Delete(ctx, "upload", sessionID)
		}
		lease.Stop()
		_ = s.distLock.Unlock(ctx, "upload:"+sessionID, lease.Token)
		return true
	})

	s.downloadSessions.Range(func(key, value interface{}) bool {
		session := value.(*DownloadSession)
		session.progressMu.Lock()
		updatedAt, err := utils.ParseTimestamp(session.UpdatedAt)
		session.progressMu.Unlock()
		if err != nil {
			return true
		}
		if updatedAt.Before(expiryTime) {
			operationLease, err := s.acquireDownloadOperation(key.(string), database.NamespaceExclusive)
			if err != nil {
				return true
			}
			session.readMu.Lock()
			exists, existsErr := s.sessionStore.Exists(ctx, "download", key.(string))
			if existsErr != nil {
				session.readMu.Unlock()
				releaseNamespaceLease(operationLease)
				return true
			}
			if !exists {
				s.downloadSessions.Delete(key)
				releaseDownloadNamespaceLease(session)
				session.readMu.Unlock()
				releaseNamespaceLease(operationLease)
				return true
			}
			var stored DownloadSession
			if err = s.sessionStore.Get(ctx, "download", key.(string), &stored); err == nil {
				updatedAt, err = utils.ParseTimestamp(stored.UpdatedAt)
			}
			if err != nil || !updatedAt.Before(expiryTime) {
				if err == nil {
					session.progressMu.Lock()
					session.UpdatedAt = stored.UpdatedAt
					session.progressMu.Unlock()
				}
				session.readMu.Unlock()
				releaseNamespaceLease(operationLease)
				return true
			}
			if err := s.sessionStore.Delete(ctx, "download", key.(string)); err != nil {
				session.readMu.Unlock()
				releaseNamespaceLease(operationLease)
				return true
			}
			session.progressMu.Lock()
			session.Status = "expired"
			session.progressMu.Unlock()
			if session.decryptedFile != nil {
				session.decryptedFile.Close()
			}
			if session.decryptedTempPath != "" {
				os.Remove(session.decryptedTempPath)
			}
			s.downloadSessions.Delete(key)
			releaseDownloadNamespaceLease(session)
			session.readMu.Unlock()
			releaseNamespaceLease(operationLease)
		}
		return true
	})

	s.multipartSessions.Range(func(key, value interface{}) bool {
		sessionID := key.(string)
		session := value.(*MultipartUploadSession)
		lease, err := distributed.AcquireLockLease(ctx, s.distLock, "multipart:"+sessionID, 10*time.Second, 1, 10*time.Millisecond)
		if err != nil {
			return true
		}
		var stored multipartUploadState
		if err := s.sessionStore.Get(ctx, "multipart_upload", sessionID, &stored); err == nil {
			session.Status = stored.Status
			session.UpdatedAt = stored.UpdatedAt
		}
		updatedAt, err := utils.ParseTimestamp(session.UpdatedAt)
		if err != nil {
			lease.Stop()
			_ = s.distLock.Unlock(ctx, "multipart:"+sessionID, lease.Token)
			return true
		}
		if updatedAt.Before(expiryTime) {
			session.Status = "expired"
			atomic.StoreInt32(&session.closed, 1)
			session.partsMu.Lock()
			if session.tempFile != nil {
				session.tempFile.Close()
				session.tempFile = nil
			}
			session.partsMu.Unlock()
			tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
			os.Remove(tempPath)
			s.multipartSessions.Delete(key)
			s.releaseSessionSlot()
			s.sessionStore.Delete(ctx, "multipart_upload", sessionID)
		}
		lease.Stop()
		_ = s.distLock.Unlock(ctx, "multipart:"+sessionID, lease.Token)
		return true
	})
}

func (s *FileTransferService) StartCleanupThread(intervalSeconds, maxAgeSeconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cleanupCancel != nil {
		s.cleanupCancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cleanupCancel = cancel

	go func() {
		ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.CleanupExpiredSessions(maxAgeSeconds)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *FileTransferService) StopCleanupThread() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cleanupCancel != nil {
		s.cleanupCancel()
		s.cleanupCancel = nil
	}
	atomic.StoreInt32(&s.cleanupRunning, 0)
}
