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
	SessionID    string
	FileID       string
	FilePath     string
	FileName     string
	TotalSize    int64
	UploadedSize int64
	Hash         string
	ClientID     string
	Status       string
	CreatedAt    string
	UpdatedAt    string
	chunkCount   int64
	tempFile     *os.File
	tempFileMu   sync.Mutex
	hashWriter   hash.Hash
	hashMu       sync.Mutex
	lastOffset   int64
	hashValid    int32
	closed       int32
}

type DownloadSession struct {
	SessionID         string
	FileID            string
	FilePath          string
	TotalSize         int64
	DownloadedSize    int64
	ClientID          string
	Status            string
	CreatedAt         string
	UpdatedAt         string
	chunkCount        int64
	decryptedTempPath string
	decryptedFile     *os.File
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
	}
}

func (s *FileTransferService) SetCryptoService(cryptoSvc *crypto.CryptoService) {
	s.cryptoSvc = cryptoSvc
}

func (s *FileTransferService) CreateUploadSession(filePath, fileName string, totalSize int64, clientID, hash string) (string, error) {
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
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	if totalSize > 0 {
		if err := file.Truncate(totalSize); err != nil {
			file.Close()
			os.Remove(tempPath)
			return "", fmt.Errorf("failed to pre-allocate temp file: %w", err)
		}
	}

	session.tempFile = file

	if hash != "" {
		session.hashWriter = sha256.New()
		session.hashValid = 1
		session.lastOffset = 0
	}

	s.uploadSessions.Store(sessionID, session)

	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "upload", sessionID, session, 2*time.Hour); err != nil {
		logger.Warn("failed to store upload session in Redis: %v", err)
	}

	return sessionID, nil
}

func (s *FileTransferService) UploadChunk(sessionID string, data []byte, offset int64) error {
	val, ok := s.uploadSessions.Load(sessionID)
	if !ok {
		ctx := context.Background()
		var redisSession UploadSession
		if err := s.sessionStore.Get(ctx, "upload", sessionID, &redisSession); err == nil {
			val = &redisSession
			s.uploadSessions.Store(sessionID, val)
			ok = true
		}
	}

	if !ok {
		return fmt.Errorf("upload session not found: %s", sessionID)
	}

	session := val.(*UploadSession)

	if atomic.LoadInt32(&session.closed) == 1 {
		return fmt.Errorf("upload session is closed: %s", sessionID)
	}

	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}

	if offset+int64(len(data)) > session.TotalSize {
		return fmt.Errorf("write beyond file size: offset=%d len=%d total=%d", offset, len(data), session.TotalSize)
	}

	if session.tempFile != nil {
		if _, err := session.tempFile.WriteAt(data, offset); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}
	} else {
		session.tempFileMu.Lock()
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
		file.Close()
		session.tempFileMu.Unlock()
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

	atomic.AddInt64(&session.UploadedSize, int64(len(data)))
	atomic.AddInt64(&session.chunkCount, 1)

	chunkNum := atomic.LoadInt64(&session.chunkCount)
	if chunkNum%8 == 0 {
		session.UpdatedAt = utils.GetCurrentTimestamp()
		ctx := context.Background()
		if err := s.sessionStore.Set(ctx, "upload", sessionID, session, 2*time.Hour); err != nil {
			logger.Warn("failed to update upload session in Redis: %v", err)
		}
	}

	return nil
}

func (s *FileTransferService) CompleteUpload(sessionID string) error {
	val, ok := s.uploadSessions.Load(sessionID)
	if !ok {
		ctx := context.Background()
		var redisSession UploadSession
		if err := s.sessionStore.Get(ctx, "upload", sessionID, &redisSession); err == nil {
			val = &redisSession
			s.uploadSessions.Store(sessionID, val)
			ok = true
		}
	}

	if !ok {
		return fmt.Errorf("upload session not found: %s", sessionID)
	}

	session := val.(*UploadSession)

	if atomic.LoadInt64(&session.UploadedSize) != session.TotalSize {
		return fmt.Errorf("upload incomplete: expected %d bytes, got %d bytes", session.TotalSize, atomic.LoadInt64(&session.UploadedSize))
	}

	atomic.StoreInt32(&session.closed, 1)

	if session.tempFile != nil {
		if err := session.tempFile.Sync(); err != nil {
			session.tempFile.Close()
			os.Remove(filepath.Join(s.tempDir, sessionID+".tmp"))
			return fmt.Errorf("failed to sync temp file: %w", err)
		}
		if err := session.tempFile.Close(); err != nil {
			return fmt.Errorf("failed to close temp file: %w", err)
		}
	}

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")

	if session.Hash != "" {
		var computedHash string
		if session.hashWriter != nil && atomic.LoadInt32(&session.hashValid) == 1 {
			session.hashMu.Lock()
			computedHash = hex.EncodeToString(session.hashWriter.Sum(nil))
			session.hashMu.Unlock()
		} else {
			var err error
			computedHash, err = utils.SHA256File(tempPath)
			if err != nil {
				return fmt.Errorf("failed to compute hash: %w", err)
			}
		}
		if computedHash != session.Hash {
			os.Remove(tempPath)
			s.uploadSessions.Delete(sessionID)
			return fmt.Errorf("hash mismatch: expected %s, got %s", session.Hash, computedHash)
		}
	}

	// Encrypt the temp file before writing to storage if encryption is enabled
	storageTempPath := tempPath
	storageHash := session.Hash
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		encTempPath := tempPath + ".enc"
		if err := s.cryptoSvc.EncryptFile(tempPath, encTempPath); err != nil {
			os.Remove(tempPath)
			s.uploadSessions.Delete(sessionID)
			return fmt.Errorf("failed to encrypt file: %w", err)
		}
		os.Remove(tempPath)

		// Compute hash on encrypted data
		var err error
		storageHash, err = utils.SHA256File(encTempPath)
		if err != nil {
			os.Remove(encTempPath)
			s.uploadSessions.Delete(sessionID)
			return fmt.Errorf("failed to compute encrypted hash: %w", err)
		}
		storageTempPath = encTempPath
	}

	token, err := distributed.AcquireLock(context.Background(), s.distLock, "file:"+session.FilePath, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		os.Remove(storageTempPath)
		return fmt.Errorf("failed to acquire lock for file %s: %w", session.FilePath, err)
	}
	defer s.distLock.Unlock(context.Background(), "file:"+session.FilePath, token)

	if err := s.storage.WriteFromTempFile(session.FilePath, storageTempPath); err != nil {
		os.Remove(storageTempPath)
		s.uploadSessions.Delete(sessionID)
		return fmt.Errorf("failed to write file from temp: %w", err)
	}

	now := utils.GetCurrentTimestamp()

	existingMeta, err := database.NewFileMetadataService(s.db).GetByPath(session.FilePath)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		// Log the error but continue - treat as new file
		logger.Error("Failed to query existing metadata: %v", err)
	}

	var meta *database.FileMetadata
	if existingMeta != nil {
		existingMeta.Size = session.TotalSize
		existingMeta.Hash = storageHash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		if err := database.NewFileMetadataService(s.db).Update(existingMeta); err != nil {
			return fmt.Errorf("failed to update file metadata: %w", err)
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

		if err := database.NewFileMetadataService(s.db).Create(meta); err != nil {
			s.storage.Remove(session.FilePath)
			return fmt.Errorf("failed to create file metadata: %w", err)
		}
	}

	session.FileID = meta.ID
	session.Status = "completed"
	session.UpdatedAt = now

	s.uploadSessions.Delete(sessionID)

	os.Remove(storageTempPath)

	ctx := context.Background()
	if err := s.sessionStore.Delete(ctx, "upload", sessionID); err != nil {
		logger.Warn("Failed to delete session from store: %v", err)
	}

	return nil
}

func (s *FileTransferService) AbortUpload(sessionID string) error {
	val, ok := s.uploadSessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("upload session not found: %s", sessionID)
	}

	session := val.(*UploadSession)
	atomic.StoreInt32(&session.closed, 1)
	if session.tempFile != nil {
		session.tempFile.Close()
	}
	session.Status = "aborted"
	session.UpdatedAt = utils.GetCurrentTimestamp()
	s.uploadSessions.Delete(sessionID)

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	os.Remove(tempPath)

	ctx := context.Background()
	if err := s.sessionStore.Delete(ctx, "upload", sessionID); err != nil {
		logger.Warn("Failed to delete session from store: %v", err)
	}

	return nil
}

func (s *FileTransferService) GetUploadSession(sessionID string) (*UploadSession, error) {
	val, ok := s.uploadSessions.Load(sessionID)
	if !ok {
		ctx := context.Background()
		var redisSession UploadSession
		if err := s.sessionStore.Get(ctx, "upload", sessionID, &redisSession); err == nil {
			return &redisSession, nil
		}
	}

	if !ok {
		return nil, fmt.Errorf("upload session not found: %s", sessionID)
	}

	return val.(*UploadSession), nil
}

func (s *FileTransferService) CreateDownloadSession(filePath, clientID string) (string, error) {
	filePath = utils.NormalizePath(filePath)
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
		SessionID: sessionID,
		FileID:    meta.ID,
		FilePath:  filePath,
		TotalSize: meta.Size,
		ClientID:  clientID,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}

	// If encryption is enabled, decrypt the file to a temp location for reading
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		encData, err := s.storage.Read(filePath)
		if err != nil {
			return "", fmt.Errorf("failed to read encrypted file: %w", err)
		}

		decrypted, err := s.cryptoSvc.Decrypt(string(encData))
		if err != nil {
			return "", fmt.Errorf("failed to decrypt file: %w", err)
		}

		decTempPath := filepath.Join(s.tempDir, sessionID+".dec")
		if err := os.WriteFile(decTempPath, []byte(decrypted), 0644); err != nil {
			return "", fmt.Errorf("failed to write decrypted temp file: %w", err)
		}

		decFile, err := os.Open(decTempPath)
		if err != nil {
			os.Remove(decTempPath)
			return "", fmt.Errorf("failed to open decrypted temp file: %w", err)
		}

		session.decryptedTempPath = decTempPath
		session.decryptedFile = decFile

		// Update total size to the decrypted (plaintext) size
		if info, err := decFile.Stat(); err == nil {
			session.TotalSize = info.Size()
		}
	}

	s.downloadSessions.Store(sessionID, session)

	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "download", sessionID, session, 2*time.Hour); err != nil {
		logger.Warn("failed to store download session in Redis: %v", err)
	}

	return sessionID, nil
}

func (s *FileTransferService) DownloadChunk(sessionID string, size int, offset int64) ([]byte, error) {
	val, ok := s.downloadSessions.Load(sessionID)
	if !ok {
		ctx := context.Background()
		var redisSession DownloadSession
		if err := s.sessionStore.Get(ctx, "download", sessionID, &redisSession); err == nil {
			val = &redisSession
			s.downloadSessions.Store(sessionID, val)
			ok = true
		}
	}

	if !ok {
		return nil, fmt.Errorf("download session not found: %s", sessionID)
	}

	session := val.(*DownloadSession)

	if offset < 0 {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	if offset >= session.TotalSize {
		return nil, fmt.Errorf("offset beyond file size: %d >= %d", offset, session.TotalSize)
	}

	var data []byte
	var err error

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

	chunkNum := atomic.LoadInt64(&session.chunkCount)
	if chunkNum%8 == 0 {
		session.UpdatedAt = utils.GetCurrentTimestamp()
		ctx := context.Background()
		if err := s.sessionStore.Set(ctx, "download", sessionID, session, 2*time.Hour); err != nil {
			logger.Warn("failed to update download session in Redis: %v", err)
		}
	}

	return data, nil
}

func (s *FileTransferService) CompleteDownload(sessionID string) error {
	val, ok := s.downloadSessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("download session not found: %s", sessionID)
	}

	session := val.(*DownloadSession)
	session.Status = "completed"
	session.UpdatedAt = utils.GetCurrentTimestamp()

	// Clean up decrypted temp file if present
	if session.decryptedFile != nil {
		session.decryptedFile.Close()
	}
	if session.decryptedTempPath != "" {
		os.Remove(session.decryptedTempPath)
	}

	s.downloadSessions.Delete(sessionID)

	ctx := context.Background()
	s.sessionStore.Delete(ctx, "download", sessionID)

	return nil
}

func (s *FileTransferService) AbortDownload(sessionID string) error {
	val, ok := s.downloadSessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("download session not found: %s", sessionID)
	}

	session := val.(*DownloadSession)
	session.Status = "aborted"
	session.UpdatedAt = utils.GetCurrentTimestamp()

	// Clean up decrypted temp file if present
	if session.decryptedFile != nil {
		session.decryptedFile.Close()
	}
	if session.decryptedTempPath != "" {
		os.Remove(session.decryptedTempPath)
	}

	s.downloadSessions.Delete(sessionID)

	ctx := context.Background()
	s.sessionStore.Delete(ctx, "download", sessionID)

	return nil
}

func (s *FileTransferService) GetUploadProgress(sessionID string) int64 {
	val, ok := s.uploadSessions.Load(sessionID)
	if !ok {
		return 0
	}
	session := val.(*UploadSession)
	return atomic.LoadInt64(&session.UploadedSize)
}

func (s *FileTransferService) CleanupExpiredSessions(maxAgeSeconds int) {
	expiryTime := time.Now().Add(-time.Duration(maxAgeSeconds) * time.Second)
	ctx := context.Background()

	s.uploadSessions.Range(func(key, value interface{}) bool {
		session := value.(*UploadSession)
		createdAt, err := utils.ParseTimestamp(session.CreatedAt)
		if err != nil {
			return true
		}
		if createdAt.Before(expiryTime) {
			session.Status = "expired"
			atomic.StoreInt32(&session.closed, 1)
			if session.tempFile != nil {
				session.tempFile.Close()
			}
			tempPath := filepath.Join(s.tempDir, key.(string)+".tmp")
			os.Remove(tempPath)
			s.uploadSessions.Delete(key)
			s.sessionStore.Delete(ctx, "upload", key.(string))
		}
		return true
	})

	s.downloadSessions.Range(func(key, value interface{}) bool {
		session := value.(*DownloadSession)
		createdAt, err := utils.ParseTimestamp(session.CreatedAt)
		if err != nil {
			return true
		}
		if createdAt.Before(expiryTime) {
			session.Status = "expired"
			if session.decryptedFile != nil {
				session.decryptedFile.Close()
			}
			if session.decryptedTempPath != "" {
				os.Remove(session.decryptedTempPath)
			}
			s.downloadSessions.Delete(key)
			s.sessionStore.Delete(ctx, "download", key.(string))
		}
		return true
	})

	s.multipartSessions.Range(func(key, value interface{}) bool {
		session := value.(*MultipartUploadSession)
		createdAt, err := utils.ParseTimestamp(session.CreatedAt)
		if err != nil {
			return true
		}
		if createdAt.Before(expiryTime) {
			session.Status = "expired"
			atomic.StoreInt32(&session.closed, 1)
			if session.tempFile != nil {
				session.tempFile.Close()
			}
			tempPath := filepath.Join(s.tempDir, key.(string)+".tmp")
			os.Remove(tempPath)
			s.multipartSessions.Delete(key)
			s.sessionStore.Delete(ctx, "multipart_upload", key.(string))
		}
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
