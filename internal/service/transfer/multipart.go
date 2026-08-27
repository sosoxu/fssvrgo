package transfer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

type UploadPart struct {
	PartNumber   int
	Offset       int64
	Size         int64
	UploadedSize int64
	Hash         string
	Status       string
}

// byteRange represents a half-open interval [Start, End) of bytes that have
// been written to the temp file. A sorted, non-overlapping slice of these is
// maintained per session to verify full coverage at completion time.
type byteRange struct {
	Start int64
	End   int64
}

type multipartUploadState struct {
	SessionID     string              `json:"session_id"`
	FilePath      string              `json:"file_path"`
	FileName      string              `json:"file_name"`
	TotalSize     int64               `json:"total_size"`
	Hash          string              `json:"hash"`
	ClientID      string              `json:"client_id"`
	Status        string              `json:"status"`
	CreatedAt     string              `json:"created_at"`
	UpdatedAt     string              `json:"updated_at"`
	Parts         map[int]*UploadPart `json:"parts"`
	CoveredRanges []byteRange         `json:"covered_ranges"`
	UploadedSize  int64               `json:"uploaded_size"`
	ChunkCount    int64               `json:"chunk_count"`
}

type MultipartUploadSession struct {
	SessionID string
	FilePath  string
	FileName  string
	TotalSize int64
	Hash      string
	ClientID  string
	Status    string
	CreatedAt string
	UpdatedAt string

	tempFile      *os.File
	parts         map[int]*UploadPart
	partsMu       sync.RWMutex
	uploadedSize  int64
	chunkCount    int64
	closed        int32
	coveredRanges []byteRange // sorted, non-overlapping; guarded by partsMu
}

func cloneUploadParts(parts map[int]*UploadPart) map[int]*UploadPart {
	cloned := make(map[int]*UploadPart, len(parts))
	for number, part := range parts {
		copyPart := *part
		cloned[number] = &copyPart
	}
	return cloned
}

func (session *MultipartUploadSession) snapshotLocked() multipartUploadState {
	return multipartUploadState{
		SessionID: session.SessionID, FilePath: session.FilePath, FileName: session.FileName,
		TotalSize: session.TotalSize, Hash: session.Hash, ClientID: session.ClientID,
		Status: session.Status, CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
		Parts:         cloneUploadParts(session.parts),
		CoveredRanges: append([]byteRange(nil), session.coveredRanges...),
		UploadedSize:  atomic.LoadInt64(&session.uploadedSize),
		ChunkCount:    atomic.LoadInt64(&session.chunkCount),
	}
}

func (session *MultipartUploadSession) applyStateLocked(state multipartUploadState) {
	session.SessionID = state.SessionID
	session.FilePath = state.FilePath
	session.FileName = state.FileName
	session.TotalSize = state.TotalSize
	session.Hash = state.Hash
	session.ClientID = state.ClientID
	session.Status = state.Status
	session.CreatedAt = state.CreatedAt
	session.UpdatedAt = state.UpdatedAt
	session.parts = cloneUploadParts(state.Parts)
	if session.parts == nil {
		session.parts = make(map[int]*UploadPart)
	}
	session.coveredRanges = append([]byteRange(nil), state.CoveredRanges...)
	atomic.StoreInt64(&session.uploadedSize, state.UploadedSize)
	atomic.StoreInt64(&session.chunkCount, state.ChunkCount)
	if state.Status != "active" {
		atomic.StoreInt32(&session.closed, 1)
	}
}

func (s *FileTransferService) persistMultipartState(ctx context.Context, session *MultipartUploadSession) error {
	session.partsMu.RLock()
	state := session.snapshotLocked()
	session.partsMu.RUnlock()
	return s.sessionStore.Set(ctx, "multipart_upload", session.SessionID, &state, 2*time.Hour)
}

func (s *FileTransferService) loadMultipartSession(ctx context.Context, sessionID string, refresh bool) (*MultipartUploadSession, error) {
	if !refresh {
		if val, ok := s.multipartSessions.Load(sessionID); ok {
			return val.(*MultipartUploadSession), nil
		}
	}

	var state multipartUploadState
	if err := s.sessionStore.Get(ctx, "multipart_upload", sessionID, &state); err != nil {
		if !refresh {
			if val, ok := s.multipartSessions.Load(sessionID); ok {
				return val.(*MultipartUploadSession), nil
			}
		}
		return nil, fmt.Errorf("multipart upload session not found: %s", sessionID)
	}

	if val, ok := s.multipartSessions.Load(sessionID); ok {
		session := val.(*MultipartUploadSession)
		session.partsMu.Lock()
		session.applyStateLocked(state)
		session.partsMu.Unlock()
		return session, nil
	}
	if state.Status != "active" {
		restored := &MultipartUploadSession{parts: make(map[int]*UploadPart)}
		restored.applyStateLocked(state)
		actual, _ := s.multipartSessions.LoadOrStore(sessionID, restored)
		return actual.(*MultipartUploadSession), nil
	}

	if !s.acquireSessionSlot() {
		return nil, fmt.Errorf("maximum number of concurrent upload sessions reached")
	}
	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	file, err := os.OpenFile(tempPath, os.O_RDWR, 0644)
	if err != nil {
		s.releaseSessionSlot()
		return nil, fmt.Errorf("failed to reopen multipart temp file: %w", err)
	}
	restored := &MultipartUploadSession{tempFile: file, parts: make(map[int]*UploadPart)}
	restored.applyStateLocked(state)
	actual, loaded := s.multipartSessions.LoadOrStore(sessionID, restored)
	if loaded {
		file.Close()
		s.releaseSessionSlot()
		return actual.(*MultipartUploadSession), nil
	}
	return restored, nil
}

// addCoveredRange records that [start, end) has been written to the temp file
// and merges it into the sorted, non-overlapping coveredRanges slice.
// Caller must hold partsMu.
func (session *MultipartUploadSession) addCoveredRange(start, end int64) {
	if start >= end {
		return
	}
	merged := []byteRange{}
	inserted := false
	for _, r := range session.coveredRanges {
		if r.End < start {
			merged = append(merged, r)
		} else if r.Start > end {
			if !inserted {
				merged = append(merged, byteRange{Start: start, End: end})
				inserted = true
			}
			merged = append(merged, r)
		} else {
			// Overlapping or adjacent — expand the range
			if r.Start < start {
				start = r.Start
			}
			if r.End > end {
				end = r.End
			}
		}
	}
	if !inserted {
		merged = append(merged, byteRange{Start: start, End: end})
	}
	session.coveredRanges = merged
}

// isFullyCovered returns true if the covered ranges completely cover
// [0, TotalSize) with no gaps.
func (session *MultipartUploadSession) isFullyCovered() bool {
	if len(session.coveredRanges) == 0 {
		return session.TotalSize == 0
	}
	if session.coveredRanges[0].Start != 0 {
		return false
	}
	if session.coveredRanges[len(session.coveredRanges)-1].End != session.TotalSize {
		return false
	}
	for i := 1; i < len(session.coveredRanges); i++ {
		if session.coveredRanges[i].Start != session.coveredRanges[i-1].End {
			return false
		}
	}
	return true
}

type DownloadSegment struct {
	Offset int64
	Size   int
}

type DownloadSegmentResult struct {
	SegmentIndex int
	Offset       int64
	Data         []byte
	Error        error
}

func suggestedPartSize(totalSize int64) int64 {
	switch {
	case totalSize <= 100*1024*1024:
		return 8 * 1024 * 1024
	case totalSize <= 1024*1024*1024:
		return 16 * 1024 * 1024
	case totalSize <= 10*1024*1024*1024:
		return 64 * 1024 * 1024
	default:
		return 128 * 1024 * 1024
	}
}

func (s *FileTransferService) CreateMultipartUpload(filePath, fileName string, totalSize int64, clientID, hash string) (string, int64, error) {
	if !s.acquireSessionSlot() {
		return "", 0, fmt.Errorf("maximum number of concurrent upload sessions reached")
	}
	filePath = utils.NormalizePath(filePath)
	sessionID := utils.GenerateUUID()
	now := utils.GetCurrentTimestamp()

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	file, err := os.Create(tempPath)
	if err != nil {
		s.releaseSessionSlot()
		return "", 0, fmt.Errorf("failed to create temp file: %w", err)
	}

	if err := file.Truncate(totalSize); err != nil {
		file.Close()
		os.Remove(tempPath)
		s.releaseSessionSlot()
		return "", 0, fmt.Errorf("failed to pre-allocate temp file: %w", err)
	}

	partSize := suggestedPartSize(totalSize)

	session := &MultipartUploadSession{
		SessionID: sessionID,
		FilePath:  filePath,
		FileName:  fileName,
		TotalSize: totalSize,
		Hash:      hash,
		ClientID:  clientID,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
		tempFile:  file,
		parts:     make(map[int]*UploadPart),
	}

	ctx := context.Background()
	if err := s.persistMultipartState(ctx, session); err != nil {
		file.Close()
		os.Remove(tempPath)
		s.releaseSessionSlot()
		return "", 0, fmt.Errorf("failed to persist multipart upload session: %w", err)
	}
	s.multipartSessions.Store(sessionID, session)

	return sessionID, partSize, nil
}

func (s *FileTransferService) UploadPartData(sessionID string, partNumber int, offset int64, data []byte) error {
	lease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "multipart:"+sessionID, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire multipart session lock: %w", err)
	}
	defer lease.Stop()
	defer s.distLock.Unlock(context.Background(), "multipart:"+sessionID, lease.Token)
	terminal, err := s.getTransferResult(sessionID)
	if err != nil {
		return fmt.Errorf("failed to check multipart terminal state: %w", err)
	}
	if terminal != nil {
		return fmt.Errorf("multipart upload session is %s: %s", terminal.Status, sessionID)
	}

	session, err := s.loadMultipartSession(context.Background(), sessionID, true)
	if err != nil {
		return err
	}

	if atomic.LoadInt32(&session.closed) == 1 {
		return fmt.Errorf("multipart upload session is closed: %s", sessionID)
	}

	if session.Status != "active" {
		return fmt.Errorf("multipart upload session is not active: %s", sessionID)
	}

	if partNumber < 1 || partNumber > 10000 {
		return fmt.Errorf("part number too large: %d (max 10000)", partNumber)
	}

	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}

	if offset+int64(len(data)) > session.TotalSize {
		return fmt.Errorf("write beyond file size: offset=%d len=%d total=%d", offset, len(data), session.TotalSize)
	}

	session.partsMu.Lock()
	defer session.partsMu.Unlock()
	previousState := session.snapshotLocked()
	part, exists := session.parts[partNumber]
	if exists {
		if part.Offset != offset || part.Size != int64(len(data)) {
			return fmt.Errorf("part %d already uses offset=%d size=%d", partNumber, part.Offset, part.Size)
		}
	} else {
		end := offset + int64(len(data))
		for existingNumber, existing := range session.parts {
			existingEnd := existing.Offset + existing.Size
			if offset < existingEnd && end > existing.Offset {
				return fmt.Errorf("part %d overlaps part %d", partNumber, existingNumber)
			}
		}
	}

	if _, err := session.tempFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write part data: %w", err)
	}
	if err := session.tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync part data: %w", err)
	}
	if err := lease.Err(); err != nil {
		return err
	}
	partHash := sha256.Sum256(data)
	if !exists {
		part = &UploadPart{
			PartNumber:   partNumber,
			Offset:       offset,
			Size:         int64(len(data)),
			UploadedSize: int64(len(data)),
			Hash:         hex.EncodeToString(partHash[:]),
			Status:       "completed",
		}
		session.parts[partNumber] = part
		atomic.AddInt64(&session.uploadedSize, int64(len(data)))
	} else {
		part.UploadedSize = int64(len(data))
		part.Hash = hex.EncodeToString(partHash[:])
		part.Status = "completed"
	}
	session.coveredRanges = nil
	for _, uploadedPart := range session.parts {
		session.addCoveredRange(uploadedPart.Offset, uploadedPart.Offset+uploadedPart.Size)
	}
	atomic.AddInt64(&session.chunkCount, 1)
	session.UpdatedAt = utils.GetCurrentTimestamp()
	state := session.snapshotLocked()
	if err := s.sessionStore.Set(context.Background(), "multipart_upload", sessionID, &state, 2*time.Hour); err != nil {
		session.applyStateLocked(previousState)
		return fmt.Errorf("failed to persist multipart progress: %w", err)
	}

	return nil
}

func (s *FileTransferService) CompleteMultipartUpload(sessionID string) error {
	sessionLease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "multipart:"+sessionID, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire multipart session lock: %w", err)
	}
	defer sessionLease.Stop()
	defer s.distLock.Unlock(context.Background(), "multipart:"+sessionID, sessionLease.Token)
	terminal, err := s.getTransferResult(sessionID)
	if err != nil {
		return fmt.Errorf("failed to check multipart terminal state: %w", err)
	}
	if terminal != nil {
		if terminal.SessionType == "multipart" && terminal.Status == "completed" {
			return nil
		}
		return fmt.Errorf("multipart upload session is %s: %s", terminal.Status, sessionID)
	}

	session, err := s.loadMultipartSession(context.Background(), sessionID, true)
	if err != nil {
		return err
	}
	if session.Status != "active" {
		return fmt.Errorf("multipart upload session is not active: %s", sessionID)
	}
	if err := sessionLease.Err(); err != nil {
		return err
	}

	totalUploaded := atomic.LoadInt64(&session.uploadedSize)
	if totalUploaded != session.TotalSize {
		return fmt.Errorf("upload incomplete: expected %d bytes, got %d bytes", session.TotalSize, totalUploaded)
	}

	// Verify that every byte in [0, TotalSize) was actually written — the
	// uploadedSize counter alone cannot detect duplicate or overlapping uploads
	// that leave zero-filled gaps in the pre-allocated temp file.
	session.partsMu.RLock()
	fullyCovered := session.isFullyCovered()
	session.partsMu.RUnlock()
	if !fullyCovered {
		return fmt.Errorf("upload incomplete: byte range coverage has gaps or overlaps (total size %d)", session.TotalSize)
	}

	session.partsMu.RLock()
	for pn, part := range session.parts {
		if part.Status != "completed" {
			session.partsMu.RUnlock()
			return fmt.Errorf("part %d is not completed (status: %s)", pn, part.Status)
		}
	}
	session.partsMu.RUnlock()

	atomic.StoreInt32(&session.closed, 1)

	if err := session.tempFile.Sync(); err != nil {
		session.tempFile.Close()
		os.Remove(filepath.Join(s.tempDir, sessionID+".tmp"))
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err := session.tempFile.Close(); err != nil {
		os.Remove(filepath.Join(s.tempDir, sessionID+".tmp"))
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")

	computedHash, err := utils.SHA256File(tempPath)
	if err != nil {
		os.Remove(tempPath)
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return fmt.Errorf("failed to compute hash: %w", err)
	}
	if session.Hash != "" {
		if computedHash != session.Hash {
			os.Remove(tempPath)
			s.multipartSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return fmt.Errorf("hash mismatch: expected %s, got %s", session.Hash, computedHash)
		}
	}

	// Encrypt the temp file before writing to storage if encryption is enabled
	storageTempPath := tempPath
	storageHash := computedHash
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		encTempPath := tempPath + ".enc"
		if err := s.cryptoSvc.EncryptFile(tempPath, encTempPath); err != nil {
			os.Remove(tempPath)
			s.multipartSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return fmt.Errorf("failed to encrypt file: %w", err)
		}
		os.Remove(tempPath)

		storageTempPath = encTempPath
	}
	if err := sessionLease.Err(); err != nil {
		os.Remove(storageTempPath)
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return err
	}
	namespaceLease, err := s.acquireFileNamespace(session.FilePath)
	if err != nil {
		os.Remove(storageTempPath)
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return err
	}
	defer releaseNamespaceLease(namespaceLease)

	fileLease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "file:"+session.FilePath, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		os.Remove(storageTempPath)
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return fmt.Errorf("failed to acquire lock for file %s: %w", session.FilePath, err)
	}
	defer s.distLock.Unlock(context.Background(), "file:"+session.FilePath, fileLease.Token)
	defer fileLease.Stop()

	var backup storage.ReplaceBackup
	fileMetadataSvc := database.NewFileMetadataService(s.db)
	tx, err := s.db.BeginNamespaceWrite(context.Background(), namespaceLease, session.FilePath)
	if err != nil {
		os.Remove(storageTempPath)
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return fmt.Errorf("failed to begin fenced multipart transaction: %w", err)
	}
	rollbackTx := true
	defer func() {
		if rollbackTx {
			_ = tx.Rollback()
		}
	}()
	existingMeta, err := fileMetadataSvc.GetByPathTx(tx, session.FilePath)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing metadata: %w", err)
	}
	var committedMeta *database.FileMetadata
	if existingMeta != nil {
		backup, err = storage.BeginReplace(s.storage, session.FilePath)
		if err != nil {
			os.Remove(storageTempPath)
			s.multipartSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return fmt.Errorf("failed to prepare multipart commit: %w", err)
		}
	}

	if err := s.storage.WriteFromTempFile(session.FilePath, storageTempPath); err != nil {
		rollbackStorageBackup(backup, session.FilePath)
		os.Remove(storageTempPath)
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return fmt.Errorf("failed to write file from temp: %w", err)
	}
	if err := fileLease.Err(); err != nil {
		if backup != nil {
			rollbackStorageBackup(backup, session.FilePath)
		} else {
			_ = s.storage.Remove(session.FilePath)
		}
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return err
	}
	if err := namespaceLease.Err(); err != nil {
		if backup != nil {
			rollbackStorageBackup(backup, session.FilePath)
		} else {
			_ = s.storage.Remove(session.FilePath)
		}
		s.multipartSessions.Delete(sessionID)
		s.releaseSessionSlot()
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return err
	}

	now := utils.GetCurrentTimestamp()

	if existingMeta != nil {
		existingMeta.Size = session.TotalSize
		existingMeta.Hash = storageHash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		if err := fileMetadataSvc.UpdateTx(tx, existingMeta); err != nil {
			rollbackStorageBackup(backup, session.FilePath)
			os.Remove(storageTempPath)
			s.multipartSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return fmt.Errorf("failed to update file metadata: %w", err)
		}
		committedMeta = existingMeta
	} else {
		meta := &database.FileMetadata{
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
			os.Remove(storageTempPath)
			s.multipartSessions.Delete(sessionID)
			s.releaseSessionSlot()
			return fmt.Errorf("failed to create file metadata: %w", err)
		}
		committedMeta = meta
	}
	terminal = &database.TransferSessionResult{
		SessionID: sessionID, SessionType: "multipart", Status: "completed",
		FileID: committedMeta.ID, FilePath: committedMeta.Path, FileName: committedMeta.Name,
		FileHash: committedMeta.Hash, FileCreatedAt: committedMeta.CreatedAt,
		HashProvided: session.Hash != "", HashVerified: session.Hash != "",
		UploadedSize: session.TotalSize, StorageType: s.storage.StorageType(),
		UpdatedAt: now, ExpiresAt: transferResultExpiry(),
	}
	if err := database.NewTransferSessionResultService(s.db).SaveTx(tx, terminal); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return err
	}
	if err := namespaceLease.ValidateTx(tx); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return err
	}
	if err := tx.Commit(); err != nil {
		rollbackStorageWrite(s.storage, backup, session.FilePath)
		return fmt.Errorf("failed to commit multipart metadata transaction: %w", err)
	}
	rollbackTx = false
	commitStorageBackup(backup, session.FilePath)

	session.Status = "completed"
	session.UpdatedAt = now
	if err := s.persistMultipartState(context.Background(), session); err != nil {
		logger.Warn("failed to persist completed multipart session %s: %v", sessionID, err)
	}
	s.multipartSessions.Delete(sessionID)
	s.releaseSessionSlot()

	os.Remove(storageTempPath)

	return nil
}

func (s *FileTransferService) AbortMultipartUpload(sessionID string) error {
	lease, err := distributed.AcquireLockLease(context.Background(), s.distLock, "multipart:"+sessionID, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire multipart session lock: %w", err)
	}
	defer lease.Stop()
	defer s.distLock.Unlock(context.Background(), "multipart:"+sessionID, lease.Token)
	terminal, err := s.getTransferResult(sessionID)
	if err != nil {
		return fmt.Errorf("failed to check multipart terminal state: %w", err)
	}
	if terminal != nil {
		if terminal.SessionType == "multipart" && terminal.Status == "aborted" {
			return nil
		}
		return fmt.Errorf("multipart upload session is %s: %s", terminal.Status, sessionID)
	}

	session, err := s.loadMultipartSession(context.Background(), sessionID, true)
	if err != nil {
		return err
	}
	if err := lease.Err(); err != nil {
		return err
	}
	now := utils.GetCurrentTimestamp()
	terminal = &database.TransferSessionResult{
		SessionID: sessionID, SessionType: "multipart", Status: "aborted",
		FilePath: session.FilePath, FileName: session.FileName,
		UploadedSize: atomic.LoadInt64(&session.uploadedSize), StorageType: s.storage.StorageType(),
		UpdatedAt: now, ExpiresAt: transferResultExpiry(),
	}
	if err := database.NewTransferSessionResultService(s.db).Save(terminal); err != nil {
		return err
	}
	atomic.StoreInt32(&session.closed, 1)
	if session.tempFile != nil {
		session.tempFile.Close()
		session.tempFile = nil
	}
	session.Status = "aborted"
	session.UpdatedAt = now
	if err := s.persistMultipartState(context.Background(), session); err != nil {
		logger.Warn("failed to persist aborted multipart session %s: %v", sessionID, err)
	}
	s.multipartSessions.Delete(sessionID)
	s.releaseSessionSlot()

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	os.Remove(tempPath)

	return nil
}

func (s *FileTransferService) GetMultipartUploadSession(sessionID string) (*MultipartUploadSession, error) {
	session, err := s.loadMultipartSession(context.Background(), sessionID, true)
	if err != nil {
		return nil, err
	}
	if session.Status != "active" {
		return nil, fmt.Errorf("multipart upload session is %s: %s", session.Status, sessionID)
	}
	return session, nil
}

func (s *FileTransferService) GetMultipartUploadProgress(sessionID string) (uploaded int64, total int64, completedParts int) {
	session, err := s.loadMultipartSession(context.Background(), sessionID, true)
	if err != nil {
		return 0, 0, 0
	}
	session.partsMu.RLock()
	for _, part := range session.parts {
		if part.Status == "completed" {
			completedParts++
		}
	}
	session.partsMu.RUnlock()
	return atomic.LoadInt64(&session.uploadedSize), session.TotalSize, completedParts
}

func (s *FileTransferService) ParallelDownloadChunks(sessionID string, segments []DownloadSegment) []*DownloadSegmentResult {
	results := make([]*DownloadSegmentResult, len(segments))
	maxConcurrency := 8
	if len(segments) < maxConcurrency {
		maxConcurrency = len(segments)
	}

	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, seg := range segments {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, seg DownloadSegment) {
			defer wg.Done()
			defer func() { <-sem }()
			data, err := s.DownloadChunk(sessionID, seg.Size, seg.Offset)
			if err != nil {
				logger.Error("failed to download chunk at offset %d (segment %d): %v", seg.Offset, idx, err)
			}
			results[idx] = &DownloadSegmentResult{
				SegmentIndex: idx,
				Offset:       seg.Offset,
				Data:         data,
				Error:        err,
			}
		}(i, seg)
	}

	wg.Wait()
	return results
}
