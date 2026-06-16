package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/logger"
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

type MultipartUploadSession struct {
	SessionID    string
	FilePath     string
	FileName     string
	TotalSize    int64
	Hash         string
	ClientID     string
	Status       string
	CreatedAt    string
	UpdatedAt    string

	tempFile     *os.File
	parts        map[int]*UploadPart
	partsMu      sync.RWMutex
	uploadedSize int64
	chunkCount   int64
	closed       int32
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
	filePath = utils.NormalizePath(filePath)
	sessionID := utils.GenerateUUID()
	now := utils.GetCurrentTimestamp()

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	file, err := os.Create(tempPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp file: %w", err)
	}

	if err := file.Truncate(totalSize); err != nil {
		file.Close()
		os.Remove(tempPath)
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

	s.multipartSessions.Store(sessionID, session)

	ctx := context.Background()
	if err := s.sessionStore.Set(ctx, "multipart_upload", sessionID, map[string]interface{}{
		"session_id": sessionID,
		"file_path":  filePath,
		"file_name":  fileName,
		"total_size": totalSize,
		"hash":       hash,
		"client_id":  clientID,
		"status":     "active",
		"created_at": now,
	}, 2*time.Hour); err != nil {
		logger.Warn("failed to store multipart upload session in Redis: %v", err)
	}

	return sessionID, partSize, nil
}

func (s *FileTransferService) UploadPartData(sessionID string, partNumber int, offset int64, data []byte) error {
	val, ok := s.multipartSessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("multipart upload session not found: %s", sessionID)
	}

	session := val.(*MultipartUploadSession)

	if atomic.LoadInt32(&session.closed) == 1 {
		return fmt.Errorf("multipart upload session is closed: %s", sessionID)
	}

	if session.Status != "active" {
		return fmt.Errorf("multipart upload session is not active: %s", sessionID)
	}

	if partNumber > 10000 {
		return fmt.Errorf("part number too large: %d (max 10000)", partNumber)
	}

	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}

	if offset+int64(len(data)) > session.TotalSize {
		return fmt.Errorf("write beyond file size: offset=%d len=%d total=%d", offset, len(data), session.TotalSize)
	}

	if _, err := session.tempFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write part data: %w", err)
	}

	partHash := sha256.Sum256(data)

	session.partsMu.Lock()
	part, exists := session.parts[partNumber]
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
	} else {
		part.UploadedSize += int64(len(data))
		part.Hash = hex.EncodeToString(partHash[:])
		if part.UploadedSize >= part.Size {
			part.Status = "completed"
		}
	}
	session.partsMu.Unlock()

	atomic.AddInt64(&session.uploadedSize, int64(len(data)))
	atomic.AddInt64(&session.chunkCount, 1)

	chunkNum := atomic.LoadInt64(&session.chunkCount)
	if chunkNum%8 == 0 {
		session.UpdatedAt = utils.GetCurrentTimestamp()
	}

	return nil
}

func (s *FileTransferService) CompleteMultipartUpload(sessionID string) error {
	val, ok := s.multipartSessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("multipart upload session not found: %s", sessionID)
	}

	session := val.(*MultipartUploadSession)

	totalUploaded := atomic.LoadInt64(&session.uploadedSize)
	if totalUploaded != session.TotalSize {
		return fmt.Errorf("upload incomplete: expected %d bytes, got %d bytes", session.TotalSize, totalUploaded)
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
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err := session.tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")

	if session.Hash != "" {
		computedHash, err := utils.SHA256File(tempPath)
		if err != nil {
			return fmt.Errorf("failed to compute hash: %w", err)
		}
		if computedHash != session.Hash {
			os.Remove(tempPath)
			s.multipartSessions.Delete(sessionID)
			return fmt.Errorf("hash mismatch: expected %s, got %s", session.Hash, computedHash)
		}
	}

	token, err := distributed.AcquireLock(context.Background(), s.distLock, "file:"+session.FilePath, 10*time.Second, 30, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire lock for file %s: %w", session.FilePath, err)
	}
	defer s.distLock.Unlock(context.Background(), "file:"+session.FilePath, token)

	if err := s.storage.WriteFromTempFile(session.FilePath, tempPath); err != nil {
		os.Remove(tempPath)
		s.multipartSessions.Delete(sessionID)
		return fmt.Errorf("failed to write file from temp: %w", err)
	}

	now := utils.GetCurrentTimestamp()

	existingMeta, _ := database.NewFileMetadataService(s.db).GetByPath(session.FilePath)

	if existingMeta != nil {
		existingMeta.Size = session.TotalSize
		existingMeta.Hash = session.Hash
		existingMeta.UpdatedAt = now
		existingMeta.IsDeleted = false
		if err := database.NewFileMetadataService(s.db).Update(existingMeta); err != nil {
			return fmt.Errorf("failed to update file metadata: %w", err)
		}
	} else {
		meta := &database.FileMetadata{
			ID:              utils.GenerateUUID(),
			Path:            session.FilePath,
			Name:            session.FileName,
			Size:            session.TotalSize,
			Hash:            session.Hash,
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

	session.Status = "completed"
	session.UpdatedAt = now
	s.multipartSessions.Delete(sessionID)

	os.Remove(tempPath)

	ctx := context.Background()
	s.sessionStore.Delete(ctx, "multipart_upload", sessionID)

	return nil
}

func (s *FileTransferService) AbortMultipartUpload(sessionID string) error {
	val, ok := s.multipartSessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("multipart upload session not found: %s", sessionID)
	}

	session := val.(*MultipartUploadSession)
	atomic.StoreInt32(&session.closed, 1)
	session.tempFile.Close()
	session.Status = "aborted"
	session.UpdatedAt = utils.GetCurrentTimestamp()
	s.multipartSessions.Delete(sessionID)

	tempPath := filepath.Join(s.tempDir, sessionID+".tmp")
	os.Remove(tempPath)

	ctx := context.Background()
	s.sessionStore.Delete(ctx, "multipart_upload", sessionID)

	return nil
}

func (s *FileTransferService) GetMultipartUploadSession(sessionID string) (*MultipartUploadSession, error) {
	val, ok := s.multipartSessions.Load(sessionID)
	if !ok {
		return nil, fmt.Errorf("multipart upload session not found: %s", sessionID)
	}
	return val.(*MultipartUploadSession), nil
}

func (s *FileTransferService) GetMultipartUploadProgress(sessionID string) (uploaded int64, total int64, completedParts int) {
	val, ok := s.multipartSessions.Load(sessionID)
	if !ok {
		return 0, 0, 0
	}
	session := val.(*MultipartUploadSession)
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
