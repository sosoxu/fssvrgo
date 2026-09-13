package http

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// bindUploadInit parses and validates the common body of the resumable and
// multipart upload-creation endpoints. On failure it writes the error response
// and returns ok=false.
func (s *Server) bindUploadInit(c *gin.Context) (filePath, fileName, hash string, totalSize int64, ok bool) {
	var req struct {
		FilePath  string `json:"file_path" binding:"required"`
		FileName  string `json:"file_name" binding:"required"`
		TotalSize int64  `json:"total_size" binding:"required"`
		Hash      string `json:"hash"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return "", "", "", 0, false
	}

	if !isValidFileName(req.FileName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return "", "", "", 0, false
	}

	if !isValidFilePath(req.FilePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return "", "", "", 0, false
	}

	if req.TotalSize <= 0 {
		sendError(c, http.StatusBadRequest, "Total size must be positive")
		return "", "", "", 0, false
	}

	if req.TotalSize > s.maxUploadSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return "", "", "", 0, false
	}

	return req.FilePath, req.FileName, req.Hash, req.TotalSize, true
}

// readChunkPart reads a "data" multipart field capped at maxChunkSize. label is
// the capitalized resource name used in error messages ("Chunk" or "Part"). On
// failure it writes the error response and returns ok=false.
func (s *Server) readChunkPart(c *gin.Context, label string) (data []byte, ok bool) {
	field := strings.ToLower(label)
	file, _, err := c.Request.FormFile("data")
	if err != nil {
		sendError(c, http.StatusBadRequest, "No "+field+" data provided")
		return nil, false
	}
	defer file.Close()

	data, err = io.ReadAll(io.LimitReader(file, s.maxChunkSize+1))
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read "+field+" data")
		return nil, false
	}

	if int64(len(data)) > s.maxChunkSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("%s size exceeds maximum allowed size of %d MB", label, s.config.MaxChunkSizeMB))
		return nil, false
	}
	return data, true
}

func (s *Server) handleCreateUploadSession(c *gin.Context) {
	filePath, fileName, hash, totalSize, ok := s.bindUploadInit(c)
	if !ok {
		return
	}

	clientID := c.ClientIP()
	sessionID, err := s.transferSvc.CreateUploadSession(c.Request.Context(), filePath, fileName, totalSize, clientID, hash)
	if err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("create_upload_session", filePath, c, true, fmt.Sprintf("session_id=%s", sessionID))
	c.JSON(http.StatusCreated, gin.H{"session_id": sessionID})
}

func (s *Server) handleUploadChunk(c *gin.Context) {
	sessionID := c.Param("id")

	data, ok := s.readChunkPart(c, "Chunk")
	if !ok {
		return
	}

	offsetStr := c.PostForm("offset")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		sendError(c, http.StatusBadRequest, "Invalid offset value")
		return
	}

	if err := s.transferSvc.UploadChunk(c.Request.Context(), sessionID, data, offset); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Chunk uploaded successfully"})
}

func (s *Server) handleGetUploadProgress(c *gin.Context) {
	sessionID := c.Param("id")
	progress := s.transferSvc.GetUploadProgress(sessionID)
	c.JSON(http.StatusOK, gin.H{"uploaded_bytes": progress})
}

func (s *Server) handleCompleteUpload(c *gin.Context) {
	sessionID := c.Param("id")
	result, err := s.transferSvc.CompleteUpload(c.Request.Context(), sessionID)
	if err != nil {
		sendInternalError(c, err, "Failed to complete upload")
		return
	}

	s.auditLog("complete_upload", "", c, true, fmt.Sprintf("session_id=%s hash_provided=%v hash_verified=%v", sessionID, result.HashProvided, result.HashVerified))
	// Explicitly report hash verification status so callers can tell whether
	// the uploaded content was integrity-checked (#29: hash verification is
	// optional but the outcome is now explicit in the response).
	c.JSON(http.StatusOK, gin.H{
		"message":        "Upload completed successfully",
		"file_id":        result.FileID,
		"hash_provided":  result.HashProvided,
		"hash_verified":  result.HashVerified,
		"uploaded_bytes": result.UploadedSize,
		"storage_type":   result.StorageType,
	})
}

func (s *Server) handleAbortUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.AbortUpload(c.Request.Context(), sessionID); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Upload aborted"})
}

func (s *Server) handleCreateMultipartUpload(c *gin.Context) {
	filePath, fileName, hash, totalSize, ok := s.bindUploadInit(c)
	if !ok {
		return
	}

	clientID := c.ClientIP()
	sessionID, partSize, err := s.transferSvc.CreateMultipartUpload(c.Request.Context(), filePath, fileName, totalSize, clientID, hash)
	if err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("create_multipart_upload", filePath, c, true, fmt.Sprintf("session_id=%s part_size=%d", sessionID, partSize))
	c.JSON(http.StatusCreated, gin.H{
		"session_id": sessionID,
		"part_size":  partSize,
	})
}

func (s *Server) handleUploadPart(c *gin.Context) {
	sessionID := c.Param("id")
	partNumber, err := strconv.Atoi(c.Param("partNumber"))
	if err != nil || partNumber < 1 {
		sendError(c, http.StatusBadRequest, "Part number must be a positive integer")
		return
	}

	if partNumber > 10000 {
		sendError(c, http.StatusBadRequest, "Part number exceeds maximum (10000)")
		return
	}

	data, ok := s.readChunkPart(c, "Part")
	if !ok {
		return
	}

	offsetStr := c.PostForm("offset")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		sendError(c, http.StatusBadRequest, "Invalid offset value")
		return
	}

	if err := s.transferSvc.UploadPartData(c.Request.Context(), sessionID, partNumber, offset, data); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":     "Part uploaded successfully",
		"part_number": partNumber,
		"size":        len(data),
	})
}

func (s *Server) handleGetMultipartUploadStatus(c *gin.Context) {
	sessionID := c.Param("id")
	uploaded, total, completedParts := s.transferSvc.GetMultipartUploadProgress(sessionID)
	c.JSON(http.StatusOK, gin.H{
		"uploaded_bytes":  uploaded,
		"total_bytes":     total,
		"completed_parts": completedParts,
	})
}

func (s *Server) handleCompleteMultipartUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.CompleteMultipartUpload(c.Request.Context(), sessionID); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("complete_multipart_upload", "", c, true, fmt.Sprintf("session_id=%s", sessionID))
	c.JSON(http.StatusOK, gin.H{"message": "Multipart upload completed successfully"})
}

func (s *Server) handleAbortMultipartUpload(c *gin.Context) {
	sessionID := c.Param("id")
	if err := s.transferSvc.AbortMultipartUpload(c.Request.Context(), sessionID); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Multipart upload aborted"})
}
