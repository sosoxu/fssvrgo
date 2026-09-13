package http

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/service/filelist"
)

// smallUploadThreshold is the cutoff below which handleUpload buffers the
// body in memory and calls FileManager.UploadFile (which hands the storage
// layer a bytes.Reader — an io.ReadSeeker). For MinIO this triggers a single
// PutObject instead of multipart upload (3 HTTP round-trips), eliminating the
// ~47x write throughput penalty observed for small objects. Files above this
// threshold continue to stream via UploadFileFromReader to bound peak memory.
//
// Kept small on purpose: the concurrency semaphore holds up to Workers*4
// in-flight uploads, so peak memory ≈ smallUploadThreshold * Workers * 4.
// At 1MB this is 32MB * Workers (e.g. 3.2GB at Workers=100) — comparable to
// the streaming path's buffer footprint. A larger cutoff (e.g. 32MB) would
// allow 12.8GB at Workers=100 and reintroduce the OOM risk (#33) that the
// streaming path was designed to avoid. The 1KB benchmark target is well
// below this cutoff, so the MinIO optimization still applies.
const smallUploadThreshold = 1 * 1024 * 1024 // 1 MB

func (s *Server) handleUpload(c *gin.Context) {
	ctx := c.Request.Context()

	file, header, err := c.Request.FormFile("file")
	if err != nil {
		sendError(c, http.StatusBadRequest, "No file provided")
		return
	}
	defer file.Close()

	if header.Size > s.maxUploadSize {
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return
	}

	fileName := header.Filename
	if !isValidFileName(fileName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return
	}

	filePath := c.PostForm("path")
	if filePath == "" {
		filePath = "/" + fileName
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	// Encryption requires the full plaintext in memory (AES-GCM authenticates
	// the whole message), so for the encrypted path we still read the body —
	// but bounded by maxUploadSize. For the non-encrypted path we stream the
	// multipart body straight into storage via UploadFileFromReader so peak
	// memory stays bounded by the copy buffer instead of the file size,
	// avoiding OOM under concurrent large uploads (#33).
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		data, err := io.ReadAll(io.LimitReader(file, s.maxUploadSize+1))
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to read file data")
			return
		}
		if int64(len(data)) > s.maxUploadSize {
			sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
			return
		}
		encrypted, err := s.cryptoSvc.Encrypt(string(data))
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to encrypt file")
			return
		}
		meta, err := s.fm.UploadFile(ctx, filePath, []byte(encrypted))
		if err != nil {
			sendInternalError(c, err, "Failed to upload file")
			return
		}
		s.auditLog("upload", filePath, c, true, "")
		c.JSON(http.StatusCreated, meta)
		return
	}

	// Non-encrypted path. Two strategies based on size:
	//  - Small files (<= smallUploadThreshold): buffer in memory and call
	//    UploadFile so the storage layer receives a bytes.Reader (io.ReadSeeker).
	//    This is critical for MinIO: a Seeker makes minio-go issue a single
	//    PutObject (1 HTTP round-trip) instead of multipart upload (3
	//    round-trips), which is ~47x faster for 1KB objects. The TeeReader
	//    used by UploadFileFromReader is not a Seeker, forcing multipart.
	//  - Large files: stream via UploadFileFromReader to keep peak memory
	//    bounded by the copy buffer (avoids OOM under concurrent large
	//    uploads, #33).
	if header.Size >= 0 && header.Size <= smallUploadThreshold {
		data, err := io.ReadAll(io.LimitReader(file, smallUploadThreshold+1))
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to read file data")
			return
		}
		if int64(len(data)) > smallUploadThreshold {
			// header.Size was forged or the stream ran past the threshold; the
			// streaming path will re-check against maxUploadSize, so report
			// the actual limiting threshold here rather than maxUploadSizeMB
			// to avoid a misleading message.
			sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("Inline upload size exceeds %d bytes; use streaming upload for larger files", smallUploadThreshold))
			return
		}
		meta, err := s.fm.UploadFile(ctx, filePath, data)
		if err != nil {
			sendInternalError(c, err, "Failed to upload file")
			return
		}
		s.auditLog("upload", filePath, c, true, "")
		c.JSON(http.StatusCreated, meta)
		return
	}

	// Large file path: stream into storage. Cap the reader so a
	// misbehaving client cannot stream past the limit; the actual size is
	// verified against the stat returned after the upload.
	limited := io.LimitReader(file, s.maxUploadSize+1)
	meta, err := s.fm.UploadFileFromReader(ctx, filePath, limited)
	if err != nil {
		sendInternalError(c, err, "Failed to upload file")
		return
	}
	if meta != nil && meta.Size > s.maxUploadSize {
		_ = s.fm.DeleteFile(ctx, filePath)
		sendError(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("File size exceeds maximum allowed size of %d MB", s.config.MaxUploadSizeMB))
		return
	}

	s.auditLog("upload", filePath, c, true, "")
	c.JSON(http.StatusCreated, meta)
}

func (s *Server) handleList(c *gin.Context) {
	ctx := c.Request.Context()
	dirPath := c.Query("path")
	sortBy := c.DefaultQuery("sort_by", "name")
	sortOrder := c.DefaultQuery("sort_order", "asc")
	recursive := c.Query("recursive") == "true"
	// include_total defaults to false: the COUNT is O(N) on large directories,
	// and most clients only need HasMore for pagination. Pass ?include_total=true
	// when the exact total is required (e.g. a UI showing "N items").
	includeTotal := c.Query("include_total") == "true"

	if dirPath != "" && !isValidFilePath(dirPath) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
		return
	}

	page, pageSize := parsePageParams(c, s.maxPageSize)

	var result *filelist.FileListResult
	var err error
	if includeTotal {
		result, err = s.flSvc.ListFilesWithTotal(ctx, dirPath, recursive, page, pageSize, sortBy, sortOrder)
	} else {
		result, err = s.flSvc.ListFiles(ctx, dirPath, recursive, page, pageSize, sortBy, sortOrder)
	}
	if err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	c.JSON(http.StatusOK, result)
}

func (s *Server) handleDelete(c *gin.Context) {
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "File path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	if err := s.fm.DeleteFile(c.Request.Context(), filePath); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("delete", filePath, c, true, "")
	c.JSON(http.StatusOK, gin.H{"message": "File deleted successfully"})
}

func (s *Server) handleRename(c *gin.Context) {
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "File path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	newName := c.PostForm("new_name")
	if newName == "" {
		var req struct {
			NewName string `json:"new_name"`
		}
		if err := c.ShouldBindJSON(&req); err == nil && req.NewName != "" {
			newName = req.NewName
		}
	}

	if newName == "" {
		sendError(c, http.StatusBadRequest, "New name is required")
		return
	}

	if !isValidFileName(newName) {
		sendError(c, http.StatusBadRequest, "Invalid file name")
		return
	}

	if err := s.fm.RenameFile(c.Request.Context(), filePath, newName); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("rename", filePath, c, true, fmt.Sprintf("new_name=%s", newName))
	c.JSON(http.StatusOK, gin.H{"message": "File renamed successfully"})
}

func (s *Server) handleGetMetadata(c *gin.Context) {
	ctx := c.Request.Context()
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "Path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	cacheKey := fmt.Sprintf("metadata:%s", filePath)
	if s.cacheSvc != nil {
		if cached, ok := s.cacheSvc.Get(ctx, cacheKey); ok {
			if metaMap, ok := cached.(map[string]interface{}); ok {
				c.JSON(http.StatusOK, metaMap)
				return
			}
		}
	}

	meta, err := s.fm.GetFileMetadata(ctx, filePath)
	if err == nil {
		if s.cacheSvc != nil {
			s.cacheSvc.Set(ctx, cacheKey, gin.H{"type": "file", "metadata": meta})
		}
		c.JSON(http.StatusOK, gin.H{"type": "file", "metadata": meta})
		return
	}

	dirMeta, err := s.dirSvc.GetDirectoryMetadata(ctx, filePath)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"type": "directory", "metadata": dirMeta})
		return
	}

	sendError(c, http.StatusNotFound, "Path not found")
}
