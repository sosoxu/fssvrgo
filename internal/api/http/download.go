package http

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func (s *Server) handleDownload(c *gin.Context) {
	ctx := c.Request.Context()
	filePath := c.Param("path")
	if filePath == "" {
		sendError(c, http.StatusBadRequest, "File path is required")
		return
	}

	if !isValidFilePath(filePath) {
		sendError(c, http.StatusBadRequest, "Invalid file path")
		return
	}

	// Normalize once at the entry so every downstream call (including the
	// large-file streaming branch that calls store.OpenReader directly, which
	// would otherwise reject a leading "/" on MinIO) sees a clean object key.
	filePath = utils.NormalizePath(filePath)

	meta, err := s.fm.GetFileMetadata(ctx, filePath)
	if err != nil {
		sendError(c, http.StatusNotFound, "File not found")
		return
	}

	// 加密文件统一走解密临时文件路径：http.ServeContent 会基于明文临时文件
	// 正确处理 Content-Length（明文大小）和 Range 请求（基于明文偏移），
	// 修复原先大文件流式下载返回密文、Range 请求返回密文片段、
	// Content-Length 与实际返回内容大小不匹配三个问题。
	if s.cryptoSvc != nil && s.cryptoSvc.IsEnabled() {
		s.handleEncryptedDownload(c, meta, filePath)
		return
	}

	rangeHeader := c.GetHeader("Range")
	if rangeHeader != "" {
		s.handleRangeDownload(c, meta, filePath, rangeHeader)
		return
	}

	fileSize := meta.Size
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", meta.Name))
	c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
	c.Header("Accept-Ranges", "bytes")

	if fileSize > 32*1024*1024 {
		reader, err := s.store.OpenReader(ctx, filePath)
		if err != nil {
			sendError(c, http.StatusInternalServerError, "Failed to open file")
			return
		}
		defer reader.Close()
		if seekReader, ok := reader.(io.ReadSeeker); ok {
			http.ServeContent(c.Writer, c.Request, meta.Name, time.Time{}, seekReader)
		} else {
			if _, err := io.Copy(c.Writer, reader); err != nil {
				logger.Warn("failed to stream file %s: %v", filePath, err)
			}
		}
		return
	}

	data, err := s.fm.DownloadFileData(ctx, meta)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read file")
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", data)
}

// handleEncryptedDownload 将加密文件解密到临时文件后通过 http.ServeContent 响应，
// 正确处理 Content-Length（明文大小）和 Range 请求（基于明文偏移）。
// 临时文件在响应结束后清理。
//
// 解密直接从存储读取流（分块认证），只多一个明文临时文件：峰值内存是一块明文，
// 不再需要先把整个密文读进内存，也不再需要中间的密文临时文件（#119）。
func (s *Server) handleEncryptedDownload(c *gin.Context, meta *database.FileMetadata, filePath string) {
	ctx := c.Request.Context()
	tempDir := s.transferSvc.TempDir()
	sessionID := utils.GenerateUUID()
	decTempPath := filepath.Join(tempDir, sessionID+".dec")

	cleanup := func() {
		os.Remove(decTempPath)
	}

	reader, err := s.store.OpenReader(ctx, filePath)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to open encrypted file")
		return
	}
	defer reader.Close()

	decFile, err := os.Create(decTempPath)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to create temp file")
		return
	}

	if err := s.cryptoSvc.DecryptStream(decFile, reader); err != nil {
		decFile.Close()
		cleanup()
		sendError(c, http.StatusInternalServerError, "Failed to decrypt file")
		return
	}
	if err := decFile.Close(); err != nil {
		cleanup()
		sendError(c, http.StatusInternalServerError, "Failed to finalize decrypted file")
		return
	}
	defer cleanup()

	// ServeContent 处理 Range 与 Content-Length（基于明文临时文件）。
	decFile, err = os.Open(decTempPath)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to open decrypted file")
		return
	}
	defer decFile.Close()

	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", meta.Name))
	http.ServeContent(c.Writer, c.Request, meta.Name, time.Time{}, decFile)
}

func (s *Server) handleRangeDownload(c *gin.Context, meta *database.FileMetadata, filePath, rangeHeader string) {
	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
		return
	}

	var start, end int64
	var err error

	if parts[0] == "" {
		suffixLength, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil {
			sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
			return
		}
		start = meta.Size - suffixLength
		end = meta.Size - 1
	} else {
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
			return
		}
		if parts[1] == "" {
			end = meta.Size - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				sendError(c, http.StatusRequestedRangeNotSatisfiable, "Invalid range")
				return
			}
		}
	}

	if start < 0 || start >= meta.Size || end >= meta.Size || start > end {
		sendError(c, http.StatusRequestedRangeNotSatisfiable, "Range out of bounds")
		return
	}

	chunkSize := int(end - start + 1)
	if chunkSize > 32*1024*1024 {
		sendError(c, http.StatusRequestedRangeNotSatisfiable, "Range too large, maximum 32MB per request")
		return
	}

	data, err := s.fm.DownloadFileDataAt(c.Request.Context(), meta, chunkSize, start)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to read file range")
		return
	}

	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.Size))
	c.Header("Content-Length", strconv.Itoa(len(data)))
	c.Header("Accept-Ranges", "bytes")
	c.Data(http.StatusPartialContent, "application/octet-stream", data)
}
