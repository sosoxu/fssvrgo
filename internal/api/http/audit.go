package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/service/auditlog"
)

// auditLog records one operation against the audit trail. It never blocks the
// request: the service writes the process log immediately and submits the DB
// row to an asynchronous writer.
func (s *Server) auditLog(operation, resourcePath string, c *gin.Context, success bool, details string) {
	if s.auditSvc == nil {
		return
	}

	// The identifier is a fixed marker rather than the credential itself; the
	// authenticated user (when there is one) is recorded separately by the
	// auth middleware in the "user" context value.
	userIdentifier := "anonymous"
	if c.GetHeader("X-API-Key") != "" {
		userIdentifier = "api-key:***"
	} else if strings.HasPrefix(c.GetHeader("Authorization"), "Bearer ") {
		userIdentifier = "bearer:***"
	}

	s.auditSvc.Record(auditlog.Entry{
		Operation:      operation,
		ResourcePath:   resourcePath,
		UserIdentifier: userIdentifier,
		ClientIP:       c.ClientIP(),
		UserAgent:      c.GetHeader("User-Agent"),
		RequestID:      c.GetString("request_id"),
		Success:        success,
		Details:        details,
	})
}

func (s *Server) handleListAuditLogs(c *gin.Context) {
	if s.auditSvc == nil {
		sendError(c, http.StatusServiceUnavailable, "Database not available")
		return
	}

	operation := c.Query("operation")
	resourcePath := c.Query("resource_path")
	page, pageSize := parsePageParams(c, s.maxPageSize)

	// The service flushes pending async writes before querying, so a caller
	// that just performed an operation observes it in the result instead of
	// racing the writer's flush interval.
	logs, err := s.auditSvc.List(c.Request.Context(), operation, resourcePath, page, pageSize)
	if err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"logs":      logs,
		"page":      page,
		"page_size": pageSize,
	})
}
