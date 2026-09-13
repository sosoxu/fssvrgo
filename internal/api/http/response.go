package http

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func sendError(c *gin.Context, status int, msg string) {
	c.JSON(status, gin.H{"error": msg})
}

// sendInternalError logs the underlying error (with request context) and
// returns a generic message to the client. Use this instead of
// sendError(c, 500, err.Error()) so internal details (paths, SQL errors,
// stack hints) are not leaked to API consumers.
func sendInternalError(c *gin.Context, err error, publicMsg string) {
	if publicMsg == "" {
		publicMsg = "Internal server error"
	}
	reqID := c.GetString("request_id")
	logger.Error("internal error: req_id=%s method=%s path=%s err=%v",
		reqID, c.Request.Method, c.Request.URL.Path, err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": publicMsg})
}

// parsePageParams reads and clamps the page/page_size query parameters shared
// by every paginated endpoint.
func parsePageParams(c *gin.Context, maxPageSize int) (page, pageSize int) {
	page, _ = strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ = strconv.Atoi(c.DefaultQuery("page_size", "20"))

	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if page < 1 {
		page = 1
	}
	return page, pageSize
}

func isValidFileName(name string) bool {
	return utils.IsValidFileName(name)
}

func isValidFilePath(p string) bool {
	return utils.IsValidFilePath(p)
}
