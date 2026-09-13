package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/service/apikey"
)

// apiKeyView is the API representation of a stored key. The key hash is never
// returned; the plaintext is returned exactly once, by handleCreateApiKey.
func apiKeyView(k *database.ApiKey) gin.H {
	return gin.H{
		"id":           k.ID,
		"name":         k.Name,
		"description":  k.Description,
		"permissions":  k.Permissions,
		"created_at":   k.CreatedAt,
		"expires_at":   k.ExpiresAt,
		"last_used_at": k.LastUsedAt,
		"is_active":    k.IsActive,
	}
}

// apiKeyError maps the service's sentinel errors to HTTP responses.
func apiKeyError(c *gin.Context, err error, fallback string) {
	switch {
	case errors.Is(err, apikey.ErrInvalidPermissions):
		sendError(c, http.StatusBadRequest, "Invalid permissions: allowed values are 'admin', 'user', or comma-separated 'user:read,user:write'")
	case errors.Is(err, apikey.ErrNotFound):
		sendError(c, http.StatusNotFound, "API key not found")
	default:
		sendError(c, http.StatusInternalServerError, fallback)
	}
}

func (s *Server) handleCreateApiKey(c *gin.Context) {
	var req struct {
		Name        string `json:"name" binding:"required"`
		Description string `json:"description"`
		Permissions string `json:"permissions"`
		ExpiresAt   string `json:"expires_at"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Name is required")
		return
	}

	created, err := s.apiKeySvc.Create(c.Request.Context(), apikey.CreateParams{
		Name:        req.Name,
		Description: req.Description,
		Permissions: req.Permissions,
		ExpiresAt:   req.ExpiresAt,
	})
	if err != nil {
		apiKeyError(c, err, "Failed to create API key")
		return
	}

	s.auditLog("create_api_key", created.Key.ID, c, true, fmt.Sprintf("name=%s", created.Key.Name))
	view := apiKeyView(created.Key)
	view["key"] = created.Plaintext
	c.JSON(http.StatusCreated, view)
}

func (s *Server) handleListApiKeys(c *gin.Context) {
	activeOnly := c.Query("active_only") == "true"
	page, pageSize := parsePageParams(c, s.maxPageSize)

	keys, err := s.apiKeySvc.List(c.Request.Context(), activeOnly, page, pageSize)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to list API keys")
		return
	}

	result := make([]gin.H, 0, len(keys))
	for i := range keys {
		result = append(result, apiKeyView(&keys[i]))
	}

	c.JSON(http.StatusOK, gin.H{
		"keys":      result,
		"page":      page,
		"page_size": pageSize,
	})
}

func (s *Server) handleGetApiKey(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		sendError(c, http.StatusBadRequest, "API key ID is required")
		return
	}

	key, err := s.apiKeySvc.Get(c.Request.Context(), id)
	if err != nil {
		apiKeyError(c, err, "Failed to get API key")
		return
	}

	c.JSON(http.StatusOK, apiKeyView(key))
}

func (s *Server) handleUpdateApiKey(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		sendError(c, http.StatusBadRequest, "API key ID is required")
		return
	}

	var req struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
		IsActive    *bool   `json:"is_active"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	key, err := s.apiKeySvc.Update(c.Request.Context(), id, apikey.UpdateParams{
		Name:        req.Name,
		Description: req.Description,
		IsActive:    req.IsActive,
	})
	if err != nil {
		apiKeyError(c, err, "Failed to update API key")
		return
	}

	s.auditLog("update_api_key", id, c, true, fmt.Sprintf("name=%s is_active=%v", key.Name, key.IsActive))
	c.JSON(http.StatusOK, apiKeyView(key))
}

func (s *Server) handleDeleteApiKey(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		sendError(c, http.StatusBadRequest, "API key ID is required")
		return
	}

	key, err := s.apiKeySvc.Delete(c.Request.Context(), id)
	if err != nil {
		apiKeyError(c, err, "Failed to delete API key")
		return
	}

	s.auditLog("delete_api_key", id, c, true, fmt.Sprintf("name=%s", key.Name))
	c.JSON(http.StatusOK, gin.H{"message": "API key deleted successfully"})
}
