package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/auth"
)

func (s *Server) handleGenerateToken(c *gin.Context) {
	if s.authSvc == nil || s.authSvc.GetJWTService() == nil {
		sendError(c, http.StatusServiceUnavailable, "JWT authentication not configured")
		return
	}

	var req struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Resolve the caller's identity from the auth middleware. The caller cannot
	// self-elevate: only an admin may issue admin tokens or impersonate another user.
	callerRole := "user"
	callerID := req.UserID
	if val, exists := c.Get("user"); exists {
		if user, ok := val.(*auth.User); ok && user != nil {
			callerRole = user.Role
			if callerRole == "admin" {
				// Admins can specify a different user_id; default to their own.
				if callerID == "" {
					callerID = user.ID
				}
			} else {
				// Non-admins must use their own authenticated identity.
				callerID = user.ID
			}
		}
	}

	if callerID == "" {
		sendError(c, http.StatusBadRequest, "user_id is required")
		return
	}

	// Non-admin callers always receive a "user" token regardless of what they request.
	grantRole := "user"
	if callerRole == "admin" && (req.Role == "admin" || req.Role == "user") {
		grantRole = req.Role
	}

	tokenPair, err := s.authSvc.GetJWTService().GenerateTokenPair(callerID, grantRole)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "Failed to generate token")
		return
	}

	s.auditLog("issue_token", callerID, c, true, fmt.Sprintf("role=%s", grantRole))
	c.JSON(http.StatusOK, tokenPair)
}

func (s *Server) handleRefreshToken(c *gin.Context) {
	if s.authSvc == nil || s.authSvc.GetJWTService() == nil {
		sendError(c, http.StatusServiceUnavailable, "JWT authentication not configured")
		return
	}

	var req struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Invalid request: refresh_token is required")
		return
	}

	tokenPair, err := s.authSvc.GetJWTService().RefreshToken(req.RefreshToken)
	if err != nil {
		sendError(c, http.StatusUnauthorized, "Invalid or expired refresh token")
		return
	}

	c.JSON(http.StatusOK, tokenPair)
}
