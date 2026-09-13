package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/auth"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func (s *Server) corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// Only emit Access-Control-Allow-Origin when an explicit allowlist is
		// configured. An empty cors_allowed_origins (the safe default) means
		// no CORS headers are sent, so browsers will block cross-origin
		// requests — which is the desired posture for a production file
		// service unless the operator explicitly opts in.
		allowedOrigin := ""
		if s.corsOrigins == "*" {
			allowedOrigin = "*"
		} else if s.corsOrigins != "" && origin != "" {
			for _, o := range strings.Split(s.corsOrigins, ",") {
				if strings.TrimSpace(o) == origin {
					allowedOrigin = origin
					break
				}
			}
		}

		if allowedOrigin != "" {
			c.Header("Access-Control-Allow-Origin", allowedOrigin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			c.Header("Access-Control-Max-Age", "86400")
			if allowedOrigin != "*" {
				c.Header("Access-Control-Allow-Credentials", "true")
			}
		}

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func (s *Server) concurrencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case s.concurrencySem <- struct{}{}:
			defer func() { <-s.concurrencySem }()
			c.Next()
		default:
			sendError(c, http.StatusServiceUnavailable, "Server is busy, please try again later")
			c.Abort()
		}
	}
}

// requestIDMiddleware ensures every request carries an X-Request-Id. If the
// client supplied one it is honored (after trimming to a sane length);
// otherwise a new UUID is generated. The id is stored in the gin.Context
// ("request_id") and echoed back on the response so logs, audit entries, and
// client-side correlation all share the same identifier.
func (s *Server) requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := strings.TrimSpace(c.GetHeader("X-Request-Id"))
		if reqID == "" || len(reqID) > 128 {
			reqID = utils.GenerateUUID()
		}
		c.Set("request_id", reqID)
		c.Header("X-Request-Id", reqID)
		c.Next()
	}
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		ctx := c.Request.Context()

		if s.authSvc.IsRateLimited(clientIP) {
			sendError(c, http.StatusTooManyRequests, "Too many authentication failures")
			c.Abort()
			return
		}

		if !s.authSvc.ValidateApiKey(ctx, "") {
			authHeader := c.GetHeader("Authorization")
			apiKeyHeader := c.GetHeader("X-API-Key")

			var apiKey string
			if apiKeyHeader != "" {
				apiKey = apiKeyHeader
			} else if strings.HasPrefix(authHeader, "Bearer ") {
				apiKey = strings.TrimPrefix(authHeader, "Bearer ")
			} else if strings.HasPrefix(authHeader, "Api-Key ") {
				apiKey = strings.TrimPrefix(authHeader, "Api-Key ")
			}

			if apiKey == "" {
				s.authSvc.RecordAuthFailure(clientIP)
				sendError(c, http.StatusUnauthorized, "Authentication required")
				c.Abort()
				return
			}

			if !s.authSvc.ValidateApiKey(ctx, apiKey) {
				// API key validation failed — try JWT as a separate authentication path.
				jwtSvc := s.authSvc.GetJWTService()
				if jwtSvc != nil {
					claims, err := jwtSvc.ValidateToken(apiKey)
					if err == nil && claims != nil && (claims.TokenType == "" || claims.TokenType == "access") {
						s.authSvc.ClearAuthFailure(clientIP)
						c.Set("user", &auth.User{
							ID:      claims.UserID,
							Name:    claims.UserID,
							Role:    claims.Role,
							Enabled: true,
						})
						c.Set("api_key", apiKey)
						c.Next()
						return
					}
				}
				s.authSvc.RecordAuthFailure(clientIP)
				sendError(c, http.StatusUnauthorized, "Invalid API key")
				c.Abort()
				return
			}

			s.authSvc.ClearAuthFailure(clientIP)

			// Store the resolved user in the context for downstream RBAC checks.
			if user := s.authSvc.GetUserByApiKey(ctx, apiKey); user != nil {
				c.Set("user", user)
				c.Set("api_key", apiKey)
			}
		}

		c.Next()
	}
}

// requirePermission returns a middleware that enforces RBAC on the given resource/action.
func (s *Server) requirePermission(resource, action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.authSvc.ValidateApiKey(c.Request.Context(), "") {
			// Auth is enabled; resolve the user from the context (set by authMiddleware).
			val, exists := c.Get("user")
			if !exists {
				sendError(c, http.StatusForbidden, "Permission denied: no authenticated user")
				c.Abort()
				return
			}
			user, ok := val.(*auth.User)
			if !ok || user == nil {
				sendError(c, http.StatusForbidden, "Permission denied")
				c.Abort()
				return
			}
			if user.Role == "admin" {
				c.Next()
				return
			}
			if !(user.Role == "user" && resource == "files" && (action == "read" || action == "write")) {
				sendError(c, http.StatusForbidden, "Permission denied")
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

// requireAdmin returns a middleware that restricts access to admin users only.
func (s *Server) requireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.authSvc.ValidateApiKey(c.Request.Context(), "") {
			val, exists := c.Get("user")
			if !exists {
				sendError(c, http.StatusForbidden, "Admin permission required")
				c.Abort()
				return
			}
			user, ok := val.(*auth.User)
			if !ok || user == nil || user.Role != "admin" {
				sendError(c, http.StatusForbidden, "Admin permission required")
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

func (s *Server) metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.metricsSvc != nil {
			start := time.Now()
			c.Next()
			duration := time.Since(start)
			s.metricsSvc.RecordHTTPRequest(c.Request.Method, c.FullPath(), c.Writer.Status(), duration)
		} else {
			c.Next()
		}
	}
}
