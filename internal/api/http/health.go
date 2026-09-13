package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func (s *Server) handleHealth(c *gin.Context) {
	s.handleHealthCheck(c, false)
}

func (s *Server) handleReady(c *gin.Context) {
	s.handleHealthCheck(c, true)
}

// handleHealthCheck implements a tiered probe:
//   - liveness (/health): process + DB only — cheap, safe for high-frequency
//     Kubernetes liveness probes.
//   - readiness (/ready): also verifies storage reachability. To avoid the
//     write/read/remove churn the previous implementation performed on every
//     probe (which produced significant I/O under high-frequency probes), the
//     storage check now issues a single lightweight GetSize on a probe key and
//     treats "not exist" as healthy (the backend is reachable; the probe key
//     simply does not exist). Other errors mark storage as degraded.
func (s *Server) handleHealthCheck(c *gin.Context, includeStorage bool) {
	status := gin.H{
		"status":    "ok",
		"timestamp": utils.GetCurrentTimestamp(),
		"uptime":    time.Since(s.startTime).String(),
	}

	if s.db != nil {
		if err := s.db.PingContext(c.Request.Context()); err != nil {
			status["database"] = "error"
			status["status"] = "degraded"
		} else {
			status["database"] = "ok"
		}
	}

	if includeStorage && s.store != nil {
		// Probe storage reachability without writing or deleting any object.
		// A NoSuchKey/NotFound (or a successful stat) means the backend is
		// reachable; only non-not-exist errors are treated as degraded.
		if _, err := s.store.GetSize(c.Request.Context(), ".health-probe"); err != nil && !storage.IsNotExist(err) {
			status["storage"] = "error"
			status["status"] = "degraded"
		} else {
			status["storage"] = "ok"
		}
	}

	code := http.StatusOK
	if status["status"] != "ok" {
		code = http.StatusServiceUnavailable
	}

	c.JSON(code, status)
}

func (s *Server) handleMetrics(c *gin.Context) {
	if s.metricsSvc != nil {
		s.metricsSvc.Handler().ServeHTTP(c.Writer, c.Request)
	} else {
		c.JSON(http.StatusOK, gin.H{"uptime": time.Since(s.startTime).String(), "version": "1.0.0"})
	}
}
