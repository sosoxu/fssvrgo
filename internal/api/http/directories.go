package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *Server) handleCreateDirectory(c *gin.Context) {
	var req struct {
		Path string `json:"path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, "Directory path is required")
		return
	}

	if !isValidFilePath(req.Path) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
		return
	}

	if err := s.dirSvc.CreateDirectory(c.Request.Context(), req.Path); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("create_directory", req.Path, c, true, "")
	c.JSON(http.StatusCreated, gin.H{"message": "Directory created successfully"})
}

func (s *Server) handleDeleteDirectory(c *gin.Context) {
	dirPath := c.Param("path")
	if dirPath == "" {
		sendError(c, http.StatusBadRequest, "Directory path is required")
		return
	}

	if !isValidFilePath(dirPath) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
		return
	}

	recursive := c.Query("recursive") == "true"

	if err := s.dirSvc.DeleteDirectory(c.Request.Context(), dirPath, recursive); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("delete_directory", dirPath, c, true, fmt.Sprintf("recursive=%v", recursive))
	c.JSON(http.StatusOK, gin.H{"message": "Directory deleted successfully"})
}

func (s *Server) handleRenameDirectory(c *gin.Context) {
	dirPath := c.Param("path")
	if dirPath == "" {
		sendError(c, http.StatusBadRequest, "Directory path is required")
		return
	}

	if !isValidFilePath(dirPath) {
		sendError(c, http.StatusBadRequest, "Invalid directory path")
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
		sendError(c, http.StatusBadRequest, "Invalid directory name")
		return
	}

	if err := s.dirSvc.RenameDirectory(c.Request.Context(), dirPath, newName); err != nil {
		sendInternalError(c, err, "Internal server error")
		return
	}

	s.auditLog("rename_directory", dirPath, c, true, fmt.Sprintf("new_name=%s", newName))
	c.JSON(http.StatusOK, gin.H{"message": "Directory renamed successfully"})
}
