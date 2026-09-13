package http

import (
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	os.Exit(m.Run())
}

func pageContext(query string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/?"+query, nil)
	return c
}

func TestParsePageParamsClampsAndDefaults(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		maxPageSize  int
		wantPage     int
		wantPageSize int
	}{
		{"defaults", "", 1000, 1, 20},
		{"explicit", "page=3&page_size=50", 1000, 3, 50},
		{"page_size clamped to max", "page_size=5000", 100, 1, 100},
		{"page_size below one falls back", "page_size=0", 1000, 1, 20},
		{"negative page_size falls back", "page_size=-5", 1000, 1, 20},
		{"page below one falls back", "page=0", 1000, 1, 20},
		{"non numeric values fall back", "page=abc&page_size=xyz", 1000, 1, 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			page, pageSize := parsePageParams(pageContext(tc.query), tc.maxPageSize)
			if page != tc.wantPage || pageSize != tc.wantPageSize {
				t.Errorf("parsePageParams(%q, %d) = %d/%d, want %d/%d",
					tc.query, tc.maxPageSize, page, pageSize, tc.wantPage, tc.wantPageSize)
			}
		})
	}
}

func TestValidationHelpers(t *testing.T) {
	validNames := []string{"a.txt", "report-2026.pdf", "数据.csv"}
	for _, name := range validNames {
		if !isValidFileName(name) {
			t.Errorf("isValidFileName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"", "../evil", "dir/evil.txt"} {
		if isValidFileName(name) {
			t.Errorf("isValidFileName(%q) = true, want false", name)
		}
	}

	// Note: "/" and any path containing a ".." or empty segment are rejected
	// by IsValidFilePath, even when path.Clean would resolve them somewhere
	// safe — the check runs on the pre-clean segments.
	validPaths := []string{"/docs/a.txt", "docs/a.txt"}
	for _, p := range validPaths {
		if !isValidFilePath(p) {
			t.Errorf("isValidFilePath(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"", "/", "../../etc/passwd", "/a//b", "/a/b/../c.txt"} {
		if isValidFilePath(p) {
			t.Errorf("isValidFilePath(%q) = true, want false", p)
		}
	}
}
