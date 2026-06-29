package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGenerateUUID(t *testing.T) {
	id1 := GenerateUUID()
	if id1 == "" {
		t.Errorf("UUID should not be empty")
	}

	id2 := GenerateUUID()
	if id2 == "" {
		t.Errorf("UUID should not be empty")
	}

	if id1 == id2 {
		t.Errorf("UUIDs should be unique")
	}
}

func TestMD5(t *testing.T) {
	result := MD5("hello")
	if result != "5d41402abc4b2a76b9719d911017c592" {
		t.Errorf("MD5('hello') expected 5d41402abc4b2a76b9719d911017c592, got %s", result)
	}
}

func TestSHA256(t *testing.T) {
	result := SHA256("hello")
	if result != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Errorf("SHA256('hello') expected 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824, got %s", result)
	}
}

func TestSHA256File(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "testfile")
	content := "hello"
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	fileHash, err := SHA256File(filePath)
	if err != nil {
		t.Fatalf("SHA256File failed: %v", err)
	}

	expectedHash := SHA256(content)
	if fileHash != expectedHash {
		t.Errorf("SHA256File hash %s does not match SHA256 string hash %s", fileHash, expectedHash)
	}
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/foo/bar", "foo/bar"},
		{"foo/bar", "foo/bar"},
		{"/foo/../bar", "bar"},
		{"foo/./bar", "foo/bar"},
		{".", "."},
		{"/", ""},
		{"//foo", "foo"},
		{"foo//bar", "foo/bar"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := NormalizePath(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizePath(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetFileName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/foo/bar.txt", "bar.txt"},
		{"bar.txt", "bar.txt"},
		{"/foo/bar/baz", "baz"},
		{"baz", "baz"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := GetFileName(tt.input)
			if result != tt.expected {
				t.Errorf("GetFileName(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsValidFileName(t *testing.T) {
	validNames := []string{"test.txt", "hello world.doc", "file_1.csv", "my-file.json", "数据.xlsx", "file.txt", "文档.pdf"}
	for _, name := range validNames {
		if !IsValidFileName(name) {
			t.Errorf("IsValidFileName(%q) should be true", name)
		}
	}

	invalidNames := []struct {
		name   string
		reason string
	}{
		{"", "empty"},
		{strings.Repeat("a", 256), "too long"},
		{"..", "path traversal"},
		{".", "current dir"},
		{"foo..bar", "contains double dot"},
		{"foo/bar", "contains slash"},
		{"foo\\bar", "contains backslash"},
		{"a/b", "contains slash"},
		{"a\\b", "contains backslash"},
		{"foo\x00bar", "control char null"},
		{"foo\nbar", "control char newline"},
		{"foo\rbar", "control char carriage return"},
	}

	for _, tt := range invalidNames {
		if IsValidFileName(tt.name) {
			t.Errorf("IsValidFileName(%q) should be false (%s)", tt.name, tt.reason)
		}
	}
}

func TestIsValidFilePath(t *testing.T) {
	validPaths := []string{"file.txt", "dir/file.txt", "/dir/file.txt"}
	for _, p := range validPaths {
		if !IsValidFilePath(p) {
			t.Errorf("IsValidFilePath(%q) should be true", p)
		}
	}

	invalidPaths := []struct {
		path   string
		reason string
	}{
		{"", "empty"},
		{"/", "root only"},
		{"..", "parent directory"},
		{"../etc/passwd", "traversal"},
		{"dir/../etc", "traversal in middle"},
		{"dir//file", "double slash"},
	}
	for _, tt := range invalidPaths {
		if IsValidFilePath(tt.path) {
			t.Errorf("IsValidFilePath(%q) should be false (%s)", tt.path, tt.reason)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	original := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	formatted := FormatTimestamp(original)
	parsed, err := ParseTimestamp(formatted)
	if err != nil {
		t.Fatalf("ParseTimestamp failed: %v", err)
	}
	if !parsed.Equal(original) {
		t.Errorf("roundtrip failed: expected %v, got %v", original, parsed)
	}
}

func TestFileExists(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "exists.txt")
	if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	if !FileExists(filePath) {
		t.Errorf("FileExists should return true for existing file")
	}

	if FileExists(filepath.Join(dir, "nonexistent.txt")) {
		t.Errorf("FileExists should return false for non-existing file")
	}

	if FileExists(dir) {
		t.Errorf("FileExists should return false for directory")
	}
}

func TestDirectoryExists(t *testing.T) {
	dir := t.TempDir()

	if !DirectoryExists(dir) {
		t.Errorf("DirectoryExists should return true for existing directory")
	}

	if DirectoryExists(filepath.Join(dir, "nonexistent")) {
		t.Errorf("DirectoryExists should return false for non-existing directory")
	}

	filePath := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	if DirectoryExists(filePath) {
		t.Errorf("DirectoryExists should return false for file")
	}
}
