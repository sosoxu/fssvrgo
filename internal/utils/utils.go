package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

func GenerateUUID() string {
	return uuid.New().String()
}

func MD5(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func SHA256(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func SHA256File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	h := sha256.New()
	buf := make([]byte, 256*1024)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func JoinPath(parts []string) string {
	return path.Join(parts...)
}

func NormalizePath(pathStr string) string {
	pathStr = path.Clean(pathStr)
	for strings.HasPrefix(pathStr, "/") {
		pathStr = pathStr[1:]
	}
	return pathStr
}

func GetFileName(pathStr string) string {
	return path.Base(pathStr)
}

func GetDirectory(pathStr string) string {
	return path.Dir(pathStr)
}

func FormatTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func ParseTimestamp(s string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05Z", s)
}

func GetCurrentTimestamp() string {
	return FormatTimestamp(time.Now())
}

func Trim(str string) string {
	return strings.TrimSpace(str)
}

func Split(str string, delimiter rune) []string {
	return strings.Split(str, string(delimiter))
}

func ToLower(str string) string {
	return strings.ToLower(str)
}

func ToUpper(str string) string {
	return strings.ToUpper(str)
}

func FileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func DirectoryExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func CreateDirectory(path string) error {
	return os.MkdirAll(path, 0755)
}

func RemoveDirectory(path string) error {
	return os.RemoveAll(path)
}

func IsValidFileName(name string) bool {
	if name == "" {
		return false
	}
	if len(name) > 255 {
		return false
	}
	if name == ".." || name == "." {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	if strings.ContainsAny(name, "/\\") {
		return false
	}
	for _, r := range name {
		if r < 0x20 {
			return false
		}
	}
	return true
}

// IsValidFilePath validates a logical file path against traversal attacks.
// A valid path is non-empty, does not contain ".." segments, and uses "/"
// as the separator. Leading "/" is stripped before validation.
func IsValidFilePath(p string) bool {
	if p == "" || p == "/" {
		return false
	}
	// Normalize: strip leading slashes.
	for strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	if p == "" {
		return false
	}
	// Reject paths that resolve to the current or parent directory.
	cleaned := path.Clean(p)
	if cleaned == "." || cleaned == ".." || cleaned == "/" {
		return false
	}
	// Inspect the original (pre-clean) segments so that ".." traversal
	// attempts and empty segments (double slashes) are rejected even when
	// path.Clean would resolve them to a safe path.
	parts := strings.Split(p, "/")
	for _, part := range parts {
		if part == ".." || part == "" {
			return false
		}
	}
	return true
}
