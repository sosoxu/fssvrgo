package storage

import (
	"context"
	"testing"
)

// mustExist wraps the (bool, error) Exists contract for tests: a backend error
// fails the test instead of being silently treated as "absent".
func mustExist(tb testing.TB, s interface {
	Exists(context.Context, string) (bool, error)
}, path string) bool {
	tb.Helper()
	exists, err := s.Exists(context.Background(), path)
	if err != nil {
		tb.Fatalf("Exists(%q) returned error: %v", path, err)
	}
	return exists
}
