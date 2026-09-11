package storage

import "testing"

// mustExist wraps the (bool, error) Exists contract for tests: a backend error
// fails the test instead of being silently treated as "absent".
func mustExist(tb testing.TB, s interface{ Exists(string) (bool, error) }, path string) bool {
	tb.Helper()
	exists, err := s.Exists(path)
	if err != nil {
		tb.Fatalf("Exists(%q) returned error: %v", path, err)
	}
	return exists
}
