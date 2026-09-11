package directory

import "testing"

// mustExist wraps the storage adapter's (bool, error) Exists contract for
// tests: a backend error fails the test instead of being read as "absent".
func mustExist(tb testing.TB, s interface{ Exists(string) (bool, error) }, path string) bool {
	tb.Helper()
	exists, err := s.Exists(path)
	if err != nil {
		tb.Fatalf("Exists(%q) returned error: %v", path, err)
	}
	return exists
}
