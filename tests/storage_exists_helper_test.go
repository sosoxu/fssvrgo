package tests

import "testing"

// mustStorageExist wraps the storage adapter's (bool, error) Exists contract
// for the integration suite: a backend error fails the test instead of being
// silently read as "absent".
func mustStorageExist(tb testing.TB, s interface{ Exists(string) (bool, error) }, path string) bool {
	tb.Helper()
	exists, err := s.Exists(path)
	if err != nil {
		tb.Fatalf("Exists(%q) returned error: %v", path, err)
	}
	return exists
}
