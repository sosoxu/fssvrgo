package database

import "testing"

// TestDBQueryFailureDropsCachedStatement pins the fix for issue #121: when a
// cached statement fails at execution time it must be removed *and* closed,
// otherwise every failure leaks a server-side prepared statement.
func TestDBQueryFailureDropsCachedStatement(t *testing.T) {
	db := newTestDB(t)

	query := "SELECT 1 / ?"
	translated := db.GetDialect().Translate(query)

	// Prepares fine, fails at execution: division by zero.
	if _, err := db.Query(query, 0); err == nil {
		t.Fatal("expected division by zero to fail")
	}
	if _, cached := db.stmts.Load(translated); cached {
		t.Fatal("failing query left a cached prepared statement behind (statement leak)")
	}

	rows, err := db.Query(query, 1)
	if err != nil {
		t.Fatalf("expected query to succeed after eviction: %v", err)
	}
	rows.Close()
	if _, cached := db.stmts.Load(translated); !cached {
		t.Fatal("successful query should be cached again")
	}
}
