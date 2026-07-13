package database

import (
	"context"
	"testing"
)

// TestTx_Commit verifies that statements executed on a Tx are visible after
// Commit and that the Tx translates placeholders consistent with the DB.
func TestTx_Commit(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "tx1", "a"); err != nil {
		tx.Rollback()
		t.Fatalf("tx.Exec insert: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "tx2", "b"); err != nil {
		tx.Rollback()
		t.Fatalf("tx.Exec insert 2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM kv WHERE k LIKE ?", "tx%").Scan(&n); err != nil {
		t.Fatalf("QueryRow count: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows after commit, got %d", n)
	}
}

// TestTx_Rollback verifies that statements executed on a Tx are NOT visible
// after Rollback, so multi-statement updates can be aborted atomically.
func TestTx_Rollback(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	// Seed one row outside the tx.
	if _, err := db.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "seed", "0"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "rb1", "x"); err != nil {
		tx.Rollback()
		t.Fatalf("tx.Exec insert: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM kv").Scan(&n); err != nil {
		t.Fatalf("QueryRow count: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row (only seed) after rollback, got %d", n)
	}
}

// TestTx_RollbackOnErrorPattern verifies the idiomatic "begin, exec, rollback
// on error, commit at end" pattern used by directory rename/delete works as
// expected: a simulated mid-tx error leaves the table unchanged.
func TestTx_RollbackOnErrorPattern(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	if _, err := tx.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "p1", "1"); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Simulate a processing error: bail out without committing. The deferred
	// Rollback should undo the insert.
	simulatedErr := true
	if simulatedErr {
		return
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	committed = true

	t.Fatal("should not reach commit path when simulatedErr is true")
}

func TestTx_RollbackOnErrorPattern_PostCheck(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	// Run the pattern in a helper so the deferred Rollback fires on return.
	func() {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback()
			}
		}()
		if _, err := tx.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "p2", "1"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		// Return without committing -> deferred Rollback runs.
	}()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM kv").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows after rolled-back pattern, got %d", n)
	}
}
