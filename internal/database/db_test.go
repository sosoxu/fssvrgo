package database

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/pgtest"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	// Each test runs in its own PostgreSQL schema so tests stay isolated.
	cfg := pgtest.NewSchema(t)
	raw, err := sql.Open("postgres", pgtest.DSN(cfg))
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { raw.Close() })

	db := NewDB(raw, DialectPostgreSQL)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

func createKVTable(t *testing.T, db *DB) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS kv (id SERIAL PRIMARY KEY, k TEXT, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func countStmts(db *DB) int {
	count := 0
	db.stmts.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func TestDB_PreparedStatements(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	insertQ := "INSERT INTO kv (k, v) VALUES (?, ?)"

	if _, err := db.Exec(insertQ, "a", "1"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	after1 := countStmts(db)
	if after1 == 0 {
		t.Fatalf("expected prepared statement to be cached")
	}

	// second call with the same query must reuse the cached statement
	if _, err := db.Exec(insertQ, "b", "2"); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	after2 := countStmts(db)
	if after2 != after1 {
		t.Fatalf("expected cached statement reuse, before=%d after=%d", after1, after2)
	}
}

func TestDB_DialectTranslation(t *testing.T) {
	// Translate is a pure function on Dialect, so no database connection is
	// required to exercise placeholder translation.
	//
	// SQLite dialect leaves "?" placeholders untouched
	sqliteDB := NewDB(nil, DialectSQLite)
	if got := sqliteDB.GetDialect().Translate("SELECT * FROM t WHERE a = ? AND b = ?"); got != "SELECT * FROM t WHERE a = ? AND b = ?" {
		t.Fatalf("sqlite translate: got %q", got)
	}

	// PostgreSQL dialect translates "?" into $1, $2, ...
	pgDB := NewDB(nil, DialectPostgreSQL)
	if got := pgDB.GetDialect().Translate("SELECT * FROM t WHERE a = ? AND b = ?"); got != "SELECT * FROM t WHERE a = $1 AND b = $2" {
		t.Fatalf("pg translate: got %q", got)
	}
}

func TestDB_Query(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	if _, err := db.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "name", "value"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Query("SELECT k, v FROM kv WHERE k = ?", "name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if k != "name" || v != "value" {
			t.Fatalf("unexpected row: k=%q v=%q", k, v)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestDB_Exec(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	res, err := db.Exec("INSERT INTO kv (k, v) VALUES (?, ?)", "key1", "val1")
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row affected, got %d", n)
	}

	var v string
	if err := db.QueryRow("SELECT v FROM kv WHERE k = ?", "key1").Scan(&v); err != nil {
		t.Fatalf("queryrow: %v", err)
	}
	if v != "val1" {
		t.Fatalf("expected val1, got %s", v)
	}
}

func TestDB_Transaction(t *testing.T) {
	db := newTestDB(t)
	createKVTable(t, db)

	raw := db.Underlying()

	// commit path
	tx, err := raw.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO kv (k, v) VALUES ($1, $2)", "tx1", "tv1"); err != nil {
		tx.Rollback()
		t.Fatalf("tx insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var v string
	if err := db.QueryRow("SELECT v FROM kv WHERE k = ?", "tx1").Scan(&v); err != nil {
		t.Fatalf("query after commit: %v", err)
	}
	if v != "tv1" {
		t.Fatalf("expected tv1, got %s", v)
	}

	// rollback path
	tx2, err := raw.Begin()
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	if _, err := tx2.Exec("INSERT INTO kv (k, v) VALUES ($1, $2)", "tx2", "tv2"); err != nil {
		tx2.Rollback()
		t.Fatalf("tx2 insert: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var v2 string
	err = db.QueryRow("SELECT v FROM kv WHERE k = ?", "tx2").Scan(&v2)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows after rollback, got err=%v v=%q", err, v2)
	}
}
