package database

import (
	"context"
	"database/sql"
	"sync"
)

type DB struct {
	db      *sql.DB
	dialect Dialect
	stmts   sync.Map
}

// Tx wraps a *sql.Tx with the same dialect translation as DB so that callers
// can run multi-statement updates atomically without re-implementing placeholder
// translation. Statements run on Tx bypass the prepared-statement cache (they
// are bound to the transaction), which is acceptable for low-frequency
// administrative operations like directory rename/delete.
type Tx struct {
	tx      *sql.Tx
	dialect Dialect
}

// BeginTx starts a new transaction. The returned Tx provides Exec/Query/QueryRow
// with dialect translation matching DB.
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, dialect: d.dialect}, nil
}

func (t *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return t.tx.Exec(t.dialect.Translate(query), args...)
}

func (t *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.Query(t.dialect.Translate(query), args...)
}

func (t *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRow(t.dialect.Translate(query), args...)
}

func (t *Tx) Commit() error { return t.tx.Commit() }

func (t *Tx) Rollback() error { return t.tx.Rollback() }

func NewDB(db *sql.DB, dialect Dialect) *DB {
	return &DB{db: db, dialect: dialect}
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := d.prepareStmt(query)
	if err != nil {
		return d.db.Exec(d.dialect.Translate(query), args...)
	}
	return stmt.Exec(args...)
}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := d.prepareStmt(query)
	if err != nil {
		return d.db.Query(d.dialect.Translate(query), args...)
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		// Drop the cached statement and close it: leaving it registered would
		// leak a server-side prepared statement every time a query fails (for
		// example when a table is missing or a parameter type is rejected).
		d.discardStmt(d.dialect.Translate(query))
		return d.db.Query(d.dialect.Translate(query), args...)
	}
	return rows, nil
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	stmt, err := d.prepareStmt(query)
	if err != nil {
		return d.db.QueryRow(d.dialect.Translate(query), args...)
	}
	return stmt.QueryRow(args...)
}

func (d *DB) prepareStmt(query string) (*sql.Stmt, error) {
	translated := d.dialect.Translate(query)

	val, ok := d.stmts.Load(translated)
	if ok {
		return val.(*sql.Stmt), nil
	}

	stmt, err := d.db.Prepare(translated)
	if err != nil {
		return nil, err
	}

	actual, loaded := d.stmts.LoadOrStore(translated, stmt)
	if loaded {
		stmt.Close()
		return actual.(*sql.Stmt), nil
	}
	return stmt, nil
}

// discardStmt removes a statement from the cache and closes it. Closing is
// best-effort: a statement that is already broken must not mask the original
// error the caller is about to see.
func (d *DB) discardStmt(translated string) {
	val, ok := d.stmts.LoadAndDelete(translated)
	if !ok {
		return
	}
	if stmt, ok := val.(*sql.Stmt); ok {
		_ = stmt.Close()
	}
}

func (d *DB) Underlying() *sql.DB {
	return d.db
}

func (d *DB) GetDialect() Dialect {
	return d.dialect
}

func (d *DB) Ping() error {
	return d.db.Ping()
}

func (d *DB) Close() error {
	d.stmts.Range(func(key, value interface{}) bool {
		stmt := value.(*sql.Stmt)
		stmt.Close()
		return true
	})
	d.stmts = sync.Map{}
	return d.db.Close()
}
