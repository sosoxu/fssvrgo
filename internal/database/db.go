package database

import (
	"database/sql"
	"sync"
)

type DB struct {
	db      *sql.DB
	dialect Dialect
	stmts   sync.Map
}

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
		d.stmts.Delete(d.dialect.Translate(query))
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
