package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"

	"github.com/sosoxu/fssvrgo/internal/config"
)

type Database struct {
	db       *sql.DB
	config   config.DatabaseConfig
	dialect  Dialect
	queryDB  *DB
	mu       sync.RWMutex
	lastError string
}

func NewDatabase() *Database {
	return &Database{}
}

func (d *Database) Connect(cfg config.DatabaseConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.config = cfg
	d.dialect = DialectFromString(cfg.Type)

	var db *sql.DB
	var err error

	switch d.dialect {
	case DialectPostgreSQL:
		db, err = d.connectPostgreSQL(cfg)
	default:
		db, err = d.connectSQLite(cfg)
	}

	if err != nil {
		d.lastError = err.Error()
		return err
	}

	maxOpenConns := cfg.PoolSize
	if maxOpenConns < 1 {
		maxOpenConns = 25
	}
	maxIdleConns := maxOpenConns
	if maxIdleConns > 10 {
		maxIdleConns = 10
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		d.lastError = fmt.Sprintf("failed to ping database: %v", err)
		return fmt.Errorf("failed to ping database: %w", err)
	}

	d.db = db
	d.queryDB = NewDB(db, d.dialect)
	d.lastError = ""
	return nil
}

func (d *Database) connectSQLite(cfg config.DatabaseConfig) (*sql.DB, error) {
	dir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}

	return db, nil
}

func (d *Database) connectPostgreSQL(cfg config.DatabaseConfig) (*sql.DB, error) {
	sslmode := cfg.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Name, sslmode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgresql database: %w", err)
	}

	return db, nil
}

func (d *Database) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db == nil {
		return nil
	}

	if d.queryDB != nil {
		d.queryDB.Close()
		d.queryDB = nil
	}

	err := d.db.Close()
	d.db = nil
	if err != nil {
		d.lastError = fmt.Sprintf("failed to close database: %v", err)
		return fmt.Errorf("failed to close database: %w", err)
	}
	return nil
}

func (d *Database) Execute(query string, args ...interface{}) error {
	d.mu.RLock()
	db := d.db
	d.mu.RUnlock()

	if db == nil {
		d.lastError = "not connected to database"
		return fmt.Errorf("not connected to database")
	}

	query = d.dialect.Translate(query)
	_, err := db.Exec(query, args...)
	if err != nil {
		d.mu.Lock()
		d.lastError = fmt.Sprintf("execute error: %v", err)
		d.mu.Unlock()
		return fmt.Errorf("execute error: %w", err)
	}
	return nil
}

func (d *Database) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	d.mu.RLock()
	db := d.db
	d.mu.RUnlock()

	if db == nil {
		d.lastError = "not connected to database"
		return nil, fmt.Errorf("not connected to database")
	}

	query = d.dialect.Translate(query)
	rows, err := db.Query(query, args...)
	if err != nil {
		d.mu.Lock()
		d.lastError = fmt.Sprintf("query error: %v", err)
		d.mu.Unlock()
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		d.mu.Lock()
		d.lastError = fmt.Sprintf("columns error: %v", err)
		d.mu.Unlock()
		return nil, fmt.Errorf("columns error: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			d.mu.Lock()
			d.lastError = fmt.Sprintf("scan error: %v", err)
			d.mu.Unlock()
			return nil, fmt.Errorf("scan error: %w", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			switch v := val.(type) {
			case []byte:
				row[col] = string(v)
			default:
				row[col] = v
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		d.mu.Lock()
		d.lastError = fmt.Sprintf("rows error: %v", err)
		d.mu.Unlock()
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return results, nil
}

func (d *Database) getLastError() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastError
}

func (d *Database) GetDB() *sql.DB {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db
}

func (d *Database) GetDialect() Dialect {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dialect
}

func (d *Database) GetQueryDB() *DB {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.queryDB
}
