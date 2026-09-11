// Package pgtest provides the PostgreSQL connection helpers used by the test
// suite. The suite runs exclusively against PostgreSQL (SQLite is no longer
// used by tests); every test gets its own schema, selected through the
// search_path run-time parameter, so tests stay isolated even when they run
// concurrently.
package pgtest

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/sosoxu/fssvrgo/internal/config"
)

var seq atomic.Uint64

// Config returns the PostgreSQL settings used by the test suite. The defaults
// target the local fsserver-postgres container; override them with the
// FSS_TEST_PG_* environment variables.
func Config() config.DatabaseConfig {
	return config.DatabaseConfig{
		Type:     "postgresql",
		Host:     env("FSS_TEST_PG_HOST", "localhost"),
		Port:     envInt("FSS_TEST_PG_PORT", 5432),
		Name:     env("FSS_TEST_PG_NAME", "fsserver"),
		User:     env("FSS_TEST_PG_USER", "fsserver"),
		Password: env("FSS_TEST_PG_PASS", "fsserver123"),
		SSLMode:  env("FSS_TEST_PG_SSLMODE", "disable"),
		PoolSize: envInt("FSS_TEST_PG_POOL", 10),
	}
}

// DSN renders cfg as a lib/pq key=value connection string, including the
// search_path run-time parameter when one is set.
func DSN(cfg config.DatabaseConfig) string {
	sslmode := cfg.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	dsn := fmt.Sprintf("connect_timeout=5 host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Name, sslmode)
	if cfg.SearchPath != "" {
		dsn += " search_path=" + cfg.SearchPath
	}
	return dsn
}

// NewSchemaName returns a process-unique schema name suitable for test
// isolation.
func NewSchemaName() string {
	return fmt.Sprintf("fss_test_%d_%d", time.Now().UnixNano(), seq.Add(1))
}

// CreateSchema creates the named schema and registers it for removal when the
// test finishes.
//
// When PostgreSQL is unreachable the test is skipped — unless
// FSS_TEST_REQUIRE_INFRA is set (CI does this), in which case it fails: a
// missing database must not be reported as a green build.
func CreateSchema(t testing.TB, name string) {
	t.Helper()

	if err := Create(Config(), name); err != nil {
		if requireInfra() {
			t.Fatalf("FSS_TEST_REQUIRE_INFRA is set but PostgreSQL is not available: %v", err)
		}
		t.Skipf("PostgreSQL not available: %v", err)
	}

	t.Cleanup(func() { _ = Drop(Config(), name) })
}

// requireInfra reports whether reachable test infrastructure is mandatory.
// Set FSS_TEST_REQUIRE_INFRA=1 in CI so infrastructure outages surface as
// failures instead of silently skipping the tests that depend on them.
func requireInfra() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("FSS_TEST_REQUIRE_INFRA"))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

// NewSchema provisions a fresh PostgreSQL schema and returns a config scoped to
// it via search_path. The schema is dropped when the test finishes.
//
// Tests skip (rather than fail) when PostgreSQL is unreachable so the suite can
// still be built and partially run on machines without a database.
func NewSchema(t testing.TB) config.DatabaseConfig {
	t.Helper()

	schema := NewSchemaName()
	CreateSchema(t, schema)

	cfg := Config()
	cfg.SearchPath = schema
	return cfg
}

// Create creates the named PostgreSQL schema. It is used by helpers that are
// not backed by a testing.TB (for example the integration test server).
func Create(cfg config.DatabaseConfig, name string) error {
	raw, err := sql.Open("postgres", DSN(cfg))
	if err != nil {
		return err
	}
	defer raw.Close()
	_, err = raw.Exec("CREATE SCHEMA " + name)
	return err
}

// Drop removes the named PostgreSQL schema and everything in it.
func Drop(cfg config.DatabaseConfig, name string) error {
	raw, err := sql.Open("postgres", DSN(cfg))
	if err != nil {
		return err
	}
	defer raw.Close()
	_, err = raw.Exec("DROP SCHEMA " + name + " CASCADE")
	return err
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
