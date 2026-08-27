package tests

import (
	"os"
	"strconv"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/database"
)

func postgreSQLTestConfig(poolSize int) config.DatabaseConfig {
	port := 5432
	if raw := os.Getenv("FSS_TEST_PG_PORT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			port = parsed
		}
	}
	value := func(key, fallback string) string {
		if configured := os.Getenv(key); configured != "" {
			return configured
		}
		return fallback
	}
	return config.DatabaseConfig{
		Type:     "postgresql",
		Host:     value("FSS_TEST_PG_HOST", "localhost"),
		Port:     port,
		Name:     value("FSS_TEST_PG_NAME", "fsserver"),
		User:     value("FSS_TEST_PG_USER", "fsserver"),
		Password: value("FSS_TEST_PG_PASSWORD", "fsserver123"),
		SSLMode:  value("FSS_TEST_PG_SSLMODE", "disable"),
		PoolSize: poolSize,
	}
}

func connectPostgreSQLTestDB(tb testing.TB, poolSize int) (*database.Database, *database.DB) {
	tb.Helper()
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(postgreSQLTestConfig(poolSize)); err != nil {
		tb.Skipf("PostgreSQL test database unavailable: %v", err)
	}
	qdb := dbObj.GetQueryDB()
	resetPostgreSQLTestDB(qdb)
	return dbObj, qdb
}

func requirePostgreSQLTestDB(tb testing.TB, poolSize int) (*database.Database, *database.DB) {
	tb.Helper()
	dbObj := database.NewDatabase()
	if err := dbObj.Connect(postgreSQLTestConfig(poolSize)); err != nil {
		tb.Fatalf("PostgreSQL test database unavailable: %v", err)
	}
	qdb := dbObj.GetQueryDB()
	resetPostgreSQLTestDB(qdb)
	return dbObj, qdb
}

func resetPostgreSQLTestDB(qdb *database.DB) {
	for _, table := range []string{"namespace_lock_holders", "namespace_fence_heads", "transfer_session_results", "transfer_tasks", "audit_log", "api_keys", "files", "directories", "schema_migrations"} {
		_, _ = qdb.Exec("DELETE FROM " + table)
	}
}
