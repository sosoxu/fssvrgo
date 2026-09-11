package database

import "testing"

// TestRegisterBaseMigrationCreatesSchema covers the consolidation in #118: the
// production entrypoint no longer carries its own copy of the CREATE TABLE
// statements, it runs this migration instead.
func TestRegisterBaseMigrationCreatesSchema(t *testing.T) {
	db := newTestDB(t)

	manager := NewMigrationManager(db)
	RegisterBaseMigration(manager)
	if err := manager.RunMigrations(); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	tables := []string{"files", "directories", "transfer_tasks", "audit_log", "api_keys", "schema_migrations"}
	for _, table := range tables {
		var count int
		err := db.QueryRow(
			"SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?",
			table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("query table %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("table %s was not created (count=%d)", table, count)
		}
	}

	// Migrations must stay idempotent: running them again is a no-op.
	if err := manager.RunMigrations(); err != nil {
		t.Fatalf("second RunMigrations must be a no-op: %v", err)
	}
}
