package database

// RegisterBaseMigration registers version 1 of the schema: the tables, indexes
// and soft-delete columns every deployment needs.
//
// It is the single source of truth for the schema. Previously the production
// entrypoint kept its own inline copy of these CREATE TABLE statements while
// this package kept another (used by tests), which guaranteed the two would
// drift. Both now go through InitTables.
func RegisterBaseMigration(manager *MigrationManager) {
	manager.Register(Migration{
		Version: 1,
		Name:    "initial_schema",
		Up: func() error {
			return InitTables(manager.db)
		},
	})
}
