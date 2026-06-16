package database

import (
	"fmt"
	"sort"
	"time"
)

type Migration struct {
	Version int
	Name    string
	Up      func() error
}

type MigrationManager struct {
	migrations []Migration
	db         *DB
}

func NewMigrationManager(db *DB) *MigrationManager {
	return &MigrationManager{
		db: db,
	}
}

func (m *MigrationManager) Register(migration Migration) {
	for i, existing := range m.migrations {
		if existing.Version == migration.Version {
			m.migrations[i] = migration
			sort.Slice(m.migrations, func(i, j int) bool {
				return m.migrations[i].Version < m.migrations[j].Version
			})
			return
		}
	}
	m.migrations = append(m.migrations, migration)
	sort.Slice(m.migrations, func(i, j int) bool {
		return m.migrations[i].Version < m.migrations[j].Version
	})
}

func (m *MigrationManager) Initialize() error {
	_, err := m.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}
	return nil
}

func (m *MigrationManager) RunMigrations() error {
	if err := m.Initialize(); err != nil {
		return err
	}

	pending, err := m.getPendingMigrations()
	if err != nil {
		return err
	}

	if len(pending) == 0 {
		return nil
	}

	for _, migration := range pending {
		if migration.Up == nil {
			return fmt.Errorf("migration v%d has no Up function: %s", migration.Version, migration.Name)
		}

		if err := migration.Up(); err != nil {
			return fmt.Errorf("migration v%d failed (%s): %w", migration.Version, migration.Name, err)
		}

		if err := m.recordMigration(migration.Version, migration.Name); err != nil {
			return fmt.Errorf("failed to record migration v%d: %w", migration.Version, err)
		}
	}

	return nil
}

func (m *MigrationManager) GetCurrentVersion() int {
	versions, err := m.getAppliedVersions()
	if err != nil || len(versions) == 0 {
		return 0
	}

	maxVersion := versions[0]
	for _, v := range versions[1:] {
		if v > maxVersion {
			maxVersion = v
		}
	}
	return maxVersion
}

func (m *MigrationManager) getAppliedVersions() ([]int, error) {
	rows, err := m.db.Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to scan migration version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}
	return versions, nil
}

func (m *MigrationManager) getPendingMigrations() ([]Migration, error) {
	applied, err := m.getAppliedVersions()
	if err != nil {
		return nil, err
	}

	appliedSet := make(map[int]bool)
	for _, v := range applied {
		appliedSet[v] = true
	}

	var pending []Migration
	for _, migration := range m.migrations {
		if !appliedSet[migration.Version] {
			pending = append(pending, migration)
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		return pending[i].Version < pending[j].Version
	})

	return pending, nil
}

func (m *MigrationManager) recordMigration(version int, name string) error {
	_, err := m.db.Exec("INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
		version, name, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}
	return nil
}
