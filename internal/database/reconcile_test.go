package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/storage"
)

func newReconcileFixture(t *testing.T) (*DB, *storage.LocalStorage) {
	t.Helper()
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}
	return db, storage.NewLocalStorage(t.TempDir())
}

func insertFileMeta(t *testing.T, db *DB, id, path string) {
	t.Helper()
	_, err := db.Exec(
		"INSERT INTO files (id, path, name, size, hash, storage_type, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)",
		id, path, path, 0, "", "local", false,
	)
	if err != nil {
		t.Fatalf("insert file metadata %s: %v", path, err)
	}
}

func TestReconciler_DetectsAndRepairsDrift(t *testing.T) {
	db, store := newReconcileFixture(t)
	ctx := context.Background()

	// Consistent pair: object and metadata both present.
	if err := store.Write("/keep.txt", []byte("keep")); err != nil {
		t.Fatalf("write object: %v", err)
	}
	insertFileMeta(t, db, "id1", "/keep.txt")

	// Drift 1: metadata row whose object is gone.
	insertFileMeta(t, db, "id2", "/ghost.txt")

	// Drift 2: object with no metadata row.
	if err := store.Write("/orphan.bin", []byte("orphan")); err != nil {
		t.Fatalf("write orphan object: %v", err)
	}

	reconciler := NewReconciler(db, store)
	report, err := reconciler.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !report.EnumerationSupported {
		t.Fatalf("expected local storage to support object enumeration")
	}
	if report.DBCount != 2 || report.StorageCount != 2 {
		t.Fatalf("unexpected counts: db=%d storage=%d", report.DBCount, report.StorageCount)
	}
	if len(report.MissingInStorage) != 1 || report.MissingInStorage[0] != "/ghost.txt" {
		t.Fatalf("unexpected MissingInStorage: %v", report.MissingInStorage)
	}
	if len(report.OrphanInStorage) != 1 || report.OrphanInStorage[0] != "/orphan.bin" {
		t.Fatalf("unexpected OrphanInStorage: %v", report.OrphanInStorage)
	}
	if report.Consistent() {
		t.Fatalf("report with drift must not be reported as consistent")
	}

	result, err := reconciler.Repair(ctx, report)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if result.OrphansRemoved != 1 || result.DanglingMarked != 1 {
		t.Fatalf("unexpected repair result: %+v", result)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected repair errors: %v", result.Errors)
	}

	after, err := reconciler.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan after repair: %v", err)
	}
	if !after.Consistent() {
		t.Fatalf("expected clean state after repair, got missing=%v orphan=%v",
			after.MissingInStorage, after.OrphanInStorage)
	}

	// The orphan object is gone from storage...
	if _, err := store.GetSize("/orphan.bin"); err == nil {
		t.Fatalf("orphan object still present in storage")
	}
	// ...and the dangling metadata row is soft-deleted rather than hard-deleted.
	var isDeleted bool
	if err := db.QueryRow("SELECT is_deleted FROM files WHERE path = ?", "/ghost.txt").Scan(&isDeleted); err != nil {
		t.Fatalf("query dangling metadata: %v", err)
	}
	if !isDeleted {
		t.Fatalf("dangling metadata should be soft-deleted")
	}
}

func TestReconciler_ConsistentWhenNoDrift(t *testing.T) {
	db, store := newReconcileFixture(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("/dir%d/file%d.txt", i, i)
		if err := store.Write(path, []byte("data")); err != nil {
			t.Fatalf("write object %s: %v", path, err)
		}
		insertFileMeta(t, db, fmt.Sprintf("id%d", i), path)
	}

	reconciler := NewReconciler(db, store)
	report, err := reconciler.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !report.Consistent() {
		t.Fatalf("expected consistent report, got missing=%v orphan=%v",
			report.MissingInStorage, report.OrphanInStorage)
	}
	if report.DBCount != 3 || report.StorageCount != 3 {
		t.Fatalf("unexpected counts: db=%d storage=%d", report.DBCount, report.StorageCount)
	}
}

func TestReconciler_IgnoresSoftDeletedMetadata(t *testing.T) {
	db, store := newReconcileFixture(t)
	ctx := context.Background()

	// A soft-deleted row whose object has already been removed is expected
	// state, not drift.
	insertFileMeta(t, db, "id1", "/deleted.txt")
	if _, err := db.Exec("UPDATE files SET is_deleted = ? WHERE path = ?", true, "/deleted.txt"); err != nil {
		t.Fatalf("soft-delete metadata: %v", err)
	}

	reconciler := NewReconciler(db, store)
	report, err := reconciler.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !report.Consistent() {
		t.Fatalf("soft-deleted metadata must not count as drift: %+v", report)
	}
	if report.DBCount != 0 {
		t.Fatalf("expected soft-deleted row to be excluded, DBCount=%d", report.DBCount)
	}
}
