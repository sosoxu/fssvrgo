package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

// ObjectLister is an optional capability implemented by storage backends that
// can enumerate every stored object. The reconciler degrades gracefully (and
// records that fact in its report) when the backend cannot enumerate.
type ObjectLister interface {
	ListObjects(ctx context.Context) ([]string, error)
}

// defaultReconcileSampleLimit caps how many drifted paths a report carries, so
// a badly broken deployment cannot flood logs or memory.
const defaultReconcileSampleLimit = 20

// ReconcileReport describes the drift found between file metadata rows and the
// objects actually present in storage.
type ReconcileReport struct {
	DBCount              int
	StorageCount         int
	MissingInStorage     []string // metadata exists, object is gone
	OrphanInStorage      []string // object exists, no metadata row
	Truncated            bool     // true when more drift exists than was sampled
	EnumerationSupported bool     // false when the backend cannot list objects
}

// Consistent reports whether metadata and storage agree.
func (r *ReconcileReport) Consistent() bool {
	return len(r.MissingInStorage) == 0 && len(r.OrphanInStorage) == 0
}

// ReconcileRepairResult summarizes what a repair pass changed.
type ReconcileRepairResult struct {
	OrphansRemoved int
	DanglingMarked int
	Errors         []string
}

// Reconciler compares file metadata with storage contents. It exists because a
// completion that crashes between "move object into storage" and "write
// metadata" leaves drift behind that nothing else in the process detects.
type Reconciler struct {
	db          *DB
	store       storage.StorageAdapter
	sampleLimit int
}

func NewReconciler(db *DB, store storage.StorageAdapter) *Reconciler {
	return &Reconciler{db: db, store: store, sampleLimit: defaultReconcileSampleLimit}
}

// Scan compares live metadata rows with the objects present in storage.
//
// When the backend cannot enumerate objects the scan returns a report with
// EnumerationSupported=false and no drift lists, rather than guessing from
// per-path existence checks.
func (r *Reconciler) Scan(ctx context.Context) (*ReconcileReport, error) {
	if r.db == nil || r.store == nil {
		return nil, fmt.Errorf("reconciler requires both a database and a storage adapter")
	}

	dbPaths := make(map[string]struct{})
	rows, err := r.db.Query(
		"SELECT path FROM files WHERE is_deleted = " + r.db.GetDialect().BooleanCheck(false),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list file metadata: %w", err)
	}
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan file metadata: %w", err)
		}
		dbPaths[canonicalObjectPath(path)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to read file metadata: %w", err)
	}
	rows.Close()

	report := &ReconcileReport{DBCount: len(dbPaths)}

	lister, ok := r.store.(ObjectLister)
	if !ok {
		return report, nil
	}
	report.EnumerationSupported = true

	objectPaths, err := lister.ListObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate storage objects: %w", err)
	}

	storagePaths := make(map[string]struct{}, len(objectPaths))
	for _, path := range objectPaths {
		storagePaths[canonicalObjectPath(path)] = struct{}{}
	}
	report.StorageCount = len(storagePaths)

	for path := range dbPaths {
		if _, ok := storagePaths[path]; ok {
			continue
		}
		report.MissingInStorage = appendSample(report.MissingInStorage, path, r.sampleLimit, report)
	}
	for path := range storagePaths {
		if _, ok := dbPaths[path]; ok {
			continue
		}
		report.OrphanInStorage = appendSample(report.OrphanInStorage, path, r.sampleLimit, report)
	}

	return report, nil
}

// Repair resolves the drift described by a report: orphan objects are removed
// from storage and metadata rows whose object is gone are soft-deleted. Both
// actions are conservative — nothing is hard-deleted from the database — and
// individual failures are collected instead of aborting the whole pass.
func (r *Reconciler) Repair(ctx context.Context, report *ReconcileReport) (*ReconcileRepairResult, error) {
	if report == nil {
		return nil, fmt.Errorf("reconciler repair requires a report")
	}
	result := &ReconcileRepairResult{}

	for _, path := range report.OrphanInStorage {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := r.store.Remove(path); err != nil && !storage.IsNotExist(err) {
			result.Errors = append(result.Errors, fmt.Sprintf("remove orphan %s: %v", path, err))
			continue
		}
		result.OrphansRemoved++
	}

	for _, path := range report.MissingInStorage {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if _, err := r.db.Exec(
			"UPDATE files SET is_deleted = ?, updated_at = CURRENT_TIMESTAMP WHERE path = ?",
			true, path,
		); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("mark dangling %s: %v", path, err))
			continue
		}
		result.DanglingMarked++
	}

	return result, nil
}

// LogReport writes a one-line summary, escalating to WARN when drift exists.
func (r *Reconciler) LogReport(report *ReconcileReport) {
	if report == nil {
		return
	}
	if !report.EnumerationSupported {
		logger.Warn("reconciliation skipped object enumeration: storage backend does not implement ListObjects (metadata rows=%d)", report.DBCount)
		return
	}
	if report.Consistent() {
		logger.Info("reconciliation clean: metadata rows=%d, storage objects=%d", report.DBCount, report.StorageCount)
		return
	}
	logger.Warn("reconciliation found drift: metadata=%d storage=%d missing_in_storage=%d orphan_in_storage=%d",
		report.DBCount, report.StorageCount, len(report.MissingInStorage), len(report.OrphanInStorage))
	if len(report.MissingInStorage) > 0 {
		logger.Warn("reconciliation sample: metadata without object: %s", strings.Join(report.MissingInStorage, ", "))
	}
	if len(report.OrphanInStorage) > 0 {
		logger.Warn("reconciliation sample: object without metadata: %s", strings.Join(report.OrphanInStorage, ", "))
	}
}

func appendSample(samples []string, path string, limit int, report *ReconcileReport) []string {
	if len(samples) >= limit {
		report.Truncated = true
		return samples
	}
	return append(samples, path)
}

// canonicalObjectPath normalizes both metadata paths and storage keys to a
// single form: leading slash, no trailing slash ("/dir/file.txt"). Metadata
// stores "/dir/file.txt" while backends enumerate "dir/file.txt".
func canonicalObjectPath(path string) string {
	trimmed := strings.Trim(strings.TrimSpace(path), "/")
	if trimmed == "" {
		return "/"
	}
	return "/" + trimmed
}
