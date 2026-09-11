package database

import (
	"context"
	"time"

	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

type CleanupService struct {
	db               *DB
	store            storage.StorageAdapter
	interval         time.Duration
	retention        time.Duration
	stopCh           chan struct{}
	reconciler       *Reconciler
	reconcileEnabled bool
	reconcileRepair  bool
}

func NewCleanupService(db *DB, store storage.StorageAdapter, intervalMinutes, retentionDays int) *CleanupService {
	return &CleanupService{
		db:        db,
		store:     store,
		interval:  time.Duration(intervalMinutes) * time.Minute,
		retention: time.Duration(retentionDays) * 24 * time.Hour,
		stopCh:    make(chan struct{}),
	}
}

func (s *CleanupService) Start() {
	go s.run()
	logger.Info("Cleanup service started (interval=%v, retention=%v)", s.interval, s.retention)
}

// EnableReconcile turns on periodic metadata/storage reconciliation. Repair is
// opt-in because it removes objects and soft-deletes metadata rows.
func (s *CleanupService) EnableReconcile(repair bool) {
	s.reconciler = NewReconciler(s.db, s.store)
	s.reconcileEnabled = true
	s.reconcileRepair = repair
	logger.Info("Reconciliation enabled (repair=%t, interval=%v)", repair, s.interval)
}

// ReconcileNow runs one reconciliation pass. It always reports; it only repairs
// when reconciliation was enabled together with repair, so a startup scan is
// read-only unless the operator explicitly asked otherwise.
func (s *CleanupService) ReconcileNow(ctx context.Context) (*ReconcileReport, error) {
	if s.reconciler == nil {
		s.reconciler = NewReconciler(s.db, s.store)
	}
	report, err := s.reconciler.Scan(ctx)
	if err != nil {
		return nil, err
	}
	s.reconciler.LogReport(report)

	if s.reconcileEnabled && s.reconcileRepair && !report.Consistent() {
		result, repairErr := s.reconciler.Repair(ctx, report)
		if result != nil {
			logger.Info("Reconciliation repair: orphans_removed=%d dangling_marked=%d errors=%d",
				result.OrphansRemoved, result.DanglingMarked, len(result.Errors))
			for _, repairMsg := range result.Errors {
				logger.Error("Reconciliation repair failed: %s", repairMsg)
			}
		}
		if repairErr != nil {
			return report, repairErr
		}
	}
	return report, nil
}

func (s *CleanupService) Stop() {
	close(s.stopCh)
	logger.Info("Cleanup service stopped")
}

func (s *CleanupService) run() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanup()
		case <-s.stopCh:
			return
		}
	}
}

func (s *CleanupService) cleanup() {
	if s.reconcileEnabled {
		if _, err := s.ReconcileNow(context.Background()); err != nil {
			logger.Error("Reconciliation failed: %v", err)
		}
	}

	cutoff := time.Now().Add(-s.retention)

	// Clean up soft-deleted files
	files, err := s.getDeletedFiles(cutoff)
	if err != nil {
		logger.Error("Failed to get deleted files for cleanup: %v", err)
		return
	}

	for _, file := range files {
		// Delete storage file. storage.IsNotExist works for both local
		// (os.ErrNotExist) and MinIO (NoSuchKey) backends; using os.IsNotExist
		// here would only handle the local filesystem backend.
		if err := s.store.Remove(file.Path); err != nil && !storage.IsNotExist(err) {
			logger.Error("Failed to remove storage file %s: %v", file.Path, err)
			continue
		}

		// Permanently delete from database
		if err := s.permanentlyDeleteFile(file.ID); err != nil {
			logger.Error("Failed to permanently delete file record %s: %v", file.ID, err)
			continue
		}

		logger.Info("Permanently deleted file: %s (id=%s)", file.Path, file.ID)
	}

	// Clean up soft-deleted directories
	dirs, err := s.getDeletedDirectories(cutoff)
	if err != nil {
		logger.Error("Failed to get deleted directories for cleanup: %v", err)
		return
	}

	for _, dir := range dirs {
		if err := s.permanentlyDeleteDirectory(dir.ID); err != nil {
			logger.Error("Failed to permanently delete directory record %s: %v", dir.ID, err)
			continue
		}

		logger.Info("Permanently deleted directory: %s (id=%s)", dir.Path, dir.ID)
	}

	if len(files) > 0 || len(dirs) > 0 {
		logger.Info("Cleanup completed: %d files, %d directories permanently deleted", len(files), len(dirs))
	}
}

type deletedFile struct {
	ID   string
	Path string
}

type deletedDirectory struct {
	ID   string
	Path string
}

func (s *CleanupService) getDeletedFiles(cutoff time.Time) ([]deletedFile, error) {
	query := "SELECT id, path FROM files WHERE is_deleted = TRUE AND updated_at < ?"

	rows, err := s.db.Query(query, cutoff.Format("2006-01-02 15:04:05"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []deletedFile
	for rows.Next() {
		var f deletedFile
		if err := rows.Scan(&f.ID, &f.Path); err != nil {
			continue
		}
		files = append(files, f)
	}
	return files, nil
}

func (s *CleanupService) getDeletedDirectories(cutoff time.Time) ([]deletedDirectory, error) {
	query := "SELECT id, path FROM directories WHERE is_deleted = TRUE AND updated_at < ?"

	rows, err := s.db.Query(query, cutoff.Format("2006-01-02 15:04:05"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dirs []deletedDirectory
	for rows.Next() {
		var d deletedDirectory
		if err := rows.Scan(&d.ID, &d.Path); err != nil {
			continue
		}
		dirs = append(dirs, d)
	}
	return dirs, nil
}

func (s *CleanupService) permanentlyDeleteFile(id string) error {
	query := "DELETE FROM files WHERE id = ?"
	_, err := s.db.Exec(query, id)
	return err
}

func (s *CleanupService) permanentlyDeleteDirectory(id string) error {
	query := "DELETE FROM directories WHERE id = ?"
	_, err := s.db.Exec(query, id)
	return err
}
