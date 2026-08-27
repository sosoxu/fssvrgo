package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/distributed"
	"github.com/sosoxu/fssvrgo/internal/service/directory"
	"github.com/sosoxu/fssvrgo/internal/service/filemanager"
	"github.com/sosoxu/fssvrgo/internal/service/transfer"
	"github.com/sosoxu/fssvrgo/internal/storage"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

func setupNamespacePostgreSQL(t *testing.T) (*database.Database, *database.DB) {
	t.Helper()
	dbObj, qdb := requirePostgreSQLTestDB(t, 25)
	if err := database.InitTables(qdb); err != nil {
		dbObj.Close()
		t.Fatalf("initialize namespace test tables: %v", err)
	}
	t.Cleanup(func() {
		resetPostgreSQLTestDB(qdb)
		dbObj.Close()
	})
	return dbObj, qdb
}

func releaseTestNamespaceLease(t *testing.T, lease *database.NamespaceLease) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release namespace lease: %v", err)
	}
}

func TestPostgreSQL_NamespaceSharedLocksDoNotSerializeSiblingFiles(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first, err := qdb.AcquireNamespaceLease(ctx, database.FileNamespaceRequests("foo/a.txt"), time.Second)
	if err != nil {
		t.Fatalf("acquire first shared lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, first)

	started := time.Now()
	second, err := qdb.AcquireNamespaceLease(ctx, database.FileNamespaceRequests("foo/b.txt"), time.Second)
	if err != nil {
		t.Fatalf("acquire sibling shared lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, second)
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("sibling shared lease was unexpectedly serialized: %s", elapsed)
	}
}

func TestPostgreSQL_RootFileGetsNamespaceFence(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	lease, err := qdb.AcquireNamespaceLease(context.Background(), database.FileNamespaceRequests("root.txt"), time.Second)
	if err != nil {
		t.Fatalf("acquire root file namespace lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, lease)
	if lease.Token() == "" || lease.FenceToken() <= 0 {
		t.Fatalf("root file lease has no fencing identity: token=%q fence=%d", lease.Token(), lease.FenceToken())
	}
	var path string
	if err := qdb.QueryRow("SELECT path FROM namespace_lock_holders WHERE token = ?", lease.Token()).Scan(&path); err != nil {
		t.Fatalf("query root file holder: %v", err)
	}
	if path != "root.txt" {
		t.Fatalf("root file holder path = %q, want root.txt", path)
	}
}

func TestPostgreSQL_StaleNamespaceFenceCannotBeginWrite(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	stale, err := qdb.AcquireNamespaceLease(context.Background(), database.FileNamespaceRequests("fence/file.txt"), 30*time.Second)
	if err != nil {
		t.Fatalf("acquire stale candidate lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, stale)
	if _, err := qdb.Exec("UPDATE namespace_lock_holders SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE token = ?", stale.Token()); err != nil {
		t.Fatalf("expire stale candidate lease: %v", err)
	}

	current, err := qdb.AcquireNamespaceLease(context.Background(), database.DirectoryNamespaceRequests("fence"), time.Second)
	if err != nil {
		t.Fatalf("acquire replacement lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, current)
	if current.FenceToken() <= stale.FenceToken() {
		t.Fatalf("replacement fence %d is not newer than stale fence %d", current.FenceToken(), stale.FenceToken())
	}

	if _, err := stale.BeginFencedWrite(context.Background(), "fence/file.txt"); !errors.Is(err, database.ErrNamespaceLeaseGone) {
		t.Fatalf("stale fenced write error = %v, want ErrNamespaceLeaseGone", err)
	}
	tx, err := current.BeginFencedWrite(context.Background(), "fence/file.txt")
	if err != nil {
		t.Fatalf("current fenced write was rejected: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit current fenced write: %v", err)
	}
}

func TestPostgreSQL_NewerFenceRejectsStillLiveOldWriter(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	requests := database.FileNamespaceRequests("fence/shared.txt")
	old, err := qdb.AcquireNamespaceLease(context.Background(), requests, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire old shared lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, old)
	current, err := qdb.AcquireNamespaceLease(context.Background(), requests, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire current shared lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, current)
	if current.FenceToken() <= old.FenceToken() {
		t.Fatalf("current fence %d is not newer than old fence %d", current.FenceToken(), old.FenceToken())
	}

	tx, err := current.BeginFencedWrite(context.Background(), "fence/shared.txt")
	if err != nil {
		t.Fatalf("begin current fenced write: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit current fenced write: %v", err)
	}
	if err := old.Err(); err != nil {
		t.Fatalf("old lease should still be live for this test: %v", err)
	}
	if _, err := old.BeginFencedWrite(context.Background(), "fence/shared.txt"); !errors.Is(err, database.ErrNamespaceLeaseGone) {
		t.Fatalf("old live fenced write error = %v, want ErrNamespaceLeaseGone", err)
	}
}

func TestPostgreSQL_FencedFileTransactionBlocksParentLeaseAfterExpiry(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	fileLease, err := qdb.AcquireNamespaceLease(context.Background(), database.FileNamespaceRequests("tree/file.txt"), 30*time.Second)
	if err != nil {
		t.Fatalf("acquire file lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, fileLease)
	tx, err := fileLease.BeginFencedWrite(context.Background(), "tree/file.txt")
	if err != nil {
		t.Fatalf("begin fenced file transaction: %v", err)
	}
	if _, err := qdb.Exec("UPDATE namespace_lock_holders SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE token = ?", fileLease.Token()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("expire file holder: %v", err)
	}

	type leaseResult struct {
		lease *database.NamespaceLease
		err   error
	}
	result := make(chan leaseResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		lease, err := qdb.AcquireNamespaceLease(ctx, database.DirectoryNamespaceRequests("tree"), time.Second)
		result <- leaseResult{lease: lease, err: err}
	}()
	select {
	case got := <-result:
		_ = tx.Rollback()
		t.Fatalf("parent lease bypassed active fenced transaction: %v", got.err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback fenced file transaction: %v", err)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("acquire parent lease after rollback: %v", got.err)
		}
		releaseTestNamespaceLease(t, got.lease)
	case <-time.After(3 * time.Second):
		t.Fatal("parent lease did not continue after fenced transaction rollback")
	}
}

func TestPostgreSQL_ConcurrentNamespaceSharedAcquisitions(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	const workers = 8
	start := make(chan struct{})
	leases := make(chan *database.NamespaceLease, workers)
	errs := make(chan error, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			ready.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			lease, err := qdb.AcquireNamespaceLease(ctx, database.FileNamespaceRequests("foo/a.txt"), time.Second)
			if err != nil {
				errs <- err
				return
			}
			leases <- lease
		}()
	}
	ready.Wait()
	close(start)
	for i := 0; i < workers; i++ {
		select {
		case err := <-errs:
			t.Fatalf("acquire concurrent shared lease: %v", err)
		case lease := <-leases:
			defer releaseTestNamespaceLease(t, lease)
		}
	}
}

func TestPostgreSQL_DeepNamespaceLeaseRecordsAllHoldersAtomically(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	requests := database.FileNamespaceRequests("a/b/c/file.txt")
	lease, err := qdb.AcquireNamespaceLease(context.Background(), requests, time.Second)
	if err != nil {
		t.Fatalf("acquire deep namespace lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, lease)

	rows, err := qdb.Query("SELECT path, mode FROM namespace_lock_holders WHERE token = ? ORDER BY path", lease.Token())
	if err != nil {
		t.Fatalf("query deep namespace holders: %v", err)
	}
	defer rows.Close()
	got := make([]string, 0, len(requests))
	for rows.Next() {
		var path, mode string
		if err := rows.Scan(&path, &mode); err != nil {
			t.Fatalf("scan deep namespace holder: %v", err)
		}
		got = append(got, path+":"+mode)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate deep namespace holders: %v", err)
	}
	want := []string{"a:shared", "a/b:shared", "a/b/c:shared", "a/b/c/file.txt:shared"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("deep namespace holders = %v, want %v", got, want)
	}
}

func TestPostgreSQL_NamespaceLeaseBatchCleansExpiredHolders(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	if _, err := qdb.Exec(`INSERT INTO namespace_lock_holders (path, token, mode, expires_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP - INTERVAL '1 second')`, "expired/path", "expired-token", "exclusive"); err != nil {
		t.Fatalf("insert expired namespace holder: %v", err)
	}

	lease, err := qdb.AcquireNamespaceLease(context.Background(), []database.NamespaceLockRequest{
		{Path: "expired/path", Mode: database.NamespaceShared},
	}, time.Second)
	if err != nil {
		t.Fatalf("acquire lease over expired holder: %v", err)
	}
	defer releaseTestNamespaceLease(t, lease)

	var expired, current int
	if err := qdb.QueryRow("SELECT COUNT(*) FROM namespace_lock_holders WHERE token = ?", "expired-token").Scan(&expired); err != nil {
		t.Fatalf("count expired holder: %v", err)
	}
	if err := qdb.QueryRow("SELECT COUNT(*) FROM namespace_lock_holders WHERE token = ?", lease.Token()).Scan(&current); err != nil {
		t.Fatalf("count current holder: %v", err)
	}
	if expired != 0 || current != 1 {
		t.Fatalf("holder counts after cleanup: expired=%d current=%d", expired, current)
	}
}

func TestPostgreSQL_NamespaceFencingMigrationInvalidatesLegacyHolder(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	if _, err := qdb.Exec(`INSERT INTO namespace_lock_holders (path, token, mode, expires_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP + INTERVAL '1 minute')`, "legacy/path", "legacy-token", "exclusive"); err != nil {
		t.Fatalf("insert legacy namespace holder: %v", err)
	}
	if err := database.InitNamespaceLockTable(qdb); err != nil {
		t.Fatalf("rerun namespace fencing migration: %v", err)
	}
	var count int
	if err := qdb.QueryRow("SELECT COUNT(*) FROM namespace_lock_holders WHERE token = ?", "legacy-token").Scan(&count); err != nil {
		t.Fatalf("count legacy holder: %v", err)
	}
	if count != 0 {
		t.Fatalf("legacy holder survived fencing migration: count=%d", count)
	}
}

func TestPostgreSQL_NamespaceExclusiveBlocksDescendantFile(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	shared, err := qdb.AcquireNamespaceLease(context.Background(), database.FileNamespaceRequests("foo/a.txt"), time.Second)
	if err != nil {
		t.Fatalf("acquire descendant shared lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = qdb.AcquireNamespaceLease(ctx, database.DirectoryNamespaceRequests("foo"), time.Second)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected exclusive directory lease to time out, got %v", err)
	}
	releaseTestNamespaceLease(t, shared)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	exclusive, err := qdb.AcquireNamespaceLease(ctx2, database.DirectoryNamespaceRequests("foo"), time.Second)
	if err != nil {
		t.Fatalf("acquire exclusive lease after release: %v", err)
	}
	releaseTestNamespaceLease(t, exclusive)
}

func TestPostgreSQL_NamespaceLeaseRenews(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	shared, err := qdb.AcquireNamespaceLease(context.Background(), database.FileNamespaceRequests("foo/a.txt"), 300*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire renewable lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, shared)
	time.Sleep(750 * time.Millisecond)
	if err := shared.Err(); err != nil {
		t.Fatalf("shared namespace lease was not renewed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = qdb.AcquireNamespaceLease(ctx, database.DirectoryNamespaceRequests("foo"), time.Second)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("renewed lease no longer blocked exclusive request: %v", err)
	}
}

func TestPostgreSQL_ExpiredNamespaceLeaseCannotResume(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	requests := database.FileNamespaceRequests("foo/a.txt")
	lease, err := qdb.AcquireNamespaceLease(context.Background(), requests, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire namespace lease: %v", err)
	}
	token := lease.Token()
	releaseTestNamespaceLease(t, lease)
	_, err = qdb.ResumeNamespaceLease(context.Background(), token, requests, time.Second)
	if !errors.Is(err, database.ErrNamespaceLeaseGone) {
		t.Fatalf("resume released lease error = %v, want ErrNamespaceLeaseGone", err)
	}

	deepRequests := database.FileNamespaceRequests("foo/bar/a.txt")
	incomplete, err := qdb.AcquireNamespaceLease(context.Background(), deepRequests, time.Second)
	if err != nil {
		t.Fatalf("acquire deep namespace lease: %v", err)
	}
	defer releaseTestNamespaceLease(t, incomplete)
	if _, err := qdb.Exec("DELETE FROM namespace_lock_holders WHERE token = ? AND path = ?", incomplete.Token(), "foo/bar"); err != nil {
		t.Fatalf("remove one holder path: %v", err)
	}
	_, err = qdb.ResumeNamespaceLease(context.Background(), incomplete.Token(), deepRequests, time.Second)
	if !errors.Is(err, database.ErrNamespaceLeaseGone) {
		t.Fatalf("resume incomplete lease error = %v, want ErrNamespaceLeaseGone", err)
	}
	var remaining int
	if err := qdb.QueryRow("SELECT COUNT(*) FROM namespace_lock_holders WHERE token = ?", incomplete.Token()).Scan(&remaining); err != nil {
		t.Fatalf("count incomplete holders: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("incomplete lease retained %d holder rows", remaining)
	}
}

func TestPostgreSQL_DirectoryRenameWaitsForChildNamespaceLease(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	dm := directory.NewDirectoryManagerWithDistLock(qdb, store, distributed.NewLocalDistributedLock())
	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("create foo: %v", err)
	}
	if err := store.Write("foo/a.txt", []byte("a")); err != nil {
		t.Fatalf("write child: %v", err)
	}

	shared, err := qdb.AcquireNamespaceLease(context.Background(), database.FileNamespaceRequests("foo/a.txt"), time.Second)
	if err != nil {
		t.Fatalf("acquire child namespace lease: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- dm.RenameDirectory("foo", "bar") }()

	select {
	case err := <-done:
		t.Fatalf("directory rename completed while child lease was held: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	releaseTestNamespaceLease(t, shared)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("directory rename after child release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("directory rename did not continue after child lease release")
	}
}

func TestPostgreSQL_RecursiveDeleteRollsBackMoreThanOneLegacyBatch(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	dm := directory.NewDirectoryManagerWithDistLock(qdb, store, distributed.NewLocalDistributedLock())
	if err := dm.CreateDirectory("bulk"); err != nil {
		t.Fatalf("create bulk: %v", err)
	}
	if err := dm.CreateDirectory("bulk/sub"); err != nil {
		t.Fatalf("create bulk/sub: %v", err)
	}
	if err := store.Write("bulk/sentinel.txt", []byte("keep")); err != nil {
		t.Fatalf("write storage sentinel: %v", err)
	}

	fileSvc := database.NewFileMetadataService(qdb)
	now := time.Now().UTC().Format(time.RFC3339)
	const fileCount = 1200
	for i := 0; i < fileCount; i++ {
		path := fmt.Sprintf("bulk/sub/file-%04d.txt", i)
		if err := fileSvc.Create(&database.FileMetadata{
			ID: utils.GenerateUUID(), Path: path, Name: fmt.Sprintf("file-%04d.txt", i),
			Size: 1, StorageType: "local", CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("create file metadata %d: %v", i, err)
		}
	}

	if _, err := qdb.Exec(`CREATE OR REPLACE FUNCTION fail_bulk_directory_delete() RETURNS trigger AS $$
		BEGIN
			IF OLD.path = 'bulk/sub' THEN
				RAISE EXCEPTION 'forced recursive delete failure';
			END IF;
			RETURN NEW;
		END;
	$$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create failure function: %v", err)
	}
	if _, err := qdb.Exec(`CREATE TRIGGER fail_bulk_directory_delete_trigger
		BEFORE UPDATE OF is_deleted ON directories
		FOR EACH ROW EXECUTE FUNCTION fail_bulk_directory_delete()`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = qdb.Exec("DROP TRIGGER IF EXISTS fail_bulk_directory_delete_trigger ON directories")
		_, _ = qdb.Exec("DROP FUNCTION IF EXISTS fail_bulk_directory_delete()")
	})

	if err := dm.DeleteDirectory("bulk", true); err == nil {
		t.Fatal("expected recursive delete transaction failure")
	}
	var liveFiles, liveDirectories int
	if err := qdb.QueryRow("SELECT COUNT(*) FROM files WHERE path LIKE ? AND is_deleted = FALSE", "bulk/%").Scan(&liveFiles); err != nil {
		t.Fatalf("count live files: %v", err)
	}
	if err := qdb.QueryRow("SELECT COUNT(*) FROM directories WHERE (path = ? OR path LIKE ?) AND is_deleted = FALSE", "bulk", "bulk/%").Scan(&liveDirectories); err != nil {
		t.Fatalf("count live directories: %v", err)
	}
	if liveFiles != fileCount || liveDirectories != 2 {
		t.Fatalf("recursive rollback left live files=%d/%d directories=%d/2", liveFiles, fileCount, liveDirectories)
	}
	if !store.Exists("bulk/sentinel.txt") {
		t.Fatal("storage tree was removed before metadata transaction committed")
	}
}

func TestPostgreSQL_UploadCompletionLedgerIsCrossInstanceIdempotent(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	sessions := distributed.NewMemorySessionStore()
	locks := distributed.NewLocalDistributedLock()
	sharedTemp := t.TempDir()
	first := transfer.NewFileTransferServiceWithRedis(store, qdb, sessions, locks)
	second := transfer.NewFileTransferServiceWithRedis(store, qdb, sessions, locks)
	if err := first.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("set first temp dir: %v", err)
	}
	if err := second.SetTempDir(sharedTemp); err != nil {
		t.Fatalf("set second temp dir: %v", err)
	}
	data := []byte("postgresql terminal ledger")
	sessionID, err := first.CreateUploadSession("ledger/file.txt", "file.txt", int64(len(data)), "client", "")
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}
	if err := first.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("upload chunk: %v", err)
	}
	firstResult, err := first.CompleteUpload(sessionID)
	if err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := sessions.Delete(context.Background(), "upload", sessionID); err != nil {
		t.Fatalf("delete Redis-compatible cache state: %v", err)
	}
	secondResult, err := second.CompleteUpload(sessionID)
	if err != nil {
		t.Fatalf("second-instance idempotent completion: %v", err)
	}
	if *firstResult != *secondResult {
		t.Fatalf("ledger result changed: first=%+v second=%+v", firstResult, secondResult)
	}
}

func TestPostgreSQL_UploadCompletionRollsBackWhenLedgerWriteFails(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	svc := transfer.NewFileTransferService(store, qdb)
	if _, err := qdb.Exec(`CREATE OR REPLACE FUNCTION fail_upload_terminal_insert() RETURNS trigger AS $$
		BEGIN
			IF NEW.session_type = 'upload' THEN
				RAISE EXCEPTION 'forced terminal ledger failure';
			END IF;
			RETURN NEW;
		END;
	$$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create failure function: %v", err)
	}
	if _, err := qdb.Exec(`CREATE TRIGGER fail_upload_terminal_insert_trigger
		BEFORE INSERT ON transfer_session_results
		FOR EACH ROW EXECUTE FUNCTION fail_upload_terminal_insert()`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = qdb.Exec("DROP TRIGGER IF EXISTS fail_upload_terminal_insert_trigger ON transfer_session_results")
		_, _ = qdb.Exec("DROP FUNCTION IF EXISTS fail_upload_terminal_insert()")
	})

	data := []byte("postgresql rollback")
	sessionID, err := svc.CreateUploadSession("ledger-failure.txt", "ledger-failure.txt", int64(len(data)), "client", "")
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}
	if err := svc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("upload chunk: %v", err)
	}
	if _, err := svc.CompleteUpload(sessionID); err == nil {
		t.Fatal("expected terminal ledger failure")
	}
	meta, err := database.NewFileMetadataService(qdb).GetByPath("ledger-failure.txt")
	if err != nil {
		t.Fatalf("get rolled-back metadata: %v", err)
	}
	if meta != nil || store.Exists("ledger-failure.txt") {
		t.Fatalf("failed transaction left metadata=%v storageExists=%v", meta, store.Exists("ledger-failure.txt"))
	}
}

func TestPostgreSQL_DirectoryRenameWaitsForActiveRead(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	distLock := distributed.NewLocalDistributedLock()
	dm := directory.NewDirectoryManagerWithDistLock(qdb, store, distLock)
	fm := filemanager.NewFileManagerWithDistLock(store, qdb, distLock)
	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("create foo: %v", err)
	}
	if _, err := fm.UploadFile("foo/a.txt", []byte("a")); err != nil {
		t.Fatalf("upload child: %v", err)
	}

	_, releaseRead, err := fm.BeginRead("foo/a.txt")
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- dm.RenameDirectory("foo", "bar") }()
	select {
	case err := <-done:
		t.Fatalf("rename completed during active read: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	releaseRead()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename after read release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rename did not continue after read release")
	}
}

func TestPostgreSQL_DownloadSessionHoldsNamespaceLease(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	distLock := distributed.NewLocalDistributedLock()
	dm := directory.NewDirectoryManagerWithDistLock(qdb, store, distLock)
	fm := filemanager.NewFileManagerWithDistLock(store, qdb, distLock)
	downloads := transfer.NewFileTransferServiceWithRedis(store, qdb, distributed.NewMemorySessionStore(), distLock)
	t.Cleanup(downloads.StopCleanupThread)
	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("create foo: %v", err)
	}
	if _, err := fm.UploadFile("foo/a.txt", []byte("abcdef")); err != nil {
		t.Fatalf("upload child: %v", err)
	}

	sessionID, err := downloads.CreateDownloadSession("foo/a.txt", "client")
	if err != nil {
		t.Fatalf("create download session: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- dm.RenameDirectory("foo", "bar") }()
	select {
	case err := <-done:
		t.Fatalf("rename completed during active download session: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := downloads.CompleteDownload(sessionID); err != nil {
		t.Fatalf("complete download: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename after download completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rename did not continue after download completion")
	}
}

func TestPostgreSQL_DownloadSessionLeaseTransfersAcrossInstances(t *testing.T) {
	_, qdb := setupNamespacePostgreSQL(t)
	store := storage.NewLocalStorage(t.TempDir())
	distLock := distributed.NewLocalDistributedLock()
	sessions := distributed.NewMemorySessionStore()
	dm := directory.NewDirectoryManagerWithDistLock(qdb, store, distLock)
	fm := filemanager.NewFileManagerWithDistLock(store, qdb, distLock)
	first := transfer.NewFileTransferServiceWithRedis(store, qdb, sessions, distLock)
	second := transfer.NewFileTransferServiceWithRedis(store, qdb, sessions, distLock)
	third := transfer.NewFileTransferServiceWithRedis(store, qdb, sessions, distLock)
	t.Cleanup(first.StopCleanupThread)
	t.Cleanup(second.StopCleanupThread)
	t.Cleanup(third.StopCleanupThread)
	if err := dm.CreateDirectory("foo"); err != nil {
		t.Fatalf("create foo: %v", err)
	}
	if _, err := fm.UploadFile("foo/a.txt", []byte("abcdef")); err != nil {
		t.Fatalf("upload child: %v", err)
	}

	sessionID, err := first.CreateDownloadSession("foo/a.txt", "client")
	if err != nil {
		t.Fatalf("create download session: %v", err)
	}
	if _, err := second.DownloadChunk(sessionID, 3, 0); err != nil {
		t.Fatalf("resume download on second instance: %v", err)
	}
	var persisted transfer.DownloadSession
	if err := sessions.Get(context.Background(), "download", sessionID, &persisted); err != nil {
		t.Fatalf("load persisted download session: %v", err)
	}
	if _, err := qdb.Exec("DELETE FROM namespace_lock_holders WHERE token = ?", persisted.NamespaceLeaseToken); err != nil {
		t.Fatalf("simulate expired namespace holder: %v", err)
	}
	if _, err := third.DownloadChunk(sessionID, 3, 3); err != nil {
		t.Fatalf("recover expired holder on third instance: %v", err)
	}
	if err := third.CompleteDownload(sessionID); err != nil {
		t.Fatalf("complete download on third instance: %v", err)
	}
	if _, err := first.DownloadChunk(sessionID, 3, 0); err == nil {
		t.Fatal("first instance accepted a chunk after the transferred session completed")
	}

	done := make(chan error, 1)
	go func() { done <- dm.RenameDirectory("foo", "bar") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename after transferred download completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original instance kept renewing the transferred download lease")
	}
}
