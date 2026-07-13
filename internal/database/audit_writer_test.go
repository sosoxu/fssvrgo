package database

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sosoxu/fssvrgo/internal/utils"
)

// countAuditRows returns the number of rows in audit_log. Tests use this to
// verify that the AuditWriter actually persisted entries.
func countAuditRows(t *testing.T, db *DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM audit_log").Scan(&n); err != nil {
		t.Fatalf("count audit_log: %v", err)
	}
	return n
}

func newAuditEntry(op string) *AuditLog {
	return &AuditLog{
		ID:             utils.GenerateUUID(),
		Timestamp:      utils.GetCurrentTimestamp(),
		Operation:      op,
		ResourcePath:   "/test/" + op,
		UserIdentifier: "test-user",
		ClientIP:       "127.0.0.1",
		UserAgent:      "test-ua",
		Success:        true,
		Details:        "",
	}
}

// TestAuditWriter_BatchFlushOnSize verifies that the writer flushes a batch as
// soon as it reaches batchSize entries, without waiting for the flush ticker.
func TestAuditWriter_BatchFlushOnSize(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	const batchSize = 5
	w := NewAuditWriter(db, batchSize, 10*time.Second)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})

	for i := 0; i < batchSize; i++ {
		w.Submit(newAuditEntry(fmt.Sprintf("op%d", i)))
	}

	// The batch should be flushed within ~100ms (the flush is synchronous once
	// batchSize is hit). Poll for up to 1s.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := countAuditRows(t, db); got == batchSize {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected %d rows after batch flush, got %d", batchSize, countAuditRows(t, db))
}

// TestAuditWriter_FlushOnInterval verifies that entries are flushed by the
// periodic ticker even when batchSize is not reached.
func TestAuditWriter_FlushOnInterval(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	w := NewAuditWriter(db, 100, 100*time.Millisecond)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})

	// Submit fewer than batchSize entries; they should still flush via ticker.
	w.Submit(newAuditEntry("tick1"))
	w.Submit(newAuditEntry("tick2"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := countAuditRows(t, db); got == 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected 2 rows after interval flush, got %d", countAuditRows(t, db))
}

// TestAuditWriter_CloseFlushesPending verifies that Close() flushes any entries
// still in the pending buffer (not yet flushed by batch-size or ticker).
func TestAuditWriter_CloseFlushesPending(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	// Long flush interval so entries only flush on Close.
	w := NewAuditWriter(db, 100, 10*time.Second)

	w.Submit(newAuditEntry("close1"))
	w.Submit(newAuditEntry("close2"))
	w.Submit(newAuditEntry("close3"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := countAuditRows(t, db); got != 3 {
		t.Errorf("expected 3 rows after Close flush, got %d", got)
	}
}

// TestAuditWriter_CloseIdempotent verifies that calling Close multiple times is
// safe and does not double-flush or panic.
func TestAuditWriter_CloseIdempotent(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	w := NewAuditWriter(db, 100, 10*time.Second)
	w.Submit(newAuditEntry("once"))

	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel1()
	if err := w.Close(ctx1); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	// Second Close should be a no-op (loop already exited).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := w.Close(ctx2); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	if got := countAuditRows(t, db); got != 1 {
		t.Errorf("expected exactly 1 row (no double-flush), got %d", got)
	}
}

// TestAuditWriter_NilDBIsNoop verifies that a nil DB produces a no-op writer:
// Submit is safe and Close is safe, and nothing is persisted.
func TestAuditWriter_NilDBIsNoop(t *testing.T) {
	w := NewAuditWriter(nil, 10, time.Second)

	// These must not panic.
	w.Submit(newAuditEntry("noop"))
	w.Submit(newAuditEntry("noop2"))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Errorf("Close on nil-db writer should be nil, got %v", err)
	}
}

// TestAuditWriter_ConcurrentSubmitsAreSafe verifies that Submit is safe under
// concurrent access from many goroutines (the real HTTP server calls it from
// every request handler). batchSize is chosen large enough that the channel
// buffer (batchSize*4) can hold every entry, so none are dropped by the
// backpressure path — the test is about concurrency safety, not dropping.
func TestAuditWriter_ConcurrentSubmitsAreSafe(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	const goroutines = 50
	const perG = 20
	const total = goroutines * perG // 1000
	// batchSize large so buffer (batchSize*4) >= total and nothing is dropped.
	w := NewAuditWriter(db, total, 200*time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				w.Submit(newAuditEntry("concurrent"))
			}
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := countAuditRows(t, db); got != total {
		t.Errorf("expected %d rows, got %d", total, got)
	}
}

// TestAuditWriter_DBClosedDegradesGracefully verifies that when the DB starts
// rejecting writes (simulated by closing the underlying connection), Submit
// still does not block the caller and the writer logs-and-drops rather than
// wedging. We use a real (in-memory) DB and close it after creating the writer.
func TestAuditWriter_DBClosedDegradesGracefully(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	w := NewAuditWriter(db, 5, 100*time.Millisecond)

	// Submit a batch that flushes successfully.
	for i := 0; i < 5; i++ {
		w.Submit(newAuditEntry("ok"))
	}
	// Wait for the batch flush.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := countAuditRows(t, db); got == 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now close the underlying DB so subsequent writes fail. The writer must
	// not wedge — Submit returns immediately and the flush goroutine logs the
	// error and drops the batch.
	db.Underlying().Close()

	// Submit more entries; these will fail to persist.
	for i := 0; i < 5; i++ {
		w.Submit(newAuditEntry("fail"))
	}

	// Close must still return within the deadline (not wedge).
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close should return nil even when DB is broken, got %v", err)
	}
}

// TestAuditWriter_SubmitDoesNotBlockWhenBufferFull verifies that when the
// channel buffer is full, Submit drops new entries rather than blocking. This
// is the contract that protects the request path from backpressure — a slow
// (or stuck) DB must never stall request handlers.
//
// We do not assert an exact persisted count: the loop concurrently drains the
// channel, so the number of entries that make it through depends on scheduling.
// What we assert is that Submit returns promptly (never blocks) even when asked
// to submit far more than the buffer can hold, and that Close still succeeds.
func TestAuditWriter_SubmitDoesNotBlockWhenBufferFull(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	// batchSize=1 → buffer=4. Long flush interval so the loop flushes rarely
	// and the channel stays near full.
	w := NewAuditWriter(db, 1, 10*time.Second)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			w.Submit(newAuditEntry("fill"))
		}
		close(done)
	}()
	select {
	case <-done:
		// good: Submit never blocked
	case <-time.After(3 * time.Second):
		t.Fatal("Submit blocked when buffer was full; request path would stall")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := countAuditRows(t, db)
	// At least one entry should have made it through; at most 200 (all of them
	// in the unlikely case the loop drained everything in time).
	if got == 0 || got > 200 {
		t.Errorf("expected 1..200 rows persisted, got %d", got)
	}
}
