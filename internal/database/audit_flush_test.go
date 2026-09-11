package database

import (
	"context"
	"testing"
	"time"
)

// TestAuditWriterFlushClosesVisibilityWindow covers issue #122: entries sit in
// the writer's buffer until a batch fills or the ticker fires, so a query that
// must see a just-written entry has to flush first.
func TestAuditWriterFlushClosesVisibilityWindow(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}

	// Large batch size and long interval: nothing is flushed unless we ask.
	w := NewAuditWriter(db, 100, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	w.Submit(newAuditEntry("flush-visibility"))

	svc := NewAuditLogService(db)
	before, err := svc.List("flush-visibility", "", 1, 10)
	if err != nil {
		t.Fatalf("List before flush: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("expected the entry to still be buffered before flush, got %d row(s)", len(before))
	}

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	after, err := svc.List("flush-visibility", "", 1, 10)
	if err != nil {
		t.Fatalf("List after flush: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("expected 1 row after flush, got %d", len(after))
	}
}

func TestAuditWriterFlushIsNoopWhenEmpty(t *testing.T) {
	db := newTestDB(t)
	if err := InitTables(db); err != nil {
		t.Fatalf("InitTables: %v", err)
	}
	w := NewAuditWriter(db, 100, time.Hour)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	if err := w.Flush(context.Background()); err != nil {
		t.Fatalf("Flush on empty buffer: %v", err)
	}
}
