package database

import (
	"context"
	"sync"
	"time"

	"github.com/sosoxu/fssvrgo/internal/logger"
)

// AuditWriter asynchronously batches audit-log INSERTs so that request handlers
// never block on the audit write. Entries are pushed onto a buffered channel; a
// background goroutine flushes them in batches (either when a batch reaches
// batchSize, or every flushInterval — whichever comes first).
//
// Trade-off: if the process crashes, entries still in the channel or the
// pending batch are lost. Audit logs are not a strong-consistency store, so
// this is acceptable; the synchronous logger.Info call in auditLog() remains
// the immediate record, and the DB row is the durable-but-best-effort copy.
type AuditWriter struct {
	svc *AuditLogService

	ch       chan *AuditLog
	batchSize int
	flushInterval time.Duration

	cancel context.CancelFunc
	done   chan struct{}

	// mu guards pending so that the flush goroutine and an explicit Close
	// (which does a final flush) cannot race.
	mu      sync.Mutex
	pending []*AuditLog
}

// NewAuditWriter creates an AuditWriter backed by the given DB. The writer is
// started immediately and must be Closed before the process exits to flush
// remaining entries. A nil db produces a no-op writer (audit writes are
// skipped) — this mirrors the legacy behavior where auditLog() skipped
// persistence when s.db was nil.
func NewAuditWriter(db *DB, batchSize int, flushInterval time.Duration) *AuditWriter {
	if batchSize <= 0 {
		batchSize = 100
	}
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	w := &AuditWriter{
		svc:           NewAuditLogService(db),
		ch:            make(chan *AuditLog, batchSize*4),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		done:          make(chan struct{}),
	}
	if db == nil {
		// No DB — be a no-op. Still expose a valid Close() for symmetry.
		close(w.done)
		return w
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go w.loop(ctx)
	return w
}

// Submit enqueues an audit-log entry for asynchronous persistence. It never
// blocks for longer than the channel buffer allows; if the buffer is full the
// entry is dropped (with a warning) rather than blocking the request path,
// because audit logging must never stall a user-facing operation.
func (w *AuditWriter) Submit(entry *AuditLog) {
	if w == nil || w.svc == nil || w.svc.db == nil {
		return
	}
	select {
	case w.ch <- entry:
	default:
		logger.Warn("audit writer buffer full; dropping audit entry (op=%s resource=%s)", entry.Operation, entry.ResourcePath)
	}
}

// Close flushes all pending entries and stops the background goroutine. It is
// safe to call multiple times. The caller should give Close a bounded deadline
// (e.g. 5s) so a slow DB does not stall shutdown indefinitely.
func (w *AuditWriter) Close(ctx context.Context) error {
	if w == nil || w.svc == nil || w.svc.db == nil {
		return nil
	}
	if w.cancel != nil {
		w.cancel()
	}
	// Wait for the loop to exit. The loop drains the channel into w.pending on
	// its way out but does NOT flush — Close does the final flush so it can use
	// the caller's (still-valid) context. This matters because the loop's own
	// context is cancelled here, and a final flush under a cancelled context
	// would no-op the writeBatch.
	select {
	case <-w.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Final flush of whatever the loop drained into pending.
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) > 0 {
		w.flushLocked(ctx)
	}
	return nil
}

func (w *AuditWriter) loop(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case entry := <-w.ch:
			w.mu.Lock()
			w.pending = append(w.pending, entry)
			if len(w.pending) >= w.batchSize {
				w.flushLocked(ctx)
			}
			w.mu.Unlock()
		case <-ticker.C:
			w.mu.Lock()
			if len(w.pending) > 0 {
				w.flushLocked(ctx)
			}
			w.mu.Unlock()
		case <-ctx.Done():
			// Drain the channel into pending; Close will do the final flush
			// using the caller's still-valid context (the loop's own ctx is
			// now cancelled, so flushing here would no-op writeBatch).
			w.mu.Lock()
			drained := true
			for drained {
				select {
				case entry := <-w.ch:
					w.pending = append(w.pending, entry)
				default:
					drained = false
				}
			}
			w.mu.Unlock()
			return
		}
	}
}

// flushLocked writes all pending entries to the DB. It must be called with
// w.mu held. On failure it logs and clears the batch (dropping entries) rather
// than retrying forever, so a persistently broken DB does not wedge the writer.
func (w *AuditWriter) flushLocked(ctx context.Context) {
	batch := w.pending
	w.pending = nil
	if len(batch) == 0 {
		return
	}
	if err := w.writeBatch(ctx, batch); err != nil {
		logger.Error("audit batch write failed (size=%d): %v", len(batch), err)
	}
}

// writeBatch persists a batch. It does not use a single multi-row INSERT
// because the DB dialect translation path (DB.Exec) caches prepared statements
// by query string; a multi-row INSERT with a variable placeholder count would
// defeat that cache. A per-row loop under one logical "batch" still avoids the
// per-request round-trip and lets the DB batch the writes internally.
func (w *AuditWriter) writeBatch(ctx context.Context, batch []*AuditLog) error {
	for _, entry := range batch {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := w.svc.Create(entry); err != nil {
			// Keep going on a single-row failure so one bad row does not drop
			// the rest of the batch; the per-row error is logged for triage.
			logger.Warn("audit row write failed (op=%s resource=%s): %v", entry.Operation, entry.ResourcePath, err)
		}
	}
	return nil
}
