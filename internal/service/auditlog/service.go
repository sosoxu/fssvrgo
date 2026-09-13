// Package auditlog owns the audit trail: writing entries (through an
// asynchronous, best-effort writer) and querying them back.
//
// It exists so the HTTP layer stops constructing persistence services inside
// request handlers: handlers map requests to Entry values, and this package
// decides how they are logged, buffered, and stored.
package auditlog

import (
	"context"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/logger"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// Store is the query-side data-access seam. database.AuditLogService
// satisfies it; tests can supply an in-memory fake.
type Store interface {
	List(ctx context.Context, operation, resourcePath string, page, pageSize int) ([]database.AuditLog, error)
}

// Writer is the write-side seam: an asynchronous sink that batches entries so
// a request never blocks on an audit INSERT. *database.AuditWriter satisfies
// it. A nil Writer makes Record a no-op and List skip the flush.
type Writer interface {
	Submit(entry *database.AuditLog)
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

// Entry is one audit record in domain terms. The request-scoped fields
// (identifier, IP, user agent, request id) are supplied by the caller because
// only the transport layer knows them.
type Entry struct {
	Operation      string
	ResourcePath   string
	UserIdentifier string
	ClientIP       string
	UserAgent      string
	RequestID      string
	Success        bool
	Details        string
}

type Service struct {
	store  Store
	writer Writer
}

func NewService(store Store, writer Writer) *Service {
	return &Service{store: store, writer: writer}
}

// Record logs the entry to the process log immediately and submits a copy to
// the async writer for persistence. The process log is the immediate record;
// the DB row is the queryable best-effort copy, so a full buffer drops the row
// with a warning rather than stalling the request.
func (s *Service) Record(entry Entry) {
	logger.Info("AUDIT: req_id=%s operation=%s resource=%s user=%s ip=%s ua=%s success=%v details=%s",
		entry.RequestID, entry.Operation, entry.ResourcePath, entry.UserIdentifier,
		entry.ClientIP, entry.UserAgent, entry.Success, entry.Details)

	if s.writer == nil {
		return
	}
	s.writer.Submit(&database.AuditLog{
		ID:             utils.GenerateUUID(),
		Timestamp:      utils.GetCurrentTimestamp(),
		Operation:      entry.Operation,
		ResourcePath:   entry.ResourcePath,
		UserIdentifier: entry.UserIdentifier,
		ClientIP:       entry.ClientIP,
		UserAgent:      entry.UserAgent,
		Success:        entry.Success,
		Details:        entry.Details,
	})
}

// List returns one page of audit rows. It flushes pending async writes first
// so a caller that just performed an operation observes it instead of racing
// the writer's flush interval.
func (s *Service) List(ctx context.Context, operation, resourcePath string, page, pageSize int) ([]database.AuditLog, error) {
	if s.writer != nil {
		if err := s.writer.Flush(ctx); err != nil {
			logger.Warn("failed to flush audit buffer before query: %v", err)
		}
	}

	logs, err := s.store.List(ctx, operation, resourcePath, page, pageSize)
	if err != nil {
		return nil, err
	}
	if logs == nil {
		logs = []database.AuditLog{}
	}
	return logs, nil
}

// Close flushes pending entries and stops the background writer.
func (s *Service) Close(ctx context.Context) error {
	if s.writer == nil {
		return nil
	}
	return s.writer.Close(ctx)
}
