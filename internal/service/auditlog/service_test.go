package auditlog

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
)

type fakeStore struct {
	logs     []database.AuditLog
	lastArgs []interface{}
	err      error
}

func (f *fakeStore) List(_ context.Context, operation, resourcePath string, page, pageSize int) ([]database.AuditLog, error) {
	f.lastArgs = []interface{}{operation, resourcePath, page, pageSize}
	if f.err != nil {
		return nil, f.err
	}
	return f.logs, nil
}

type fakeWriter struct {
	mu       sync.Mutex
	submits  []*database.AuditLog
	flushes  int
	closed   int
	flushErr error
}

func (f *fakeWriter) Submit(entry *database.AuditLog) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submits = append(f.submits, entry)
}

func (f *fakeWriter) Flush(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	return f.flushErr
}

func (f *fakeWriter) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func TestRecordMapsEntryAndSubmits(t *testing.T) {
	store := &fakeStore{}
	writer := &fakeWriter{}
	svc := NewService(store, writer)

	svc.Record(Entry{
		Operation:      "upload",
		ResourcePath:   "/docs/a.txt",
		UserIdentifier: "api-key:***",
		ClientIP:       "10.0.0.7",
		UserAgent:      "curl/8",
		RequestID:      "req-1",
		Success:        true,
		Details:        "size=3",
	})

	if len(writer.submits) != 1 {
		t.Fatalf("submitted %d entries, want 1", len(writer.submits))
	}
	got := writer.submits[0]
	if got.Operation != "upload" || got.ResourcePath != "/docs/a.txt" || got.Success != true || got.Details != "size=3" {
		t.Errorf("submitted entry = %+v", got)
	}
	if got.ID == "" || got.Timestamp == "" {
		t.Error("ID and Timestamp must be stamped by the service")
	}
}

func TestRecordWithoutWriterIsNoop(t *testing.T) {
	svc := NewService(&fakeStore{}, nil)
	svc.Record(Entry{Operation: "upload"}) // must not panic
	if err := svc.Close(context.Background()); err != nil {
		t.Errorf("Close without writer = %v, want nil", err)
	}
}

func TestListFlushesBeforeQuerying(t *testing.T) {
	store := &fakeStore{logs: []database.AuditLog{{ID: "a1"}}}
	writer := &fakeWriter{}
	svc := NewService(store, writer)

	logs, err := svc.List(context.Background(), "upload", "/docs", 2, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if writer.flushes != 1 {
		t.Errorf("flushes = %d, want 1 (the query must not race the async writer)", writer.flushes)
	}
	if len(logs) != 1 || logs[0].ID != "a1" {
		t.Errorf("logs = %+v, want the store's rows", logs)
	}
	wantArgs := []interface{}{"upload", "/docs", 2, 10}
	for i, want := range wantArgs {
		if store.lastArgs[i] != want {
			t.Errorf("store arg %d = %v, want %v", i, store.lastArgs[i], want)
		}
	}
}

func TestListReturnsEmptySliceNotNull(t *testing.T) {
	svc := NewService(&fakeStore{}, nil)
	logs, err := svc.List(context.Background(), "", "", 1, 20)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if logs == nil {
		t.Error("List must return an empty slice, not nil, so the JSON response is [] rather than null")
	}
}

func TestListSurfacesStoreError(t *testing.T) {
	storeErr := errors.New("query failed")
	svc := NewService(&fakeStore{err: storeErr}, nil)
	if _, err := svc.List(context.Background(), "", "", 1, 20); !errors.Is(err, storeErr) {
		t.Errorf("err = %v, want the store error", err)
	}
}

func TestCloseForwardsToWriter(t *testing.T) {
	writer := &fakeWriter{}
	svc := NewService(&fakeStore{}, writer)
	if err := svc.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if writer.closed != 1 {
		t.Errorf("writer.Close called %d times, want 1", writer.closed)
	}
}
