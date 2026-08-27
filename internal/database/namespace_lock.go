package database

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sosoxu/fssvrgo/internal/utils"
)

type NamespaceLockMode string

const (
	NamespaceShared    NamespaceLockMode = "shared"
	NamespaceExclusive NamespaceLockMode = "exclusive"
)

type NamespaceLockRequest struct {
	Path string
	Mode NamespaceLockMode
}

type NamespaceLease struct {
	db         *DB
	token      string
	fenceToken int64
	paths      int64
	requests   []NamespaceLockRequest
	cancel     context.CancelFunc
	released   sync.Once
	mu         sync.RWMutex
	lost       error
}

func (l *NamespaceLease) Err() error {
	if l == nil {
		return errors.New("namespace lease is nil")
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lost
}

func (l *NamespaceLease) Token() string {
	if l == nil {
		return ""
	}
	return l.token
}

func (l *NamespaceLease) FenceToken() int64 {
	if l == nil {
		return 0
	}
	return l.fenceToken
}

// ValidateTx verifies the holder with the PostgreSQL clock in the same
// transaction as the protected metadata change.
func (l *NamespaceLease) ValidateTx(tx *Tx) error {
	if l == nil || l.db == nil || l.db.dialect != DialectPostgreSQL || l.token == "" {
		return nil
	}
	var count int
	err := tx.QueryRow(`SELECT COUNT(*) FROM namespace_lock_holders
		WHERE token = ? AND fence_token = ? AND expires_at > CURRENT_TIMESTAMP`, l.token, l.fenceToken).Scan(&count)
	if err != nil {
		return fmt.Errorf("validate namespace fencing token: %w", err)
	}
	if count != int(l.paths) {
		return fmt.Errorf("%w: fencing token %d", ErrNamespaceLeaseGone, l.fenceToken)
	}
	return nil
}

// BeginFencedWrite serializes final writes for the exact logical file paths
// and validates this lease before any storage mutation. The returned
// transaction remains open through the storage replacement and metadata
// commit. A newer writer therefore cannot commit between this validation and
// the old writer's final storage operation.
func (l *NamespaceLease) BeginFencedWrite(ctx context.Context, paths ...string) (*Tx, error) {
	if l == nil || l.db == nil || l.db.dialect != DialectPostgreSQL || l.token == "" || l.fenceToken <= 0 {
		return nil, errors.New("fenced write requires a PostgreSQL namespace lease")
	}
	ordered := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		normalized := normalizeNamespacePath(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; !ok {
			seen[normalized] = struct{}{}
			ordered = append(ordered, normalized)
		}
	}
	sort.Strings(ordered)
	if len(ordered) == 0 {
		return nil, errors.New("fenced write requires at least one path")
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin fenced write transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	for _, request := range l.requests {
		query := "SELECT pg_advisory_xact_lock_shared(?)"
		if request.Mode == NamespaceExclusive {
			query = "SELECT pg_advisory_xact_lock(?)"
		}
		if _, err := tx.ExecContext(ctx, query, namespaceAdvisoryKey(request.Path)); err != nil {
			return nil, fmt.Errorf("lock fenced namespace path %s: %w", request.Path, err)
		}
	}
	for _, value := range ordered {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(?)", namespaceAdvisoryKey("@write/"+value)); err != nil {
			return nil, fmt.Errorf("lock fenced write for %s: %w", value, err)
		}
	}
	if err := l.ValidateTx(tx); err != nil {
		return nil, err
	}
	for _, value := range ordered {
		if _, err := tx.ExecContext(ctx, `INSERT INTO namespace_fence_heads (path, fence_token, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT (path) DO UPDATE SET fence_token = EXCLUDED.fence_token, updated_at = CURRENT_TIMESTAMP
			WHERE namespace_fence_heads.fence_token < EXCLUDED.fence_token`, value, l.fenceToken); err != nil {
			return nil, fmt.Errorf("advance namespace fence for %s: %w", value, err)
		}
		var current int64
		if err := tx.QueryRowContext(ctx, "SELECT fence_token FROM namespace_fence_heads WHERE path = ?", value).Scan(&current); err != nil {
			return nil, fmt.Errorf("read namespace fence for %s: %w", value, err)
		}
		if current > l.fenceToken {
			return nil, fmt.Errorf("%w: fencing token %d is older than %d for %s", ErrNamespaceLeaseGone, l.fenceToken, current, value)
		}
	}
	rollback = false
	return tx, nil
}

func (l *NamespaceLease) markLost(err error) {
	l.mu.Lock()
	if l.lost == nil {
		l.lost = err
	}
	l.mu.Unlock()
}

func (l *NamespaceLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	var releaseErr error
	l.released.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
		if l.db != nil && l.token != "" {
			_, releaseErr = l.db.ExecContext(ctx, "DELETE FROM namespace_lock_holders WHERE token = ?", l.token)
		}
	})
	return releaseErr
}

func normalizeNamespacePath(value string) string {
	value = utils.NormalizePath(value)
	if value == "." || value == "" || value == "/" {
		return ""
	}
	return value
}

func namespaceAncestors(value string) []string {
	value = normalizeNamespacePath(value)
	if value == "" {
		return nil
	}
	var result []string
	for parent := path.Dir(value); parent != "." && parent != "/" && parent != ""; parent = path.Dir(parent) {
		result = append(result, parent)
	}
	return result
}

func FileNamespaceRequests(paths ...string) []NamespaceLockRequest {
	var requests []NamespaceLockRequest
	for _, value := range paths {
		for _, ancestor := range namespaceAncestors(value) {
			requests = append(requests, NamespaceLockRequest{Path: ancestor, Mode: NamespaceShared})
		}
		if normalized := normalizeNamespacePath(value); normalized != "" {
			requests = append(requests, NamespaceLockRequest{Path: normalized, Mode: NamespaceShared})
		}
	}
	return requests
}

func DirectoryNamespaceRequests(paths ...string) []NamespaceLockRequest {
	var requests []NamespaceLockRequest
	for _, value := range paths {
		for _, ancestor := range namespaceAncestors(value) {
			requests = append(requests, NamespaceLockRequest{Path: ancestor, Mode: NamespaceShared})
		}
		if normalized := normalizeNamespacePath(value); normalized != "" {
			requests = append(requests, NamespaceLockRequest{Path: normalized, Mode: NamespaceExclusive})
		}
	}
	return requests
}

func DownloadSessionNamespaceRequests(sessionID string, mode NamespaceLockMode) []NamespaceLockRequest {
	return []NamespaceLockRequest{{Path: "@download-session/" + sessionID, Mode: mode}}
}

func normalizeNamespaceRequests(requests []NamespaceLockRequest) []NamespaceLockRequest {
	modes := make(map[string]NamespaceLockMode)
	for _, request := range requests {
		normalized := normalizeNamespacePath(request.Path)
		if normalized == "" {
			continue
		}
		mode := request.Mode
		if mode != NamespaceExclusive {
			mode = NamespaceShared
		}
		if existing, ok := modes[normalized]; !ok || mode == NamespaceExclusive || existing != NamespaceExclusive {
			modes[normalized] = mode
		}
	}
	paths := make([]string, 0, len(modes))
	for value := range modes {
		paths = append(paths, value)
	}
	sort.Strings(paths)
	result := make([]NamespaceLockRequest, 0, len(paths))
	for _, value := range paths {
		result = append(result, NamespaceLockRequest{Path: value, Mode: modes[value]})
	}
	return result
}

func namespaceAdvisoryKey(value string) int64 {
	sum := sha256.Sum256([]byte("fsserver:namespace:" + value))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

var (
	errNamespaceConflict  = errors.New("namespace lock conflict")
	ErrNamespaceLeaseGone = errors.New("namespace lease holder is expired or missing")
)

func (d *DB) AcquireNamespaceLease(ctx context.Context, requests []NamespaceLockRequest, ttl time.Duration) (*NamespaceLease, error) {
	requests = normalizeNamespaceRequests(requests)
	if len(requests) == 0 || d == nil || d.dialect != DialectPostgreSQL {
		return &NamespaceLease{}, nil
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}

	for {
		lease, err := d.tryAcquireNamespaceLease(ctx, requests, ttl)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, errNamespaceConflict) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("acquire namespace lease: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// BeginNamespaceWrite uses a fenced transaction in PostgreSQL and a regular
// transaction for SQLite unit tests, where namespace leases are intentionally
// disabled.
func (d *DB) BeginNamespaceWrite(ctx context.Context, lease *NamespaceLease, paths ...string) (*Tx, error) {
	if d != nil && d.dialect == DialectPostgreSQL {
		return lease.BeginFencedWrite(ctx, paths...)
	}
	return d.BeginTx(ctx, nil)
}

// ResumeNamespaceLease attaches a new renewal loop to an existing holder token.
// Multiple instances may briefly renew the same token; releasing it from either
// instance removes the single database holder and causes the other loops to stop.
func (d *DB) ResumeNamespaceLease(ctx context.Context, token string, requests []NamespaceLockRequest, ttl time.Duration) (*NamespaceLease, error) {
	requests = normalizeNamespaceRequests(requests)
	if len(requests) == 0 || d == nil || d.dialect != DialectPostgreSQL {
		return &NamespaceLease{}, nil
	}
	if token == "" {
		return nil, errors.New("namespace lease token is empty")
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}

	result, err := d.ExecContext(ctx,
		"UPDATE namespace_lock_holders SET expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond') WHERE token = ? AND expires_at > CURRENT_TIMESTAMP",
		ttl.Milliseconds(), token,
	)
	if err != nil {
		return nil, fmt.Errorf("resume namespace lease: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("count resumed namespace holders: %w", err)
	}
	if rows != int64(len(requests)) {
		if _, err := d.ExecContext(ctx, "DELETE FROM namespace_lock_holders WHERE token = ?", token); err != nil {
			return nil, fmt.Errorf("remove incomplete namespace lease: %w", err)
		}
		return nil, fmt.Errorf("%w: renewed %d of %d paths", ErrNamespaceLeaseGone, rows, len(requests))
	}

	var fenceToken int64
	if err := d.QueryRow("SELECT MIN(fence_token) FROM namespace_lock_holders WHERE token = ?", token).Scan(&fenceToken); err != nil {
		return nil, fmt.Errorf("read resumed namespace fencing token: %w", err)
	}
	if fenceToken <= 0 {
		_, _ = d.ExecContext(ctx, "DELETE FROM namespace_lock_holders WHERE token = ?", token)
		return nil, fmt.Errorf("%w: invalid fencing token %d", ErrNamespaceLeaseGone, fenceToken)
	}
	renewCtx, cancel := context.WithCancel(context.Background())
	lease := &NamespaceLease{db: d, token: token, fenceToken: fenceToken, paths: int64(len(requests)), requests: append([]NamespaceLockRequest(nil), requests...), cancel: cancel}
	go lease.renew(renewCtx, ttl)
	return lease, nil
}

func (d *DB) tryAcquireNamespaceLease(ctx context.Context, requests []NamespaceLockRequest, ttl time.Duration) (*NamespaceLease, error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	token := utils.GenerateUUID()
	var fenceToken int64
	if err := tx.QueryRowContext(ctx, "SELECT nextval('namespace_fence_seq')").Scan(&fenceToken); err != nil {
		return nil, fmt.Errorf("allocate namespace fencing token: %w", err)
	}
	for _, request := range requests {
		decisionLockQuery := "SELECT pg_advisory_xact_lock(?)"
		if request.Mode == NamespaceShared {
			decisionLockQuery = "SELECT pg_advisory_xact_lock_shared(?)"
		}
		if _, err := tx.ExecContext(ctx, decisionLockQuery, namespaceAdvisoryKey(request.Path)); err != nil {
			return nil, fmt.Errorf("lock namespace decision for %s: %w", request.Path, err)
		}
	}

	// The advisory locks above serialize conflicting decisions in deterministic
	// path order. Perform holder cleanup, conflict detection, and all inserts in
	// one round trip; the insert is gated on the aggregate conflict count, so a
	// multi-path request can never leave a partially recorded lease.
	valueRows := make([]string, 0, len(requests))
	args := make([]interface{}, 0, len(requests)*2+3)
	for _, request := range requests {
		valueRows = append(valueRows, "(?, ?)")
		args = append(args, request.Path, string(request.Mode))
	}
	args = append(args, token, fenceToken, ttl.Milliseconds())
	query := `WITH requests(path, requested_mode) AS (VALUES ` + strings.Join(valueRows, ",") + `),
		expired AS (
			DELETE FROM namespace_lock_holders h USING requests r
			WHERE h.path = r.path AND h.expires_at <= CURRENT_TIMESTAMP
			RETURNING h.path
		),
		conflicts AS (
			SELECT COUNT(*) AS count
			FROM namespace_lock_holders h
			JOIN requests r ON r.path = h.path
			WHERE h.expires_at > CURRENT_TIMESTAMP
				AND (r.requested_mode = 'exclusive' OR h.mode = 'exclusive')
		),
		inserted AS (
			INSERT INTO namespace_lock_holders (path, token, mode, fence_token, expires_at)
			SELECT r.path, ?, r.requested_mode, ?,
				CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')
			FROM requests r
			WHERE (SELECT count FROM conflicts) = 0
			RETURNING path
		)
		SELECT (SELECT count FROM conflicts), (SELECT COUNT(*) FROM inserted)`
	var conflicts, inserted int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&conflicts, &inserted); err != nil {
		return nil, fmt.Errorf("record namespace lease holders: %w", err)
	}
	if conflicts > 0 {
		return nil, errNamespaceConflict
	}
	if inserted != len(requests) {
		return nil, fmt.Errorf("record namespace lease holders: inserted %d of %d paths", inserted, len(requests))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit namespace lease: %w", err)
	}
	rollback = false

	renewCtx, cancel := context.WithCancel(context.Background())
	lease := &NamespaceLease{db: d, token: token, fenceToken: fenceToken, paths: int64(len(requests)), requests: append([]NamespaceLockRequest(nil), requests...), cancel: cancel}
	go lease.renew(renewCtx, ttl)
	return lease, nil
}

func (l *NamespaceLease) renew(ctx context.Context, ttl time.Duration) {
	interval := ttl / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := l.db.ExecContext(ctx,
				"UPDATE namespace_lock_holders SET expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond') WHERE token = ? AND expires_at > CURRENT_TIMESTAMP",
				ttl.Milliseconds(), l.token,
			)
			if err != nil {
				if ctx.Err() == nil {
					l.markLost(fmt.Errorf("renew namespace lease: %w", err))
				}
				return
			}
			rows, err := result.RowsAffected()
			if err != nil || rows != l.paths {
				l.markLost(fmt.Errorf("namespace lease lost: renewed %d of %d paths", rows, l.paths))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
