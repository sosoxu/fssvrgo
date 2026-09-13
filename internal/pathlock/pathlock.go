// Package pathlock owns the process-local, path-keyed lock table that the
// storage backends and the services above them share.
//
// # Why one table
//
// Before this package there were two independent keyed mutexes —
// FileManager.fileLocks and LocalStorage.pathLocks / MinIOStorage.pathLocks —
// plus an ad-hoc "lock old before new if old < new" ordering inside storage
// Rename. The same path could therefore be guarded by two different mutexes
// depending on which layer asked, multi-path acquisitions had no single place
// that defined their order, and the storage table had no reclamation path at
// all (its CleanPathLocks was never called outside tests, so entries only ever
// grew). Now every layer acquires from one table through one API: ordering is
// enforced in one place and so is reclamation.
//
// # Levels and ordering
//
// A key is a (level, path) pair, and a caller must always acquire the lower
// level before a higher one:
//
//	LevelDirectory — a directory-scope operation (create/rename/delete a
//	                 directory, including its recursive forms)
//	LevelFile      — one file's service-level section (the check-then-act that
//	                 decides create vs. overwrite)
//	LevelObject    — one storage-backend call on one object
//
// LockMany sorts its keys by (level, path) and drops duplicates, so any
// multi-path operation acquires in the same global order and cannot form a
// cycle. The levels also make nesting safe with non-reentrant mutexes: a
// service holds LevelFile for a path and then calls into storage, which takes
// LevelObject for the same path — a different key, so the same mutex is never
// acquired twice by one goroutine, and the level order is respected.
//
// # Reclamation
//
// Entries are reference counted. An entry with no holder and no waiter is idle
// and can be dropped at any time without letting a lock "escape": Reclaim drops
// only refs == 0 entries while holding the table lock, so a goroutine either
// observes the entry it incremented or creates a fresh one after the sweep.
// The process wires Reclaim into a periodic janitor so the table stays bounded
// by live concurrency rather than by every path ever touched.
//
// # Cross-instance locks
//
// The Redis-backed distributed locks use the same levels as key prefixes —
// "dir:<path>" for directory operations and "file:<path>" for file operations
// — so the two schemes agree on one order: directory before file, and within a
// level, path order. No current operation holds both (a directory operation
// takes only dir:, a file operation only file:), but defining the order here is
// what keeps a future "move file into directory" operation deadlock-free.
package pathlock

import (
	"sort"
	"strings"
	"sync"
)

// Level orders lock keys. See the package documentation for the contract.
type Level int

const (
	// LevelDirectory covers directory-scope operations.
	LevelDirectory Level = iota
	// LevelFile covers a single file's service-level operation.
	LevelFile
	// LevelObject covers one storage-backend call on one object.
	LevelObject
)

func (l Level) String() string {
	switch l {
	case LevelDirectory:
		return "dir"
	case LevelFile:
		return "file"
	case LevelObject:
		return "object"
	default:
		return "unknown"
	}
}

// Key identifies one lock.
type Key struct {
	Level Level
	Path  string
}

// K builds a key, normalizing the path so that "a/b", "/a/b" and "/a/b/" all
// name the same lock.
func K(level Level, path string) Key {
	return Key{Level: level, Path: CanonicalPath(path)}
}

// CanonicalPath normalizes a path for lock purposes: whitespace trimmed, a
// single leading slash, no trailing slash, and empty meaning the root.
func CanonicalPath(path string) string {
	trimmed := strings.Trim(strings.TrimSpace(path), "/")
	if trimmed == "" {
		return "/"
	}
	return "/" + trimmed
}

func (k Key) token() string {
	return k.Level.String() + ":" + k.Path
}

// entry is one keyed mutex plus the number of goroutines that currently hold or
// wait for it. refs is guarded by Locker.mu, never by entry.mu, so Reclaim can
// read it without taking the mutex it is deciding to drop.
type entry struct {
	mu   sync.Mutex
	refs int
}

// Locker is the shared table. The zero value is a usable empty table, so a
// backend built with a struct literal (as several tests do) still gets locking;
// New is the idiomatic constructor, and the storage adapters each own one.
type Locker struct {
	mu      sync.Mutex
	entries map[string]*entry
}

func New() *Locker {
	return &Locker{entries: make(map[string]*entry)}
}

// Lock acquires one key and returns the function that releases it:
//
//	release := locks.Lock(pathlock.LevelFile, path)
//	defer release()
func (l *Locker) Lock(level Level, path string) func() {
	return l.LockMany(K(level, path))
}

// LockMany acquires every given key in (level, path) order and returns the
// function that releases them in reverse order. Duplicate keys are collapsed,
// so passing the same path twice acquires it once. A nil/empty key set returns
// a no-op release function.
func (l *Locker) LockMany(keys ...Key) func() {
	ordered := sortedKeys(keys)
	held := make([]heldKey, 0, len(ordered))
	for _, key := range ordered {
		token := key.token()

		l.mu.Lock()
		if l.entries == nil {
			l.entries = make(map[string]*entry)
		}
		e := l.entries[token]
		if e == nil {
			e = &entry{}
			l.entries[token] = e
		}
		e.refs++
		l.mu.Unlock()

		e.mu.Lock()
		held = append(held, heldKey{token: token, e: e})
	}

	return func() {
		// Release in reverse acquisition order so the table is never left in a
		// state where a waiter sees a lower-level key free while a higher-level
		// one is still held by this goroutine.
		for i := len(held) - 1; i >= 0; i-- {
			held[i].e.mu.Unlock()
			l.mu.Lock()
			held[i].e.refs--
			l.mu.Unlock()
		}
	}
}

type heldKey struct {
	token string
	e     *entry
}

// Reclaim drops every idle entry and reports how many were dropped. It is safe
// to call concurrently with Lock/LockMany: an entry is only dropped while
// refs == 0, i.e. when no goroutine holds it or is waiting for it.
func (l *Locker) Reclaim() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	dropped := 0
	for token, e := range l.entries {
		if e.refs == 0 {
			delete(l.entries, token)
			dropped++
		}
	}
	return dropped
}

// Len reports how many entries the table currently holds.
func (l *Locker) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// sortedKeys normalizes, dedupes and sorts a key set into the global
// acquisition order.
func sortedKeys(keys []Key) []Key {
	normalized := make([]Key, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		k.Path = CanonicalPath(k.Path)
		token := k.token()
		if seen[token] {
			continue
		}
		seen[token] = true
		normalized = append(normalized, k)
	}

	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Level != normalized[j].Level {
			return normalized[i].Level < normalized[j].Level
		}
		return normalized[i].Path < normalized[j].Path
	})
	return normalized
}
