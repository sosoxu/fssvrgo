package distributed

import (
	"context"
	"testing"
	"time"
)

func TestMemorySessionStore_SetGet(t *testing.T) {
	s := NewMemorySessionStore()
	ctx := context.Background()

	if err := s.Set(ctx, "user", "u1", "alice", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var v string
	if err := s.Get(ctx, "user", "u1", &v); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v != "alice" {
		t.Fatalf("expected alice, got %s", v)
	}
}

func TestMemorySessionStore_Delete(t *testing.T) {
	s := NewMemorySessionStore()
	ctx := context.Background()

	if err := s.Set(ctx, "user", "u1", "alice", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := s.Delete(ctx, "user", "u1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	var v string
	if err := s.Get(ctx, "user", "u1", &v); err == nil {
		t.Fatalf("expected error after delete, got value %q", v)
	}
}

func TestMemorySessionStore_Exists(t *testing.T) {
	s := NewMemorySessionStore()
	ctx := context.Background()

	ok, err := s.Exists(ctx, "user", "u1")
	if err != nil {
		t.Fatalf("Exists before set: %v", err)
	}
	if ok {
		t.Fatal("expected not exist before set")
	}

	if err := s.Set(ctx, "user", "u1", "alice", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ok, err = s.Exists(ctx, "user", "u1")
	if err != nil {
		t.Fatalf("Exists after set: %v", err)
	}
	if !ok {
		t.Fatal("expected exist after set")
	}

	if err := s.Delete(ctx, "user", "u1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, err = s.Exists(ctx, "user", "u1")
	if err != nil {
		t.Fatalf("Exists after delete: %v", err)
	}
	if ok {
		t.Fatal("expected not exist after delete")
	}
}

func TestMemorySessionStore_TTL(t *testing.T) {
	s := NewMemorySessionStore()
	ctx := context.Background()

	if err := s.Set(ctx, "user", "u1", "alice", 30*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	var v string
	if err := s.Get(ctx, "user", "u1", &v); err == nil {
		t.Fatalf("expected error after TTL expiry, got value %q", v)
	}

	ok, err := s.Exists(ctx, "user", "u1")
	if err != nil {
		t.Fatalf("Exists after TTL: %v", err)
	}
	if ok {
		t.Fatal("expected not exist after TTL expiry")
	}
}

func TestMemorySessionStore_GetNotFound(t *testing.T) {
	s := NewMemorySessionStore()
	ctx := context.Background()

	var v string
	if err := s.Get(ctx, "user", "missing", &v); err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

func TestMemorySessionStore_StructSerialization(t *testing.T) {
	s := NewMemorySessionStore()
	ctx := context.Background()

	type session struct {
		UserID string
		Name   string
		Admin  bool
		Roles  []string
	}

	orig := session{
		UserID: "123",
		Name:   "bob",
		Admin:  true,
		Roles:  []string{"read", "write"},
	}
	if err := s.Set(ctx, "sess", "s1", orig, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var got session
	if err := s.Get(ctx, "sess", "s1", &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.UserID != orig.UserID || got.Name != orig.Name || got.Admin != orig.Admin || len(got.Roles) != 2 {
		t.Fatalf("struct mismatch: got %+v", got)
	}
	if got.Roles[0] != "read" || got.Roles[1] != "write" {
		t.Fatalf("roles mismatch: got %v", got.Roles)
	}
}
