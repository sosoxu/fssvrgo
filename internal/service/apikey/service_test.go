package apikey

import (
	"context"
	"errors"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

// fakeStore is an in-memory Store so the API-key service can be exercised
// without PostgreSQL.
type fakeStore struct {
	keys    map[string]*database.ApiKey
	created int
	updated int
	removed int
}

func newFakeStore() *fakeStore {
	return &fakeStore{keys: map[string]*database.ApiKey{}}
}

func (f *fakeStore) Create(_ context.Context, key *database.ApiKey) error {
	f.created++
	f.keys[key.ID] = key
	return nil
}

func (f *fakeStore) Update(_ context.Context, key *database.ApiKey) error {
	f.updated++
	f.keys[key.ID] = key
	return nil
}

func (f *fakeStore) Remove(_ context.Context, id string) error {
	f.removed++
	delete(f.keys, id)
	return nil
}

func (f *fakeStore) GetById(_ context.Context, id string) (*database.ApiKey, error) {
	return f.keys[id], nil
}

func (f *fakeStore) List(_ context.Context, activeOnly bool, page, pageSize int) ([]database.ApiKey, error) {
	var out []database.ApiKey
	for _, k := range f.keys {
		if activeOnly && !k.IsActive {
			continue
		}
		out = append(out, *k)
	}
	return out, nil
}

func newTestService() (*Service, *fakeStore) {
	store := newFakeStore()
	return NewService(store, func(id string) string { return "plain-" + id }), store
}

func TestCreateStoresHashAndReturnsPlaintextOnce(t *testing.T) {
	svc, store := newTestService()

	created, err := svc.Create(context.Background(), CreateParams{
		Name:        "ci",
		Permissions: "user:read,user:write",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if store.created != 1 {
		t.Fatalf("store.Create called %d times, want 1", store.created)
	}
	if created.Plaintext != "plain-"+created.Key.ID {
		t.Errorf("plaintext = %q, want generated value for id %q", created.Plaintext, created.Key.ID)
	}
	if created.Key.KeyHash != utils.SHA256(created.Plaintext) {
		t.Error("stored key hash does not match SHA256(plaintext)")
	}
	if !created.Key.IsActive {
		t.Error("new key must be active")
	}
	if created.Key.CreatedAt == "" {
		t.Error("created_at must be stamped")
	}
}

func TestCreateRejectsInvalidPermissions(t *testing.T) {
	svc, store := newTestService()

	_, err := svc.Create(context.Background(), CreateParams{Name: "bad", Permissions: "not-admin"})
	if !errors.Is(err, ErrInvalidPermissions) {
		t.Fatalf("err = %v, want ErrInvalidPermissions", err)
	}
	if store.created != 0 {
		t.Errorf("store.Create called %d times, want 0", store.created)
	}
}

func TestCreateRequiresName(t *testing.T) {
	svc, _ := newTestService()
	if _, err := svc.Create(context.Background(), CreateParams{Name: "  "}); err == nil {
		t.Fatal("Create accepted a blank name")
	}
}

func TestGetUpdateDeleteRoundTrip(t *testing.T) {
	svc, store := newTestService()
	ctx := context.Background()

	created, err := svc.Create(ctx, CreateParams{Name: "first", Description: "before"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	id := created.Key.ID

	name := "renamed"
	active := false
	updated, err := svc.Update(ctx, id, UpdateParams{Name: &name, IsActive: &active})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Name != "renamed" || updated.IsActive {
		t.Errorf("updated = %+v, want name=renamed is_active=false", updated)
	}
	if updated.Description != "before" {
		t.Errorf("description = %q, want unchanged %q", updated.Description, "before")
	}
	if store.updated != 1 {
		t.Errorf("store.Update called %d times, want 1", store.updated)
	}

	deleted, err := svc.Delete(ctx, id)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.ID != id || store.removed != 1 {
		t.Errorf("deleted id = %q removed=%d, want %q/1", deleted.ID, store.removed, id)
	}

	if _, err := svc.Get(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
	}
	if _, err := svc.Update(ctx, id, UpdateParams{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("Update on missing key: err = %v, want ErrNotFound", err)
	}
	if _, err := svc.Delete(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete on missing key: err = %v, want ErrNotFound", err)
	}
}

func TestValidatePermissions(t *testing.T) {
	valid := []string{"", "admin", "user", "user:read", "user:read,user:write", " user:read , user:write "}
	for _, p := range valid {
		if err := ValidatePermissions(p); err != nil {
			t.Errorf("ValidatePermissions(%q) = %v, want nil", p, err)
		}
	}

	invalid := []string{"superuser", "not-admin", "user:delete", "user:read,", "admin,"}
	for _, p := range invalid {
		if err := ValidatePermissions(p); !errors.Is(err, ErrInvalidPermissions) {
			t.Errorf("ValidatePermissions(%q) = %v, want ErrInvalidPermissions", p, err)
		}
	}
}
