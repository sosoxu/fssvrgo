// Package apikey owns API-key management: validation of the requested
// permissions, key generation and hashing, and the persistence round-trips.
//
// The HTTP layer maps requests to the small parameter structs below and maps
// the sentinel errors to status codes; it no longer builds SQL-backed services
// per request.
package apikey

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sosoxu/fssvrgo/internal/database"
	"github.com/sosoxu/fssvrgo/internal/utils"
)

var (
	// ErrNotFound is returned when the requested key id does not exist.
	ErrNotFound = errors.New("api key not found")
	// ErrInvalidPermissions is returned when the permissions string contains a
	// value outside the allowlist.
	ErrInvalidPermissions = errors.New("invalid permissions")
)

// Store is the data-access seam. database.ApiKeyService satisfies it; tests
// can supply an in-memory fake.
type Store interface {
	Create(ctx context.Context, key *database.ApiKey) error
	Update(ctx context.Context, key *database.ApiKey) error
	Remove(ctx context.Context, id string) error
	GetById(ctx context.Context, id string) (*database.ApiKey, error)
	List(ctx context.Context, activeOnly bool, page, pageSize int) ([]database.ApiKey, error)
}

// Generator produces the plaintext key for an id. auth.AuthService.GenerateApiKey
// is the production implementation; tests can pass a deterministic stand-in.
type Generator func(id string) string

type Service struct {
	store    Store
	generate Generator
	newID    func() string
	now      func() string
}

func NewService(store Store, generate Generator) *Service {
	return &Service{
		store:    store,
		generate: generate,
		newID:    utils.GenerateUUID,
		now:      utils.GetCurrentTimestamp,
	}
}

// CreateParams carries the user-supplied fields of a new key.
type CreateParams struct {
	Name        string
	Description string
	Permissions string
	ExpiresAt   string
}

// Created is the result of Create: the stored record plus the plaintext key,
// which is returned to the caller exactly once.
type Created struct {
	Key       *database.ApiKey
	Plaintext string
}

// UpdateParams carries a partial update. Nil fields are left unchanged.
type UpdateParams struct {
	Name        *string
	Description *string
	IsActive    *bool
}

// Create validates the requested permissions, generates a key, stores its
// hash, and returns the plaintext exactly once.
func (s *Service) Create(ctx context.Context, params CreateParams) (*Created, error) {
	if err := ValidatePermissions(params.Permissions); err != nil {
		return nil, err
	}
	if strings.TrimSpace(params.Name) == "" {
		return nil, fmt.Errorf("name is required")
	}

	id := s.newID()
	plaintext := s.generate(id)

	key := &database.ApiKey{
		ID:          id,
		KeyHash:     utils.SHA256(plaintext),
		Name:        params.Name,
		Description: params.Description,
		Permissions: params.Permissions,
		CreatedAt:   s.now(),
		ExpiresAt:   params.ExpiresAt,
		IsActive:    true,
	}
	if err := s.store.Create(ctx, key); err != nil {
		return nil, err
	}
	return &Created{Key: key, Plaintext: plaintext}, nil
}

// Get returns the key with the given id, or ErrNotFound.
func (s *Service) Get(ctx context.Context, id string) (*database.ApiKey, error) {
	key, err := s.store.GetById(ctx, id)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, ErrNotFound
	}
	return key, nil
}

// List returns one page of keys, newest first.
func (s *Service) List(ctx context.Context, activeOnly bool, page, pageSize int) ([]database.ApiKey, error) {
	return s.store.List(ctx, activeOnly, page, pageSize)
}

// Update applies the non-nil fields and persists the whole record.
func (s *Service) Update(ctx context.Context, id string, params UpdateParams) (*database.ApiKey, error) {
	key, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if params.Name != nil {
		key.Name = *params.Name
	}
	if params.Description != nil {
		key.Description = *params.Description
	}
	if params.IsActive != nil {
		key.IsActive = *params.IsActive
	}
	if err := s.store.Update(ctx, key); err != nil {
		return nil, err
	}
	return key, nil
}

// Delete removes the key and returns the record it removed, so the caller can
// audit the deleted key's name.
func (s *Service) Delete(ctx context.Context, id string) (*database.ApiKey, error) {
	key, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.store.Remove(ctx, id); err != nil {
		return nil, err
	}
	return key, nil
}

// ValidatePermissions accepts the predefined role values and the comma-separated
// fine-grained form. Rejecting anything else keeps values containing "admin" as
// a substring (e.g. "not-admin") from being mistaken for the admin role by the
// exact-match checks in internal/auth.
func ValidatePermissions(permissions string) error {
	if permissions == "" {
		return nil
	}
	allowed := map[string]bool{
		"admin": true, "user": true,
		"user:read": true, "user:write": true,
	}
	for _, p := range strings.Split(permissions, ",") {
		p = strings.TrimSpace(p)
		if p == "" || !allowed[p] {
			return fmt.Errorf("%w: allowed values are 'admin', 'user', or comma-separated 'user:read,user:write'", ErrInvalidPermissions)
		}
	}
	return nil
}
