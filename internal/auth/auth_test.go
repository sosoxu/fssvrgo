package auth

import (
	"sync"
	"testing"
)

func TestApiKeyHashing(t *testing.T) {
	hash1 := hashApiKey("test-key")
	hash2 := hashApiKey("test-key")
	if hash1 != hash2 {
		t.Errorf("same key should produce same hash")
	}

	hash3 := hashApiKey("different-key")
	if hash1 == hash3 {
		t.Errorf("different keys should produce different hashes")
	}

	emptyHash := hashApiKey("")
	if emptyHash == "" {
		t.Errorf("empty key should still produce a hash")
	}

	if len(hash1) != 64 {
		t.Errorf("hash should be 64 characters, got %d", len(hash1))
	}
}

func TestAuthDisabled(t *testing.T) {
	svc := NewAuthService()
	svc.Init(false, "")
	if !svc.ValidateApiKey("any-key") {
		t.Errorf("ValidateApiKey should return true when auth is disabled")
	}
	if !svc.ValidateApiKey("") {
		t.Errorf("ValidateApiKey should return true for empty key when auth is disabled")
	}
}

func TestAuthEnabled(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "my-secret-key")

	if !svc.ValidateApiKey("my-secret-key") {
		t.Errorf("ValidateApiKey should return true for correct key")
	}

	if svc.ValidateApiKey("wrong-key") {
		t.Errorf("ValidateApiKey should return false for wrong key")
	}

	if svc.ValidateApiKey("") {
		t.Errorf("ValidateApiKey should return false for empty key when auth is enabled")
	}
}

func TestRateLimiting(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "test-key")

	if svc.IsRateLimited("192.168.1.1") {
		t.Errorf("should not be rate limited initially")
	}

	for i := 0; i < 10; i++ {
		svc.RecordAuthFailure("192.168.1.1")
	}

	if !svc.IsRateLimited("192.168.1.1") {
		t.Errorf("should be rate limited after 10 failures")
	}

	svc.ClearAuthFailure("192.168.1.1")

	if svc.IsRateLimited("192.168.1.1") {
		t.Errorf("should not be rate limited after clearing")
	}

	for i := 0; i < 10; i++ {
		svc.RecordAuthFailure("192.168.1.1")
	}

	if svc.IsRateLimited("192.168.1.2") {
		t.Errorf("different IP should not be rate limited")
	}
}

func TestUserCRUD(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	user := &User{
		ID:         "user-1",
		ApiKeyHash: hashApiKey("user-key-1"),
		Name:       "Test User",
		Role:       "user",
		Enabled:    true,
	}

	if err := svc.CreateUser(user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	got, err := svc.GetUserById("user-1")
	if err != nil {
		t.Fatalf("GetUserById failed: %v", err)
	}
	if got.Name != "Test User" {
		t.Errorf("expected name 'Test User', got '%s'", got.Name)
	}
	if got.Role != "user" {
		t.Errorf("expected role 'user', got '%s'", got.Role)
	}

	user.Name = "Updated User"
	if err := svc.UpdateUser(user); err != nil {
		t.Fatalf("UpdateUser failed: %v", err)
	}

	got, _ = svc.GetUserById("user-1")
	if got.Name != "Updated User" {
		t.Errorf("expected name 'Updated User', got '%s'", got.Name)
	}

	if err := svc.DeleteUser("user-1"); err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	if _, err := svc.GetUserById("user-1"); err == nil {
		t.Errorf("expected error for deleted user")
	}

	dupUser := &User{ID: "dup", ApiKeyHash: "hash", Name: "Dup", Role: "user", Enabled: true}
	svc.CreateUser(dupUser)
	if err := svc.CreateUser(dupUser); err == nil {
		t.Errorf("expected error for duplicate user")
	}

	if err := svc.UpdateUser(&User{ID: "nonexistent", Name: "X"}); err == nil {
		t.Errorf("expected error for updating non-existent user")
	}
}

func TestHasPermission(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	if !svc.HasPermission("admin-key", "files", "read") {
		t.Errorf("admin should have read permission")
	}
	if !svc.HasPermission("admin-key", "files", "write") {
		t.Errorf("admin should have write permission")
	}
	if !svc.HasPermission("admin-key", "files", "delete") {
		t.Errorf("admin should have delete permission")
	}
	if !svc.HasPermission("admin-key", "users", "manage") {
		t.Errorf("admin should have all permissions")
	}

	userKey := "user-key-123"
	user := &User{
		ID:         "user-perm",
		ApiKeyHash: hashApiKey(userKey),
		Name:       "Regular User",
		Role:       "user",
		Enabled:    true,
	}
	svc.CreateUser(user)

	if !svc.HasPermission(userKey, "files", "read") {
		t.Errorf("user should have read permission on files")
	}
	if !svc.HasPermission(userKey, "files", "write") {
		t.Errorf("user should have write permission on files")
	}
	if svc.HasPermission(userKey, "files", "delete") {
		t.Errorf("user should not have delete permission on files")
	}

	if svc.HasPermission("unknown-key", "files", "read") {
		t.Errorf("unknown key should have no permission")
	}
}

func TestConcurrentAuth(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "test-key")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			svc.RecordAuthFailure("10.0.0.1")
		}()
		go func() {
			defer wg.Done()
			svc.IsRateLimited("10.0.0.1")
		}()
	}
	wg.Wait()
}
