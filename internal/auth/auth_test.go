package auth

import (
	"context"
	"sync"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/database"
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

func TestValidateApiKeyWithDatabaseLookup(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	const dbKey = "db-managed-key"
	dbHash := hashApiKey(dbKey)

	mock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == dbHash {
			return &database.ApiKey{
				ID:          "db-key-1",
				KeyHash:     keyHash,
				Name:        "DB Key",
				Permissions: "user",
				IsActive:    true,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(mock)

	if !svc.ValidateApiKey(dbKey) {
		t.Errorf("ValidateApiKey should return true for active database-managed key")
	}

	if svc.ValidateApiKey("nonexistent-db-key") {
		t.Errorf("ValidateApiKey should return false for key not found in database")
	}

	inactiveKey := "inactive-db-key"
	inactiveHash := hashApiKey(inactiveKey)
	inactiveMock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == inactiveHash {
			return &database.ApiKey{
				ID:       "db-key-2",
				KeyHash:  keyHash,
				Name:     "Inactive Key",
				IsActive: false,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(inactiveMock)
	if svc.ValidateApiKey(inactiveKey) {
		t.Errorf("ValidateApiKey should return false for inactive database key")
	}
}

func TestGetUserByApiKeyWithDatabaseLookup(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	const dbKey = "db-user-key"
	dbHash := hashApiKey(dbKey)

	mock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == dbHash {
			return &database.ApiKey{
				ID:          "db-user-1",
				KeyHash:     keyHash,
				Name:        "DB User",
				Permissions: "user",
				IsActive:    true,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(mock)

	user := svc.GetUserByApiKey(dbKey)
	if user == nil {
		t.Fatalf("GetUserByApiKey should return a user for active database key")
	}
	if user.ID != "db-user-1" {
		t.Errorf("expected user ID 'db-user-1', got '%s'", user.ID)
	}
	if user.Name != "DB User" {
		t.Errorf("expected user Name 'DB User', got '%s'", user.Name)
	}
	if user.Role != "user" {
		t.Errorf("expected user Role 'user', got '%s'", user.Role)
	}
	if !user.Enabled {
		t.Errorf("expected database-backed user to be enabled")
	}

	if svc.GetUserByApiKey("unknown-db-key") != nil {
		t.Errorf("GetUserByApiKey should return nil for unknown key")
	}

	adminMock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == dbHash {
			return &database.ApiKey{
				ID:          "db-admin-1",
				KeyHash:     keyHash,
				Name:        "DB Admin",
				Permissions: "admin",
				IsActive:    true,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(adminMock)
	adminUser := svc.GetUserByApiKey(dbKey)
	if adminUser == nil {
		t.Fatalf("GetUserByApiKey should return admin user from database")
	}
	if adminUser.Role != "admin" {
		t.Errorf("expected Role 'admin' for admin-permission key, got '%s'", adminUser.Role)
	}
}

func TestGetUserByApiKeyWithJWT(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	jwtSvc := svc.GetJWTService()
	if jwtSvc == nil {
		t.Fatalf("JWTService should not be nil after Init with auth enabled")
	}

	pair, err := jwtSvc.GenerateTokenPair("jwt-user-1", "user")
	if err != nil {
		t.Fatalf("GenerateTokenPair failed: %v", err)
	}

	user := svc.GetUserByApiKey(pair.AccessToken)
	if user == nil {
		t.Fatalf("GetUserByApiKey should return a user for valid access token")
	}
	if user.ID != "jwt-user-1" {
		t.Errorf("expected user ID 'jwt-user-1', got '%s'", user.ID)
	}
	if user.Role != "user" {
		t.Errorf("expected user Role 'user', got '%s'", user.Role)
	}
	if !user.Enabled {
		t.Errorf("expected JWT-backed user to be enabled")
	}

	// Refresh tokens should not be accepted as API auth identity.
	if svc.GetUserByApiKey(pair.RefreshToken) != nil {
		t.Errorf("GetUserByApiKey should return nil for refresh token")
	}
}

func TestHasPermissionWithDatabaseUser(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	const dbKey = "db-perm-key"
	dbHash := hashApiKey(dbKey)

	mock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == dbHash {
			return &database.ApiKey{
				ID:          "db-user-1",
				KeyHash:     keyHash,
				Name:        "DB User",
				Permissions: "user",
				IsActive:    true,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(mock)

	if !svc.HasPermission(dbKey, "files", "read") {
		t.Errorf("database user should have read permission on files")
	}
	if !svc.HasPermission(dbKey, "files", "write") {
		t.Errorf("database user should have write permission on files")
	}
	if svc.HasPermission(dbKey, "files", "delete") {
		t.Errorf("database user should not have delete permission on files")
	}
	if svc.HasPermission(dbKey, "users", "manage") {
		t.Errorf("database user should not have manage permission on users")
	}

	// Database admin key should be granted all permissions.
	adminKey := "db-admin-perm-key"
	adminHash := hashApiKey(adminKey)
	adminMock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == adminHash {
			return &database.ApiKey{
				ID:          "db-admin-1",
				KeyHash:     keyHash,
				Name:        "DB Admin",
				Permissions: "admin",
				IsActive:    true,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(adminMock)
	if !svc.HasPermission(adminKey, "files", "delete") {
		t.Errorf("database admin should have delete permission on files")
	}
	if !svc.HasPermission(adminKey, "users", "manage") {
		t.Errorf("database admin should have manage permission on users")
	}
}

func TestJWTTokenType(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	jwtSvc := svc.GetJWTService()
	if jwtSvc == nil {
		t.Fatalf("JWTService should not be nil after Init with auth enabled")
	}

	pair, err := jwtSvc.GenerateTokenPair("user-1", "user")
	if err != nil {
		t.Fatalf("GenerateTokenPair failed: %v", err)
	}

	if pair.AccessToken == "" {
		t.Errorf("access token should not be empty")
	}
	if pair.RefreshToken == "" {
		t.Errorf("refresh token should not be empty")
	}
	if pair.AccessToken == pair.RefreshToken {
		t.Errorf("access and refresh tokens should differ")
	}

	accessClaims, err := jwtSvc.ValidateToken(pair.AccessToken)
	if err != nil {
		t.Fatalf("ValidateToken for access token failed: %v", err)
	}
	if accessClaims.TokenType != "access" {
		t.Errorf("expected access token TokenType 'access', got '%s'", accessClaims.TokenType)
	}
	if accessClaims.UserID != "user-1" {
		t.Errorf("expected access token UserID 'user-1', got '%s'", accessClaims.UserID)
	}
	if accessClaims.Role != "user" {
		t.Errorf("expected access token Role 'user', got '%s'", accessClaims.Role)
	}

	refreshClaims, err := jwtSvc.ValidateToken(pair.RefreshToken)
	if err != nil {
		t.Fatalf("ValidateToken for refresh token failed: %v", err)
	}
	if refreshClaims.TokenType != "refresh" {
		t.Errorf("expected refresh token TokenType 'refresh', got '%s'", refreshClaims.TokenType)
	}
	if refreshClaims.UserID != "user-1" {
		t.Errorf("expected refresh token UserID 'user-1', got '%s'", refreshClaims.UserID)
	}
	if refreshClaims.Role != "user" {
		t.Errorf("expected refresh token Role 'user', got '%s'", refreshClaims.Role)
	}
}

func TestRefreshTokenRejectsAccessToken(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	jwtSvc := svc.GetJWTService()
	if jwtSvc == nil {
		t.Fatalf("JWTService should not be nil after Init with auth enabled")
	}

	pair, err := jwtSvc.GenerateTokenPair("user-1", "user")
	if err != nil {
		t.Fatalf("GenerateTokenPair failed: %v", err)
	}

	// Using an access token as a refresh token must be rejected.
	if _, err := jwtSvc.RefreshToken(pair.AccessToken); err == nil {
		t.Errorf("RefreshToken should reject access token")
	}

	// A valid refresh token should produce a new token pair.
	newPair, err := jwtSvc.RefreshToken(pair.RefreshToken)
	if err != nil {
		t.Fatalf("RefreshToken should accept valid refresh token, got error: %v", err)
	}
	if newPair.AccessToken == "" || newPair.RefreshToken == "" {
		t.Errorf("RefreshToken should return non-empty tokens")
	}

	// The newly issued access token should still validate and carry the
	// access TokenType.
	newAccessClaims, err := jwtSvc.ValidateToken(newPair.AccessToken)
	if err != nil {
		t.Fatalf("new access token should validate: %v", err)
	}
	if newAccessClaims.TokenType != "access" {
		t.Errorf("new access token TokenType should be 'access', got '%s'", newAccessClaims.TokenType)
	}

	// The new access token must still be rejected as a refresh token.
	if _, err := jwtSvc.RefreshToken(newPair.AccessToken); err == nil {
		t.Errorf("RefreshToken should reject newly issued access token")
	}
}

func TestValidateApiKeyRejectsEmptyKey(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	if svc.ValidateApiKey("") {
		t.Errorf("ValidateApiKey should return false for empty key when auth enabled")
	}

	// Setting a lookup function should not allow empty keys to pass.
	called := false
	mock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		called = true
		return nil, nil
	}
	svc.SetApiKeyLookup(mock)
	if svc.ValidateApiKey("") {
		t.Errorf("ValidateApiKey should return false for empty key even with lookup set")
	}
	if called {
		t.Errorf("lookup function should not be called for empty key")
	}

	// GetUserByApiKey should also reject empty keys.
	if svc.GetUserByApiKey("") != nil {
		t.Errorf("GetUserByApiKey should return nil for empty key")
	}
}

func TestSetApiKeyLookup(t *testing.T) {
	svc := NewAuthService()
	svc.Init(true, "admin-key")

	// Without a lookup function, unknown keys still fail (fall through to JWT).
	if svc.ValidateApiKey("totally-unknown-key") {
		t.Errorf("ValidateApiKey should return false for unknown key without lookup")
	}

	called := 0
	var seenHash string
	mock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		called++
		seenHash = keyHash
		return nil, nil
	}
	svc.SetApiKeyLookup(mock)

	// With the lookup set, an unknown key triggers a database lookup.
	if svc.ValidateApiKey("still-unknown-key") {
		t.Errorf("ValidateApiKey should return false when lookup returns nil")
	}
	if called == 0 {
		t.Errorf("lookup function should have been called for unknown key")
	}
	if seenHash != hashApiKey("still-unknown-key") {
		t.Errorf("lookup function should receive hashed key, got '%s'", seenHash)
	}

	// Replacing the lookup function with one that returns an active key.
	const okKey = "ok-key"
	okHash := hashApiKey(okKey)
	okMock := func(ctx context.Context, keyHash string) (*database.ApiKey, error) {
		if keyHash == okHash {
			return &database.ApiKey{
				ID:       "ok-1",
				KeyHash:  keyHash,
				Name:     "OK Key",
				IsActive: true,
			}, nil
		}
		return nil, nil
	}
	svc.SetApiKeyLookup(okMock)
	if !svc.ValidateApiKey(okKey) {
		t.Errorf("ValidateApiKey should return true after replacing lookup with one that returns active key")
	}

	// Default admin key must still validate even when a lookup is registered.
	if !svc.ValidateApiKey("admin-key") {
		t.Errorf("ValidateApiKey should still accept the default admin key with lookup set")
	}
}
