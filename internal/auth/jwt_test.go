package auth

import (
	"testing"
	"time"
)

func TestJWTService_GenerateAndValidateToken(t *testing.T) {
	svc := NewJWTService("test-secret-key", time.Hour, 24*time.Hour)

	tokenPair, err := svc.GenerateTokenPair("user123", "admin")
	if err != nil {
		t.Fatalf("GenerateTokenPair failed: %v", err)
	}

	if tokenPair.AccessToken == "" {
		t.Error("AccessToken should not be empty")
	}
	if tokenPair.RefreshToken == "" {
		t.Error("RefreshToken should not be empty")
	}
	if tokenPair.ExpiresIn != 3600 {
		t.Errorf("Expected ExpiresIn=3600, got %d", tokenPair.ExpiresIn)
	}

	claims, err := svc.ValidateToken(tokenPair.AccessToken)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}
	if claims.UserID != "user123" {
		t.Errorf("Expected UserID=user123, got %s", claims.UserID)
	}
	if claims.Role != "admin" {
		t.Errorf("Expected Role=admin, got %s", claims.Role)
	}
}

func TestJWTService_InvalidToken(t *testing.T) {
	svc := NewJWTService("test-secret-key", time.Hour, 24*time.Hour)

	_, err := svc.ValidateToken("invalid-token")
	if err == nil {
		t.Error("Expected error for invalid token")
	}
}

func TestJWTService_ExpiredToken(t *testing.T) {
	svc := NewJWTService("test-secret-key", -time.Hour, -time.Hour)

	tokenPair, err := svc.GenerateTokenPair("user123", "user")
	if err != nil {
		t.Fatalf("GenerateTokenPair failed: %v", err)
	}

	_, err = svc.ValidateToken(tokenPair.AccessToken)
	if err == nil {
		t.Error("Expected error for expired token")
	}
}

func TestJWTService_RefreshToken(t *testing.T) {
	svc := NewJWTService("test-secret-key", time.Hour, 24*time.Hour)

	tokenPair, err := svc.GenerateTokenPair("user123", "user")
	if err != nil {
		t.Fatalf("GenerateTokenPair failed: %v", err)
	}

	newTokenPair, err := svc.RefreshToken(tokenPair.RefreshToken)
	if err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	}

	if newTokenPair.AccessToken == "" {
		t.Error("New AccessToken should not be empty")
	}
}

func TestJWTService_WrongSecret(t *testing.T) {
	svc1 := NewJWTService("secret1", time.Hour, 24*time.Hour)
	svc2 := NewJWTService("secret2", time.Hour, 24*time.Hour)

	tokenPair, _ := svc1.GenerateTokenPair("user123", "user")

	_, err := svc2.ValidateToken(tokenPair.AccessToken)
	if err == nil {
		t.Error("Expected error when validating with wrong secret")
	}
}
