package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWTService struct {
	secret           []byte
	tokenExpiry      time.Duration
	refreshExpiry    time.Duration
	refreshBlacklist sync.Map // jti -> expiry time.Time; prevents refresh token replay
}

type Claims struct {
	UserID    string `json:"user_id"`
	Role      string `json:"role"`
	TokenType string `json:"token_type,omitempty"`
	jwt.RegisteredClaims
}

type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

func NewJWTService(secret string, tokenExpiry, refreshExpiry time.Duration) *JWTService {
	return &JWTService{
		secret:        []byte(secret),
		tokenExpiry:   tokenExpiry,
		refreshExpiry: refreshExpiry,
	}
}

func (s *JWTService) GenerateTokenPair(userID, role string) (*TokenPair, error) {
	now := time.Now()
	accessExpiry := now.Add(s.tokenExpiry)

	accessJTI, err := generateJTI()
	if err != nil {
		return nil, fmt.Errorf("failed to generate access token ID: %w", err)
	}

	accessClaims := Claims{
		UserID:    userID,
		Role:      role,
		TokenType: "access",
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        accessJTI,
			ExpiresAt: jwt.NewNumericDate(accessExpiry),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    "fsserver",
			Subject:   userID,
		},
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessStr, err := accessToken.SignedString(s.secret)
	if err != nil {
		return nil, err
	}

	refreshExpiry := now.Add(s.refreshExpiry)
	refreshJTI, err := generateJTI()
	if err != nil {
		return nil, fmt.Errorf("failed to generate refresh token ID: %w", err)
	}

	refreshClaims := Claims{
		UserID:    userID,
		Role:      role,
		TokenType: "refresh",
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        refreshJTI,
			ExpiresAt: jwt.NewNumericDate(refreshExpiry),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    "fsserver",
			Subject:   userID,
		},
	}

	refreshToken := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshStr, err := refreshToken.SignedString(s.secret)
	if err != nil {
		return nil, err
	}

	return &TokenPair{
		AccessToken:  accessStr,
		RefreshToken: refreshStr,
		ExpiresIn:    int64(s.tokenExpiry.Seconds()),
	}, nil
}

func (s *JWTService) ValidateToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return s.secret, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}

	// Reject blacklisted refresh tokens (one-time use enforcement).
	if claims.TokenType == "refresh" && claims.ID != "" {
		if _, blacklisted := s.refreshBlacklist.Load(claims.ID); blacklisted {
			return nil, errors.New("refresh token has been revoked")
		}
	}

	return claims, nil
}

func (s *JWTService) RefreshToken(refreshTokenStr string) (*TokenPair, error) {
	claims, err := s.ValidateToken(refreshTokenStr)
	if err != nil {
		return nil, err
	}

	// Only refresh tokens may be used to obtain a new token pair.
	if claims.TokenType != "" && claims.TokenType != "refresh" {
		return nil, errors.New("not a refresh token")
	}

	// Invalidate the old refresh token to enforce one-time use.
	if claims.ID != "" {
		s.refreshBlacklist.Store(claims.ID, time.Now().Add(s.refreshExpiry))
		s.cleanupBlacklist()
	}

	return s.GenerateTokenPair(claims.UserID, claims.Role)
}

// cleanupBlacklist removes expired entries from the refresh token blacklist.
// It is called opportunistically on each refresh to bound memory usage.
func (s *JWTService) cleanupBlacklist() {
	now := time.Now()
	s.refreshBlacklist.Range(func(key, value interface{}) bool {
		if expiry, ok := value.(time.Time); ok && now.After(expiry) {
			s.refreshBlacklist.Delete(key)
		}
		return true
	})
}

func generateJTI() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
