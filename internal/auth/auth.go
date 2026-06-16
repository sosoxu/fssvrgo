package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sosoxu/fssvrgo/internal/utils"
)

type User struct {
	ID         string
	ApiKeyHash string
	Name       string
	Role       string
	Enabled    bool
}

type AuthFailureRecord struct {
	ClientIP     string
	FailureCount int
	LastFailure  time.Time
}

type AuthService struct {
	mu               sync.RWMutex
	users            map[string]*User
	authEnabled      bool
	defaultApiKeyHash string
	authFailures     map[string]*AuthFailureRecord
	maxAuthFailures  int
	rateLimitSeconds int
	jwtService       *JWTService
}

func NewAuthService() *AuthService {
	return &AuthService{
		users:            make(map[string]*User),
		authFailures:     make(map[string]*AuthFailureRecord),
		maxAuthFailures:  10,
		rateLimitSeconds: 300,
	}
}

func (as *AuthService) Init(authEnabled bool, apiKey string) {
	as.mu.Lock()
	defer as.mu.Unlock()

	as.authEnabled = authEnabled
	if authEnabled && apiKey != "" {
		as.defaultApiKeyHash = hashApiKey(apiKey)
		tokenExpiry := time.Duration(24) * time.Hour
		refreshExpiry := time.Duration(168) * time.Hour
		as.jwtService = NewJWTService(apiKey, tokenExpiry, refreshExpiry)
	}
}

func (as *AuthService) GetJWTService() *JWTService {
	as.mu.RLock()
	defer as.mu.RUnlock()
	return as.jwtService
}

func (as *AuthService) ValidateApiKey(apiKey string) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()

	if !as.authEnabled {
		return true
	}

	hashedKey := hashApiKey(apiKey)

	if as.defaultApiKeyHash != "" && hashedKey == as.defaultApiKeyHash {
		return true
	}

	for _, user := range as.users {
		if user.Enabled && user.ApiKeyHash == hashedKey {
			return true
		}
	}

	// After API key validation fails, try JWT
	if as.jwtService != nil {
		claims, err := as.jwtService.ValidateToken(apiKey)
		if err == nil && claims != nil {
			return true
		}
	}

	return false
}

func (as *AuthService) GetUserByApiKey(apiKey string) *User {
	as.mu.RLock()
	defer as.mu.RUnlock()

	if !as.authEnabled {
		return nil
	}

	hashedKey := hashApiKey(apiKey)

	if as.defaultApiKeyHash != "" && hashedKey == as.defaultApiKeyHash {
		return &User{
			ID:      "default",
			Name:    "Default Admin",
			Role:    "admin",
			Enabled: true,
		}
	}

	for _, user := range as.users {
		if user.Enabled && user.ApiKeyHash == hashedKey {
			return user
		}
	}

	return nil
}

func (as *AuthService) HasPermission(apiKey, resource, action string) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()

	if !as.authEnabled {
		return true
	}

	user := as.getUserByApiKeyInternal(apiKey)
	if user == nil || !user.Enabled {
		return false
	}

	if user.Role == "admin" {
		return true
	}

	if user.Role == "user" && resource == "files" {
		return action == "read" || action == "write"
	}

	return false
}

func (as *AuthService) getUserByApiKeyInternal(apiKey string) *User {
	hashedKey := hashApiKey(apiKey)

	if as.defaultApiKeyHash != "" && hashedKey == as.defaultApiKeyHash {
		return &User{
			ID:      "default",
			Name:    "Default Admin",
			Role:    "admin",
			Enabled: true,
		}
	}

	for _, user := range as.users {
		if user.ApiKeyHash == hashedKey {
			return user
		}
	}

	return nil
}

func (as *AuthService) GenerateApiKey(userId string) string {
	return utils.SHA256(userId + utils.GenerateUUID())
}

func (as *AuthService) CreateUser(user *User) error {
	as.mu.Lock()
	defer as.mu.Unlock()

	if user.ID == "" {
		return fmt.Errorf("user ID cannot be empty")
	}

	if _, exists := as.users[user.ID]; exists {
		return fmt.Errorf("user with ID %s already exists", user.ID)
	}

	as.users[user.ID] = user
	return nil
}

func (as *AuthService) UpdateUser(user *User) error {
	as.mu.Lock()
	defer as.mu.Unlock()

	if user.ID == "" {
		return fmt.Errorf("user ID cannot be empty")
	}

	if _, exists := as.users[user.ID]; !exists {
		return fmt.Errorf("user with ID %s not found", user.ID)
	}

	as.users[user.ID] = user
	return nil
}

func (as *AuthService) DeleteUser(userId string) error {
	as.mu.Lock()
	defer as.mu.Unlock()

	if _, exists := as.users[userId]; !exists {
		return fmt.Errorf("user with ID %s not found", userId)
	}

	delete(as.users, userId)
	return nil
}

func (as *AuthService) GetUserById(userId string) (*User, error) {
	as.mu.RLock()
	defer as.mu.RUnlock()

	user, exists := as.users[userId]
	if !exists {
		return nil, fmt.Errorf("user with ID %s not found", userId)
	}
	return user, nil
}

func (as *AuthService) IsRateLimited(clientIP string) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()

	record, exists := as.authFailures[clientIP]
	if !exists {
		return false
	}

	if time.Since(record.LastFailure).Seconds() > float64(as.rateLimitSeconds) {
		return false
	}

	return record.FailureCount >= as.maxAuthFailures
}

func (as *AuthService) RecordAuthFailure(clientIP string) {
	as.mu.Lock()
	defer as.mu.Unlock()

	clientIP = strings.TrimSpace(clientIP)

	if len(as.authFailures) > 10000 {
		now := time.Now()
		for ip, record := range as.authFailures {
			if now.Sub(record.LastFailure).Seconds() > float64(as.rateLimitSeconds) {
				delete(as.authFailures, ip)
			}
		}
	}

	record, exists := as.authFailures[clientIP]
	if !exists {
		as.authFailures[clientIP] = &AuthFailureRecord{
			ClientIP:     clientIP,
			FailureCount: 1,
			LastFailure:  time.Now(),
		}
		return
	}

	if time.Since(record.LastFailure).Seconds() > float64(as.rateLimitSeconds) {
		record.FailureCount = 1
	} else {
		record.FailureCount++
	}
	record.LastFailure = time.Now()
}

func (as *AuthService) ClearAuthFailure(clientIP string) {
	as.mu.Lock()
	defer as.mu.Unlock()

	delete(as.authFailures, clientIP)
}

func hashApiKey(key string) string {
	h := sha256.New()
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}
