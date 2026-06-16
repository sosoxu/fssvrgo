package distributed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type DistributedLock interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (string, error)
	Unlock(ctx context.Context, key string, token string) error
	Extend(ctx context.Context, key string, token string, ttl time.Duration) error
}

type RedisDistributedLock struct {
	client *redis.Client
}

func NewRedisDistributedLock(client *redis.Client) *RedisDistributedLock {
	return &RedisDistributedLock{client: client}
}

func (l *RedisDistributedLock) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	token := generateToken()
	lockKey := fmt.Sprintf("fsserver:lock:%s", key)

	ok, err := l.client.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil {
		return "", fmt.Errorf("redis lock failed: %w", err)
	}
	if !ok {
		return "", ErrLockConflict
	}

	return token, nil
}

func (l *RedisDistributedLock) Unlock(ctx context.Context, key string, token string) error {
	lockKey := fmt.Sprintf("fsserver:lock:%s", key)

	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, l.client, []string{lockKey}, token).Int()
	if err != nil {
		return fmt.Errorf("redis unlock failed: %w", err)
	}
	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

func (l *RedisDistributedLock) Extend(ctx context.Context, key string, token string, ttl time.Duration) error {
	lockKey := fmt.Sprintf("fsserver:lock:%s", key)

	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, l.client, []string{lockKey}, token, int(ttl.Milliseconds())).Int()
	if err != nil {
		return fmt.Errorf("redis extend lock failed: %w", err)
	}
	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

type LockGuard struct {
	lock   DistributedLock
	key    string
	token  string
	ctx    context.Context
}

func (lg *LockGuard) Unlock() error {
	return lg.lock.Unlock(lg.ctx, lg.key, lg.token)
}

type LocalDistributedLock struct {
}

func NewLocalDistributedLock() *LocalDistributedLock {
	return &LocalDistributedLock{}
}

func (l *LocalDistributedLock) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return "local", nil
}

func (l *LocalDistributedLock) Unlock(ctx context.Context, key string, token string) error {
	return nil
}

func (l *LocalDistributedLock) Extend(ctx context.Context, key string, token string, ttl time.Duration) error {
	return nil
}

func AcquireLock(ctx context.Context, dl DistributedLock, key string, ttl time.Duration, retryCount int, retryDelay time.Duration) (string, error) {
	var lastErr error
	baseDelay := retryDelay
	for i := 0; i < retryCount; i++ {
		token, err := dl.Lock(ctx, key, ttl)
		if err == nil {
			return token, nil
		}
		if !errors.Is(err, ErrLockConflict) {
			return "", err
		}
		lastErr = err

		delay := baseDelay
		if i > 0 {
			backoff := time.Duration(1 << uint(i-1)) * baseDelay
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			jitter := time.Duration(randInt63n(int64(backoff) / 2))
			delay = backoff + jitter
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
		}
	}
	return "", fmt.Errorf("failed to acquire lock after %d retries: %w", retryCount, lastErr)
}

func randInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	b := make([]byte, 8)
	rand.Read(b)
	v := int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
	if v < 0 {
		v = -v
	}
	return v % n
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

var (
	ErrLockConflict = errors.New("lock conflict: another instance holds the lock")
	ErrLockNotHeld  = errors.New("lock not held by this owner")
)
