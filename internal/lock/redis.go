package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// LockKeyPrefix is the prefix for lock keys in Redis
	LockKeyPrefix = "jobqueue:lock:"
)

// RedisLock implements the Lock interface using Redis
type RedisLock struct {
	client *redis.Client
}

// NewRedisLock creates a new RedisLock instance
func NewRedisLock(client *redis.Client) Lock {
	return &RedisLock{
		client: client,
	}
}

// AcquireLock attempts to acquire a distributed lock using SETNX with expiration.
// Returns a unique owner ID if lock was acquired, empty string if already locked.
func (l *RedisLock) AcquireLock(ctx context.Context, resourceID string, ttl time.Duration) (ownerID string, acquired bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}

	lockKey := fmt.Sprintf("%s%s", LockKeyPrefix, resourceID)

	// Generate unique owner ID for this lock acquisition
	ownerID = uuid.New().String()

	// Use SET with NX (only if not exists) and EX (expiration) for atomic operation
	// Store owner ID as the value to enable owner verification on release
	ok, err := l.client.SetNX(ctx, lockKey, ownerID, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("failed to acquire lock for resource %s: %w", resourceID, err)
	}

	if !ok {
		// Lock already exists, return empty owner ID
		return "", false, nil
	}

	return ownerID, true, nil
}

// ReleaseLock releases a distributed lock using Lua script for atomicity.
// The Lua script ensures only the lock owner can release it by verifying the owner ID.
func (l *RedisLock) ReleaseLock(ctx context.Context, resourceID string, ownerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if ownerID == "" {
		return fmt.Errorf("owner ID is required to release lock")
	}

	lockKey := fmt.Sprintf("%s%s", LockKeyPrefix, resourceID)

	// Lua script to atomically verify owner and delete the lock key
	// This ensures only the lock owner can release it, preventing accidental releases
	script := `
		local lockValue = redis.call("GET", KEYS[1])
		if lockValue == false then
			return 0
		end
		if lockValue == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	result, err := l.client.Eval(ctx, script, []string{lockKey}, ownerID).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock for resource %s: %w", resourceID, err)
	}

	// Check if lock was actually released (result == 1) or owner didn't match (result == 0)
	if result == int64(0) {
		return fmt.Errorf("lock release failed: lock does not exist or owner ID does not match")
	}

	return nil
}

