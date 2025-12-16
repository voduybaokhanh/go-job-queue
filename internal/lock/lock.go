package lock

import (
	"context"
	"time"
)

// Lock defines the interface for distributed locking operations
type Lock interface {
	// AcquireLock attempts to acquire a distributed lock for a resource.
	// Returns the owner ID if lock was acquired, empty string if already locked, and error on failure.
	// The owner ID must be used when releasing the lock to ensure only the owner can release it.
	AcquireLock(ctx context.Context, resourceID string, ttl time.Duration) (ownerID string, acquired bool, err error)

	// ReleaseLock releases a distributed lock for a resource.
	// Only succeeds if the ownerID matches the lock owner, ensuring safe release.
	// Returns error if lock release fails or owner doesn't match.
	ReleaseLock(ctx context.Context, resourceID string, ownerID string) error
}

