package queue

import (
	"context"
	"time"
)

// Queue defines the interface for job queue operations
type Queue interface {
	// Enqueue adds a job ID to the queue
	Enqueue(ctx context.Context, jobID string) error

	// Dequeue retrieves a job ID from the queue (blocking operation)
	Dequeue(ctx context.Context) (string, error)

	// AcquireLock attempts to acquire a distributed lock for a job
	// Returns true if lock was acquired, false if already locked
	AcquireLock(ctx context.Context, jobID string, ttl time.Duration) (bool, error)

	// ReleaseLock releases a distributed lock for a job
	// Returns error if lock release fails
	ReleaseLock(ctx context.Context, jobID string) error

	// EnqueueDelayed adds a job ID to the delayed queue with a specific delay
	// The job will be moved to the main queue after the delay expires
	EnqueueDelayed(ctx context.Context, jobID string, delay time.Duration) error

	// ProcessDelayedQueue moves ready jobs from delayed queue to main queue
	// Should be called periodically by a background goroutine
	ProcessDelayedQueue(ctx context.Context) error
}

