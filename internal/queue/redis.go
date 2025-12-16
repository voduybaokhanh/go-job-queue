package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultQueueKey is the default Redis key for the job queue list
	DefaultQueueKey = "jobqueue:jobs"
	// LockKeyPrefix is the prefix for lock keys
	LockKeyPrefix = "jobqueue:lock:"
)

// RedisQueue implements the Queue interface using Redis
type RedisQueue struct {
	client  *redis.Client
	queueKey string
}

// NewRedisQueue creates a new RedisQueue instance
func NewRedisQueue(client *redis.Client) Queue {
	return &RedisQueue{
		client:   client,
		queueKey: DefaultQueueKey,
	}
}

// Enqueue adds a job ID to the queue using LPUSH
func (q *RedisQueue) Enqueue(ctx context.Context, jobID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	err := q.client.LPush(ctx, q.queueKey, jobID).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue job %s: %w", jobID, err)
	}

	return nil
}

// Dequeue retrieves a job ID from the queue using BRPOP (blocking pop)
// Blocks for up to 5 seconds if queue is empty, checking context periodically
func (q *RedisQueue) Dequeue(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	// Use BRPOP with 5 second timeout for blocking pop
	// This saves CPU compared to polling
	result, err := q.client.BRPop(ctx, 5*time.Second, q.queueKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Queue is empty, check if context was cancelled
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			// Return empty string for empty queue (caller can retry)
			return "", nil
		}
		return "", fmt.Errorf("failed to dequeue job: %w", err)
	}

	// BRPOP returns [key, value], we want the value (job ID)
	if len(result) < 2 {
		return "", fmt.Errorf("unexpected BRPOP result format")
	}

	jobID := result[1]

	// Check context after operation
	if err := ctx.Err(); err != nil {
		return "", err
	}

	return jobID, nil
}

// AcquireLock attempts to acquire a distributed lock using SETNX with expiration
// Returns true if lock was acquired, false if already locked
func (q *RedisQueue) AcquireLock(ctx context.Context, jobID string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	lockKey := fmt.Sprintf("%s%s", LockKeyPrefix, jobID)

	// Use SET with NX (only if not exists) and EX (expiration) for atomic operation
	// This ensures only one worker can acquire the lock
	ok, err := q.client.SetNX(ctx, lockKey, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock for job %s: %w", jobID, err)
	}

	return ok, nil
}

