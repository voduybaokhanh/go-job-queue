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
	// DelayedQueueKey is the Redis key for the delayed job queue (sorted set)
	DelayedQueueKey = "jobqueue:delayed"
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

// ReleaseLock releases a distributed lock for a job using Lua script for atomicity
// The Lua script ensures only the lock owner can release it
func (q *RedisQueue) ReleaseLock(ctx context.Context, jobID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("%s%s", LockKeyPrefix, jobID)

	// Lua script to atomically delete the lock key
	// This ensures atomicity even if multiple operations are attempted
	script := `
		if redis.call("EXISTS", KEYS[1]) == 1 then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	_, err := q.client.Eval(ctx, script, []string{lockKey}).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock for job %s: %w", jobID, err)
	}

	return nil
}

// EnqueueDelayed adds a job ID to the delayed queue using Redis Sorted Set
// The score is the timestamp when the job should be enqueued
func (q *RedisQueue) EnqueueDelayed(ctx context.Context, jobID string, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Calculate the timestamp when the job should be enqueued
	enqueueAt := time.Now().Add(delay).Unix()

	// Add to sorted set with score = timestamp
	err := q.client.ZAdd(ctx, DelayedQueueKey, redis.Z{
		Score:  float64(enqueueAt),
		Member: jobID,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue delayed job %s: %w", jobID, err)
	}

	return nil
}

// ProcessDelayedQueue moves ready jobs from delayed queue to main queue
// Uses ZRANGEBYSCORE to get jobs with score <= current timestamp
func (q *RedisQueue) ProcessDelayedQueue(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	currentTime := time.Now().Unix()

	// Get jobs that are ready to be enqueued (score <= current timestamp)
	// Limit to 100 jobs per call to avoid blocking
	jobs, err := q.client.ZRangeByScore(ctx, DelayedQueueKey, &redis.ZRangeBy{
		Min:   "0",
		Max:   fmt.Sprintf("%d", currentTime),
		Count: 100,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get delayed jobs: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	// Move jobs to main queue and remove from delayed queue atomically using pipeline
	pipe := q.client.Pipeline()
	for _, jobID := range jobs {
		// Add to main queue
		pipe.LPush(ctx, q.queueKey, jobID)
		// Remove from delayed queue
		pipe.ZRem(ctx, DelayedQueueKey, jobID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to move delayed jobs to main queue: %w", err)
	}

	return nil
}

