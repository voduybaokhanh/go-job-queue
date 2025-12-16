package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/voduybaokhanh/go-job-queue/internal/job"
)

// PostgresStorage implements Storage interface using PostgreSQL
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage creates a new PostgresStorage instance
func NewPostgresStorage(pool *pgxpool.Pool) Storage {
	return &PostgresStorage{
		pool: pool,
	}
}

// CreateJob creates a new job in the storage
func (s *PostgresStorage) CreateJob(ctx context.Context, j *job.Job) error {
	query := `
		INSERT INTO jobs (id, type, payload, status, retry_count, max_retries, scheduled_at, idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	_, err := s.pool.Exec(ctx, query,
		j.ID, j.Type, j.Payload, j.Status, j.RetryCount, j.MaxRetries,
		j.ScheduledAt, j.IdempotencyKey, j.CreatedAt, j.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	return nil
}

// UpdateStatus updates the status of an existing job
func (s *PostgresStorage) UpdateStatus(ctx context.Context, jobID uuid.UUID, status job.Status) error {
	query := `UPDATE jobs SET status = $1 WHERE id = $2`
	_, err := s.pool.Exec(ctx, query, status, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	return nil
}

// GetJob retrieves a job by its ID
func (s *PostgresStorage) GetJob(ctx context.Context, jobID uuid.UUID) (*job.Job, error) {
	query := `
		SELECT id, type, payload, status, retry_count, max_retries, scheduled_at, idempotency_key, created_at, updated_at
		FROM jobs
		WHERE id = $1`

	var j job.Job
	var scheduledAt *time.Time

	err := s.pool.QueryRow(ctx, query, jobID).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.RetryCount, &j.MaxRetries,
		&scheduledAt, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("job not found: %w", err)
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	j.ScheduledAt = scheduledAt

	return &j, nil
}

// IncrementRetryCount increments the retry count for a job
func (s *PostgresStorage) IncrementRetryCount(ctx context.Context, jobID uuid.UUID) error {
	query := `UPDATE jobs SET retry_count = retry_count + 1 WHERE id = $1`
	_, err := s.pool.Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("failed to increment retry count: %w", err)
	}

	return nil
}

// UpdateStatusAtomic atomically updates status only if current status matches
// Returns true if the update was successful (status matched and was updated), false if status didn't match
func (s *PostgresStorage) UpdateStatusAtomic(ctx context.Context, jobID uuid.UUID, fromStatus, toStatus job.Status) (bool, error) {
	query := `UPDATE jobs SET status = $1 WHERE id = $2 AND status = $3 RETURNING id`
	var updatedID uuid.UUID
	err := s.pool.QueryRow(ctx, query, toStatus, jobID, fromStatus).Scan(&updatedID)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Status didn't match, no row was updated
			return false, nil
		}
		return false, fmt.Errorf("failed to atomically update job status: %w", err)
	}

	return true, nil
}

// GetJobByIdempotencyKey retrieves a job by its idempotency key
func (s *PostgresStorage) GetJobByIdempotencyKey(ctx context.Context, idempotencyKey string) (*job.Job, error) {
	query := `
		SELECT id, type, payload, status, retry_count, max_retries, scheduled_at, idempotency_key, created_at, updated_at
		FROM jobs
		WHERE idempotency_key = $1 AND idempotency_key != ''`

	var j job.Job
	var scheduledAt *time.Time

	err := s.pool.QueryRow(ctx, query, idempotencyKey).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.RetryCount, &j.MaxRetries,
		&scheduledAt, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No job found with this key
		}
		return nil, fmt.Errorf("failed to get job by idempotency key: %w", err)
	}

	j.ScheduledAt = scheduledAt

	return &j, nil
}

// Close closes the database connection pool
func (s *PostgresStorage) Close() {
	s.pool.Close()
}

