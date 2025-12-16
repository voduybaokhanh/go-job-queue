package storage

import (
	"context"

	"github.com/google/uuid"
	"github.com/voduybaokhanh/go-job-queue/internal/job"
)

// Storage defines the interface for job persistence operations
type Storage interface {
	// CreateJob creates a new job in the storage
	CreateJob(ctx context.Context, job *job.Job) error

	// UpdateStatus updates the status of an existing job
	UpdateStatus(ctx context.Context, jobID uuid.UUID, status job.Status) error

	// GetJob retrieves a job by its ID
	GetJob(ctx context.Context, jobID uuid.UUID) (*job.Job, error)

	// IncrementRetryCount increments the retry count for a job
	IncrementRetryCount(ctx context.Context, jobID uuid.UUID) error

	// UpdateStatusAtomic atomically updates status only if current status matches
	// Returns true if the update was successful (status matched and was updated), false if status didn't match
	UpdateStatusAtomic(ctx context.Context, jobID uuid.UUID, fromStatus, toStatus job.Status) (bool, error)

	// GetJobByIdempotencyKey retrieves a job by its idempotency key
	// Returns nil if no job is found with the given key
	GetJobByIdempotencyKey(ctx context.Context, idempotencyKey string) (*job.Job, error)
}
