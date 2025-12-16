package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status represents the current state of a job
type Status string

const (
	StatusPending    Status = "Pending"
	StatusProcessing Status = "Processing"
	StatusDone       Status = "Done"
	StatusFailed     Status = "Failed"
)

// Job represents a job in the queue system
type Job struct {
	ID             uuid.UUID       `json:"id" db:"id"`
	Type           string          `json:"type" db:"type"`
	Payload        json.RawMessage `json:"payload" db:"payload"`
	Status         Status          `json:"status" db:"status"`
	RetryCount     int             `json:"retry_count" db:"retry_count"`
	MaxRetries     int             `json:"max_retries" db:"max_retries"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty" db:"scheduled_at"`
	IdempotencyKey string          `json:"idempotency_key,omitempty" db:"idempotency_key"`
	CreatedAt      time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at" db:"updated_at"`
}

// IsValidStatus checks if the status is a valid job status
func IsValidStatus(s Status) bool {
	return s == StatusPending || s == StatusProcessing || s == StatusDone || s == StatusFailed
}

// CanRetry checks if the job can be retried based on retry count
func (j *Job) CanRetry() bool {
	return j.RetryCount < j.MaxRetries
}

