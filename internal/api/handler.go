package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/voduybaokhanh/go-job-queue/internal/job"
	"github.com/voduybaokhanh/go-job-queue/internal/queue"
	"github.com/voduybaokhanh/go-job-queue/internal/storage"
)

// Handler handles HTTP requests for the job queue API
type Handler struct {
	storage storage.Storage
	queue   queue.Queue
	logger  *slog.Logger
}

// NewHandler creates a new API handler
func NewHandler(storage storage.Storage, queue queue.Queue, logger *slog.Logger) *Handler {
	return &Handler{
		storage: storage,
		queue:   queue,
		logger:  logger,
	}
}

// CreateJobRequest represents the request body for creating a job
type CreateJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// CreateJobResponse represents the response for creating a job
type CreateJobResponse struct {
	ID     string      `json:"id"`
	Status job.Status  `json:"status"`
	Type   string      `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// GetJobResponse represents the response for getting a job
type GetJobResponse struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         job.Status      `json:"status"`
	RetryCount     int             `json:"retry_count"`
	MaxRetries     int             `json:"max_retries"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// CreateJob handles POST /api/v1/jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate required fields
	if req.Type == "" {
		h.respondError(w, http.StatusBadRequest, "Type is required", nil)
		return
	}

	// Check idempotency if key is provided
	if req.IdempotencyKey != "" {
		existingJob, err := h.storage.GetJobByIdempotencyKey(ctx, req.IdempotencyKey)
		if err == nil && existingJob != nil {
			// Job with this idempotency key already exists, return it
			h.logger.Info("Job with idempotency key already exists",
				"idempotency_key", req.IdempotencyKey,
				"job_id", existingJob.ID.String())
			h.respondJSON(w, http.StatusOK, CreateJobResponse{
				ID:      existingJob.ID.String(),
				Status:  existingJob.Status,
				Type:    existingJob.Type,
				Payload: existingJob.Payload,
			})
			return
		}
	}

	// Set default max retries if not provided
	maxRetries := 3
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
		if maxRetries < 0 {
			maxRetries = 0
		}
	}

	// Create new job
	newJob := &job.Job{
		ID:             uuid.New(),
		Type:           req.Type,
		Payload:        req.Payload,
		Status:         job.StatusPending,
		RetryCount:     0,
		MaxRetries:     maxRetries,
		ScheduledAt:    req.ScheduledAt,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	// Save job to storage
	if err := h.storage.CreateJob(ctx, newJob); err != nil {
		h.logger.Error("Failed to create job", "error", err, "type", req.Type)
		h.respondError(w, http.StatusInternalServerError, "Failed to create job", err)
		return
	}

	// Enqueue job if not scheduled (or if scheduled time has passed)
	if req.ScheduledAt == nil || req.ScheduledAt.Before(time.Now()) {
		if err := h.queue.Enqueue(ctx, newJob.ID.String()); err != nil {
			h.logger.Error("Failed to enqueue job", "error", err, "job_id", newJob.ID.String())
			// Job is created but not enqueued - this is a problem but we'll return success
			// In production, you might want to handle this differently
		}
	}

	h.logger.Info("Job created", "job_id", newJob.ID.String(), "type", req.Type)

	h.respondJSON(w, http.StatusCreated, CreateJobResponse{
		ID:      newJob.ID.String(),
		Status:  newJob.Status,
		Type:    newJob.Type,
		Payload: newJob.Payload,
	})
}

// GetJob handles GET /api/v1/jobs/:id
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract job ID from URL path
	jobIDStr := r.PathValue("id")
	if jobIDStr == "" {
		h.respondError(w, http.StatusBadRequest, "Job ID is required", nil)
		return
	}

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid job ID format", err)
		return
	}

	// Get job from storage
	j, err := h.storage.GetJob(ctx, jobID)
	if err != nil {
		h.logger.Error("Failed to get job", "error", err, "job_id", jobIDStr)
		h.respondError(w, http.StatusNotFound, "Job not found", err)
		return
	}

	h.respondJSON(w, http.StatusOK, GetJobResponse{
		ID:             j.ID.String(),
		Type:           j.Type,
		Payload:        j.Payload,
		Status:         j.Status,
		RetryCount:     j.RetryCount,
		MaxRetries:     j.MaxRetries,
		ScheduledAt:    j.ScheduledAt,
		IdempotencyKey: j.IdempotencyKey,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
	})
}

// CancelJob handles POST /api/v1/jobs/:id/cancel
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract job ID from URL path
	jobIDStr := r.PathValue("id")
	if jobIDStr == "" {
		h.respondError(w, http.StatusBadRequest, "Job ID is required", nil)
		return
	}

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid job ID format", err)
		return
	}

	// Get current job status
	j, err := h.storage.GetJob(ctx, jobID)
	if err != nil {
		h.logger.Error("Failed to get job for cancellation", "error", err, "job_id", jobIDStr)
		h.respondError(w, http.StatusNotFound, "Job not found", err)
		return
	}

	// Check if job can be cancelled (only Pending or Processing jobs can be cancelled)
	if j.Status == job.StatusDone {
		h.respondError(w, http.StatusBadRequest, "Cannot cancel a completed job", nil)
		return
	}
	if j.Status == job.StatusFailed {
		h.respondError(w, http.StatusBadRequest, "Cannot cancel a failed job", nil)
		return
	}

	// Update status to Failed (cancelled)
	updated, err := h.storage.UpdateStatusAtomic(ctx, jobID, j.Status, job.StatusFailed)
	if err != nil {
		h.logger.Error("Failed to cancel job", "error", err, "job_id", jobIDStr)
		h.respondError(w, http.StatusInternalServerError, "Failed to cancel job", err)
		return
	}
	if !updated {
		h.logger.Warn("Job status changed during cancellation", "job_id", jobIDStr, "current_status", j.Status)
		h.respondError(w, http.StatusConflict, "Job status changed, cannot cancel", nil)
		return
	}

	h.logger.Info("Job cancelled", "job_id", jobIDStr)

	h.respondJSON(w, http.StatusOK, map[string]string{
		"message": "Job cancelled successfully",
		"job_id":  jobIDStr,
	})
}

// respondJSON sends a JSON response
func (h *Handler) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("Failed to encode JSON response", "error", err)
	}
}

// respondError sends an error response
func (h *Handler) respondError(w http.ResponseWriter, status int, message string, err error) {
	errorMsg := message
	if err != nil {
		errorMsg = fmt.Sprintf("%s: %v", message, err)
	}

	h.logger.Warn("API error", "status", status, "message", message, "error", err)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   http.StatusText(status),
		Message: errorMsg,
	})
}

