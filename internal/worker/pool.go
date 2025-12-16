package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/voduybaokhanh/go-job-queue/internal/job"
	"github.com/voduybaokhanh/go-job-queue/internal/lock"
	"github.com/voduybaokhanh/go-job-queue/internal/queue"
	"github.com/voduybaokhanh/go-job-queue/internal/retry"
	"github.com/voduybaokhanh/go-job-queue/internal/storage"
)

// JobHandler defines the interface for processing jobs
type JobHandler interface {
	// ProcessJob processes a job and returns an error if processing fails
	ProcessJob(ctx context.Context, job *job.Job) error
}

// Pool represents a worker pool with dispatcher and workers
type Pool struct {
	numWorkers   int
	queue        queue.Queue
	storage      storage.Storage
	handler      JobHandler
	lock         lock.Lock
	jobChan      chan string
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	lockTTL      time.Duration
	jobTimeout   time.Duration
	logger       *slog.Logger
}

// Config holds configuration for the worker pool
type Config struct {
	NumWorkers int
	Queue      queue.Queue
	Storage    storage.Storage
	Handler    JobHandler
	Lock       lock.Lock
	LockTTL    time.Duration
	JobTimeout time.Duration
	Logger     *slog.Logger
}

// NewPool creates a new worker pool with the given configuration
func NewPool(cfg Config) *Pool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 1
	}
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 5 * time.Minute
	}
	if cfg.JobTimeout == 0 {
		cfg.JobTimeout = 5 * time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Pool{
		numWorkers: cfg.NumWorkers,
		queue:      cfg.Queue,
		storage:    cfg.Storage,
		handler:    cfg.Handler,
		lock:       cfg.Lock,
		jobChan:    make(chan string, cfg.NumWorkers*2), // Buffered channel
		ctx:        ctx,
		cancel:     cancel,
		lockTTL:    cfg.LockTTL,
		jobTimeout: cfg.JobTimeout,
		logger:     cfg.Logger,
	}
}

// Start starts the worker pool (dispatcher + workers)
func (p *Pool) Start() {
	p.logger.Info("Starting worker pool", "num_workers", p.numWorkers)

	// Start dispatcher
	p.wg.Add(1)
	go p.dispatcher()

	// Start delayed queue processor
	p.wg.Add(1)
	go p.delayedQueueProcessor()

	// Start workers
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Stop gracefully stops the worker pool
func (p *Pool) Stop() {
	p.logger.Info("Stopping worker pool")
	p.cancel()
	close(p.jobChan)
	p.wg.Wait()
	p.logger.Info("Worker pool stopped")
}

// dispatcher continuously dequeues jobs and sends them to the job channel
func (p *Pool) dispatcher() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Dispatcher stopped")
			return
		default:
			jobID, err := p.queue.Dequeue(p.ctx)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return
				}
				p.logger.Error("Error dequeuing job", "error", err)
				// Continue to retry
				continue
			}

			// Empty job ID means queue is empty (BRPOP timeout)
			if jobID == "" {
				continue
			}

			// Send job ID to workers via channel
			select {
			case p.jobChan <- jobID:
				// Job sent successfully
			case <-p.ctx.Done():
				return
			}
		}
	}
}

// delayedQueueProcessor periodically processes the delayed queue
// Moves ready jobs from delayed queue to main queue
func (p *Pool) delayedQueueProcessor() {
	defer p.wg.Done()

	ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
	defer ticker.Stop()

	// Process immediately on start
	p.queue.ProcessDelayedQueue(p.ctx)

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Delayed queue processor stopped")
			return
		case <-ticker.C:
			if err := p.queue.ProcessDelayedQueue(p.ctx); err != nil {
				p.logger.Error("Error processing delayed queue", "error", err)
			}
		}
	}
}

// worker processes jobs from the job channel
func (p *Pool) worker(id int) {
	defer p.wg.Done()

	p.logger.Info("Worker started", "worker_id", id)

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Worker stopped", "worker_id", id)
			return
		case jobID, ok := <-p.jobChan:
			if !ok {
				// Channel closed
				p.logger.Info("Job channel closed", "worker_id", id)
				return
			}

			// Process job with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						p.logger.Error("Panic recovered while processing job",
							"worker_id", id,
							"job_id", jobID,
							"panic", r)
						// Update job status to Failed on panic
						p.handleJobError(jobID, fmt.Errorf("panic: %v", r))
					}
				}()

				p.processJob(jobID)
			}()
		}
	}
}

// processJob handles a single job following the strict flow:
// 1. Pull job_id (already done by dispatcher)
// 2. Acquire lock (Redis, TTL)
// 3. Load job (DB)
// 4. Idempotency check
// 5. Mark PROCESSING (atomic)
// 6. Execute handler (with timeout)
// 7. DONE / RETRY / FAILED
// 8. Release lock
func (p *Pool) processJob(jobIDStr string) {
	// Parse job ID
	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		p.logger.Error("Invalid job ID", "job_id", jobIDStr, "error", err)
		return
	}

	// Step 1: Load job from storage first to check status
	j, err := p.storage.GetJob(p.ctx, jobID)
	if err != nil {
		p.logger.Error("Error getting job from storage", "job_id", jobIDStr, "error", err)
		return
	}

	// Check if job is already DONE - skip processing
	if j.Status == job.StatusDone {
		p.logger.Info("Job already done, skipping", "job_id", jobIDStr)
		return
	}

	// Step 2: Acquire lock - if failed, another worker is processing this job
	lockOwnerID, acquired, err := p.lock.AcquireLock(p.ctx, jobIDStr, p.lockTTL)
	if err != nil {
		p.logger.Error("Error acquiring lock for job", "job_id", jobIDStr, "error", err)
		return
	}
	if !acquired {
		// Another worker is processing this job
		p.logger.Info("Lock not acquired, another worker is processing", "job_id", jobIDStr)
		return
	}

	// Ensure lock is released after processing (success or failure)
	defer func() {
		if releaseErr := p.lock.ReleaseLock(p.ctx, jobIDStr, lockOwnerID); releaseErr != nil {
			p.logger.Error("Error releasing lock", "job_id", jobIDStr, "owner_id", lockOwnerID, "error", releaseErr)
		}
	}()

	// Step 3: Reload job to get latest state after acquiring lock
	j, err = p.storage.GetJob(p.ctx, jobID)
	if err != nil {
		p.logger.Error("Error reloading job from storage", "job_id", jobIDStr, "error", err)
		return
	}

	// Double-check status after acquiring lock (race condition protection)
	if j.Status == job.StatusDone {
		p.logger.Info("Job already done after lock acquisition, skipping", "job_id", jobIDStr)
		return
	}

	// Step 4: Idempotency check - check if job with same idempotency_key already exists and is DONE
	if j.IdempotencyKey != "" {
		existingJob, err := p.storage.GetJobByIdempotencyKey(p.ctx, j.IdempotencyKey)
		if err != nil {
			p.logger.Error("Error checking idempotency key", "job_id", jobIDStr, "idempotency_key", j.IdempotencyKey, "error", err)
			// Continue processing if check fails (fail open)
		} else if existingJob != nil && existingJob.ID != jobID && existingJob.Status == job.StatusDone {
			// Another job with same idempotency key already completed successfully
			p.logger.Info("Job with same idempotency key already done, marking as duplicate",
				"job_id", jobIDStr,
				"idempotency_key", j.IdempotencyKey,
				"existing_job_id", existingJob.ID.String())
			
			// Mark current job as DONE (duplicate) - atomic update
			updated, updateErr := p.storage.UpdateStatusAtomic(p.ctx, jobID, job.StatusPending, job.StatusDone)
			if updateErr != nil {
				p.logger.Error("Error marking duplicate job as Done", "job_id", jobIDStr, "error", updateErr)
			} else if !updated {
				// Try updating from any status to Done
				if updateErr := p.storage.UpdateStatus(p.ctx, jobID, job.StatusDone); updateErr != nil {
					p.logger.Error("Error updating duplicate job status to Done", "job_id", jobIDStr, "error", updateErr)
				}
			}
			return
		}
	}

	// Step 5: Atomically update status from Pending to Processing
	updated, err := p.storage.UpdateStatusAtomic(p.ctx, jobID, job.StatusPending, job.StatusProcessing)
	if err != nil {
		p.logger.Error("Error atomically updating job status to Processing", "job_id", jobIDStr, "error", err)
		return
	}
	if !updated {
		// Status didn't match (another worker might have processed it)
		p.logger.Info("Job status update failed, status mismatch", "job_id", jobIDStr)
		return
	}

	p.logger.Info("Processing job", "job_id", jobIDStr, "type", j.Type, "lock_owner", lockOwnerID)

	// Step 6: Execute handler with per-job timeout
	jobCtx, jobCancel := context.WithTimeout(p.ctx, p.jobTimeout)
	defer jobCancel()

	err = p.handler.ProcessJob(jobCtx, j)
	if err != nil {
		if jobCtx.Err() == context.DeadlineExceeded {
			p.logger.Error("Job processing timeout exceeded", "job_id", jobIDStr, "timeout", p.jobTimeout)
		} else {
			p.logger.Error("Job processing failed", "job_id", jobIDStr, "error", err)
		}
		p.handleJobError(jobIDStr, err)
		return
	}

	// Step 7: Job completed successfully - atomically update from Processing to Done
	updated, err = p.storage.UpdateStatusAtomic(p.ctx, jobID, job.StatusProcessing, job.StatusDone)
	if err != nil {
		p.logger.Error("Error atomically updating job status to Done", "job_id", jobIDStr, "error", err)
		return
	}
	if !updated {
		p.logger.Warn("Job status update to Done failed, status mismatch", "job_id", jobIDStr)
		return
	}

	p.logger.Info("Job completed successfully", "job_id", jobIDStr)
}

// handleJobError handles job processing errors, including retry logic with exponential backoff
func (p *Pool) handleJobError(jobIDStr string, err error) {
	jobID, parseErr := uuid.Parse(jobIDStr)
	if parseErr != nil {
		p.logger.Error("Invalid job ID in handleJobError", "job_id", jobIDStr)
		return
	}

	// Get current job state
	j, getErr := p.storage.GetJob(p.ctx, jobID)
	if getErr != nil {
		p.logger.Error("Error getting job for error handling", "job_id", jobIDStr, "error", getErr)
		return
	}

	// Increment retry count
	if incErr := p.storage.IncrementRetryCount(p.ctx, jobID); incErr != nil {
		p.logger.Error("Error incrementing retry count", "job_id", jobIDStr, "error", incErr)
	}

	// Reload job to get updated retry count
	j, getErr = p.storage.GetJob(p.ctx, jobID)
	if getErr != nil {
		p.logger.Error("Error reloading job after incrementing retry", "job_id", jobIDStr, "error", getErr)
		return
	}

	// Check if job can be retried
	if j.CanRetry() {
		// Calculate exponential backoff delay using pure function
		baseDelay := 1 * time.Second
		maxDelay := 5 * time.Minute
		backoffDelay := retry.CalculateBackoff(j.RetryCount, baseDelay, maxDelay)

		p.logger.Info("Job failed, will retry",
			"job_id", jobIDStr,
			"backoff_delay", backoffDelay.String(),
			"retry_count", j.RetryCount,
			"max_retries", j.MaxRetries)

		// Update status back to Pending for retry
		if updateErr := p.storage.UpdateStatus(p.ctx, jobID, job.StatusPending); updateErr != nil {
			p.logger.Error("Error updating job status to Pending for retry", "job_id", jobIDStr, "error", updateErr)
			return
		}

		// Enqueue job to delayed queue with backoff delay
		// The delayed queue processor will move it to main queue when ready
		if enqueueErr := p.queue.EnqueueDelayed(p.ctx, jobIDStr, backoffDelay); enqueueErr != nil {
			p.logger.Error("Error enqueuing job to delayed queue for retry", "job_id", jobIDStr, "error", enqueueErr)
		} else {
			p.logger.Info("Job enqueued to delayed queue for retry",
				"job_id", jobIDStr,
				"backoff_delay", backoffDelay.String(),
				"retry_count", j.RetryCount,
				"max_retries", j.MaxRetries)
		}
	} else {
		// Max retries exceeded, mark as Failed (try atomic update from Processing to Failed)
		updated, updateErr := p.storage.UpdateStatusAtomic(p.ctx, jobID, job.StatusProcessing, job.StatusFailed)
		if updateErr != nil {
			p.logger.Error("Error atomically updating job status to Failed", "job_id", jobIDStr, "error", updateErr)
		} else if !updated {
			// If status wasn't Processing, try updating from any status to Failed
			if updateErr := p.storage.UpdateStatus(p.ctx, jobID, job.StatusFailed); updateErr != nil {
				p.logger.Error("Error updating job status to Failed", "job_id", jobIDStr, "error", updateErr)
			} else {
				p.logger.Error("Job failed after max retries",
					"job_id", jobIDStr,
					"max_retries", j.MaxRetries,
					"error", err)
			}
		} else {
			p.logger.Error("Job failed after max retries",
				"job_id", jobIDStr,
				"max_retries", j.MaxRetries,
				"error", err)
		}
	}
}

