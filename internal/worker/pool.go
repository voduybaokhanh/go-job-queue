package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/voduybaokhanh/go-job-queue/internal/job"
	"github.com/voduybaokhanh/go-job-queue/internal/queue"
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
	jobChan      chan string
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	lockTTL      time.Duration
	logger       *slog.Logger
}

// Config holds configuration for the worker pool
type Config struct {
	NumWorkers int
	Queue      queue.Queue
	Storage    storage.Storage
	Handler    JobHandler
	LockTTL    time.Duration
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
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Pool{
		numWorkers: cfg.NumWorkers,
		queue:      cfg.Queue,
		storage:    cfg.Storage,
		handler:    cfg.Handler,
		jobChan:    make(chan string, cfg.NumWorkers*2), // Buffered channel
		ctx:        ctx,
		cancel:     cancel,
		lockTTL:    cfg.LockTTL,
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

// processJob handles a single job
func (p *Pool) processJob(jobIDStr string) {
	// Parse job ID
	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		p.logger.Error("Invalid job ID", "job_id", jobIDStr, "error", err)
		return
	}

	// Load job from storage first to check status and idempotency
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

	// Try to acquire lock - if failed, another worker is processing this job
	acquired, err := p.queue.AcquireLock(p.ctx, jobIDStr, p.lockTTL)
	if err != nil {
		p.logger.Error("Error acquiring lock for job", "job_id", jobIDStr, "error", err)
		return
	}
	if !acquired {
		// Another worker is processing this job
		p.logger.Info("Lock not acquired, another worker is processing", "job_id", jobIDStr)
		return
	}

	// Reload job to get latest state after acquiring lock
	j, err = p.storage.GetJob(p.ctx, jobID)
	if err != nil {
		p.logger.Error("Error reloading job from storage", "job_id", jobIDStr, "error", err)
		// Release lock on error
		if releaseErr := p.queue.ReleaseLock(p.ctx, jobIDStr); releaseErr != nil {
			p.logger.Error("Error releasing lock", "job_id", jobIDStr, "error", releaseErr)
		}
		return
	}

	// Double-check status after acquiring lock (race condition protection)
	if j.Status == job.StatusDone {
		p.logger.Info("Job already done after lock acquisition, skipping", "job_id", jobIDStr)
		// Release lock before returning
		if releaseErr := p.queue.ReleaseLock(p.ctx, jobIDStr); releaseErr != nil {
			p.logger.Error("Error releasing lock", "job_id", jobIDStr, "error", releaseErr)
		}
		return
	}

	// Atomically update status from Pending to Processing
	updated, err := p.storage.UpdateStatusAtomic(p.ctx, jobID, job.StatusPending, job.StatusProcessing)
	if err != nil {
		p.logger.Error("Error atomically updating job status to Processing", "job_id", jobIDStr, "error", err)
		// Release lock on error
		if releaseErr := p.queue.ReleaseLock(p.ctx, jobIDStr); releaseErr != nil {
			p.logger.Error("Error releasing lock", "job_id", jobIDStr, "error", releaseErr)
		}
		return
	}
	if !updated {
		// Status didn't match (another worker might have processed it)
		p.logger.Info("Job status update failed, status mismatch", "job_id", jobIDStr)
		// Release lock before returning
		if releaseErr := p.queue.ReleaseLock(p.ctx, jobIDStr); releaseErr != nil {
			p.logger.Error("Error releasing lock", "job_id", jobIDStr, "error", releaseErr)
		}
		return
	}

	// Ensure lock is released after processing (success or failure)
	defer func() {
		if releaseErr := p.queue.ReleaseLock(p.ctx, jobIDStr); releaseErr != nil {
			p.logger.Error("Error releasing lock", "job_id", jobIDStr, "error", releaseErr)
		}
	}()

	p.logger.Info("Processing job", "job_id", jobIDStr, "type", j.Type)

	// Process the job using handler
	err = p.handler.ProcessJob(p.ctx, j)
	if err != nil {
		p.logger.Error("Job processing failed", "job_id", jobIDStr, "error", err)
		p.handleJobError(jobIDStr, err)
		return
	}

	// Job completed successfully - atomically update from Processing to Done
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
		// Calculate exponential backoff delay: base * 2^retry_count
		// Base delay: 1 second
		baseDelay := 1 * time.Second
		backoffDelay := baseDelay * time.Duration(1<<uint(j.RetryCount))
		
		// Cap maximum delay at 5 minutes to prevent excessive delays
		maxDelay := 5 * time.Minute
		if backoffDelay > maxDelay {
			backoffDelay = maxDelay
		}

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

