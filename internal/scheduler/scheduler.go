package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/voduybaokhanh/go-job-queue/internal/job"
	"github.com/voduybaokhanh/go-job-queue/internal/queue"
)

// Scheduler handles enqueuing scheduled jobs
type Scheduler struct {
	pool      *pgxpool.Pool
	queue     queue.Queue
	logger    *slog.Logger
	interval  time.Duration
	ctx       context.Context
	cancel    context.CancelFunc
}

// Config holds configuration for the scheduler
type Config struct {
	Pool     *pgxpool.Pool
	Queue    queue.Queue
	Logger   *slog.Logger
	Interval time.Duration // How often to check for scheduled jobs
}

// NewScheduler creates a new scheduler instance
func NewScheduler(cfg Config) *Scheduler {
	if cfg.Interval == 0 {
		cfg.Interval = 10 * time.Second // Default interval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		pool:     cfg.Pool,
		queue:    cfg.Queue,
		logger:   cfg.Logger,
		interval: cfg.Interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start starts the scheduler goroutine
func (s *Scheduler) Start() {
	s.logger.Info("Starting scheduler", "interval", s.interval.String())
	go s.run()
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.logger.Info("Stopping scheduler")
	s.cancel()
}

// run is the main scheduler loop
func (s *Scheduler) run() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Run immediately on start
	s.processScheduledJobs()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Scheduler stopped")
			return
		case <-ticker.C:
			s.processScheduledJobs()
		}
	}
}

// processScheduledJobs queries for scheduled jobs that are ready to be enqueued
func (s *Scheduler) processScheduledJobs() {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	// Query for jobs that are Pending, have a scheduled_at time, and the time has passed
	// Use FOR UPDATE SKIP LOCKED to prevent multiple schedulers from processing the same job
	query := `
		SELECT id, type, payload, status, retry_count, max_retries, scheduled_at, idempotency_key, created_at, updated_at
		FROM jobs
		WHERE status = $1
		  AND scheduled_at IS NOT NULL
		  AND scheduled_at <= NOW()
		ORDER BY scheduled_at ASC
		LIMIT 100
		FOR UPDATE SKIP LOCKED`

	rows, err := s.pool.Query(ctx, query, job.StatusPending)
	if err != nil {
		s.logger.Error("Error querying scheduled jobs", "error", err)
		return
	}
	defer rows.Close()

	var enqueuedCount int
	for rows.Next() {
		var j job.Job
		var scheduledAt *time.Time

		err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &j.Status, &j.RetryCount, &j.MaxRetries,
			&scheduledAt, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			s.logger.Error("Error scanning scheduled job", "error", err)
			continue
		}

		j.ScheduledAt = scheduledAt

		// Enqueue the job
		if err := s.queue.Enqueue(ctx, j.ID.String()); err != nil {
			s.logger.Error("Error enqueuing scheduled job",
				"job_id", j.ID.String(),
				"error", err)
			continue
		}

		enqueuedCount++
		s.logger.Info("Scheduled job enqueued",
			"job_id", j.ID.String(),
			"type", j.Type,
			"scheduled_at", scheduledAt)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating scheduled jobs", "error", err)
		return
	}

	if enqueuedCount > 0 {
		s.logger.Info("Processed scheduled jobs", "count", enqueuedCount)
	}
}

