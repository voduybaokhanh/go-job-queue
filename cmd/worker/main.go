package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/voduybaokhanh/go-job-queue/internal/queue"
	"github.com/voduybaokhanh/go-job-queue/internal/scheduler"
	"github.com/voduybaokhanh/go-job-queue/internal/storage"
	"github.com/voduybaokhanh/go-job-queue/internal/worker"
)

const (
	// Default number of workers
	defaultNumWorkers = 5
	// Database connection timeout
	dbConnectTimeout = 10 * time.Second
	// Graceful shutdown timeout
	shutdownTimeout = 30 * time.Second
)

func main() {
	logger := setupLogger()
	logger.Info("Starting worker")

	// Initialize PostgreSQL connection
	pgPool, err := initPostgreSQL(logger)
	if err != nil {
		logger.Error("Failed to initialize PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	logger.Info("PostgreSQL connection established")

	// Initialize Redis connection
	redisClient, err := initRedis(logger)
	if err != nil {
		logger.Error("Failed to initialize Redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()
	logger.Info("Redis connection established")

	// Create storage
	storage := storage.NewPostgresStorage(pgPool)

	// Create queue
	queue := queue.NewRedisQueue(redisClient)

	// Create job handler (using default handler - can be replaced with custom handlers)
	handler := worker.NewDefaultHandler()

	// Create worker pool
	pool := worker.NewPool(worker.Config{
		NumWorkers: defaultNumWorkers,
		Queue:      queue,
		Storage:    storage,
		Handler:    handler,
		LockTTL:    5 * time.Minute,
		Logger:     logger,
	})

	// Create and start scheduler for scheduled jobs
	sched := scheduler.NewScheduler(scheduler.Config{
		Pool:     pgPool,
		Queue:    queue,
		Logger:   logger,
		Interval: 10 * time.Second,
	})
	sched.Start()
	logger.Info("Scheduler started")

	// Start worker pool
	pool.Start()
	logger.Info("Worker pool started")

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("Received shutdown signal", "signal", sig.String())

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Shutdown scheduler and worker pool
	shutdownWg := sync.WaitGroup{}
	
	// Stop scheduler
	shutdownWg.Add(1)
	go func() {
		defer shutdownWg.Done()
		sched.Stop()
		logger.Info("Scheduler stopped")
	}()

	// Stop worker pool
	shutdownWg.Add(1)
	go func() {
		defer shutdownWg.Done()
		pool.Stop()
		logger.Info("Worker pool stopped")
	}()

	// Wait for worker pool to stop or timeout
	done := make(chan struct{})
	go func() {
		shutdownWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All workers finished processing")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout reached, forcing exit")
	}

	// Close database connections
	logger.Info("Closing database connections")
	pgPool.Close()
	redisClient.Close()

	logger.Info("Worker shutdown complete")
}

// setupLogger creates a structured JSON logger
func setupLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return slog.New(handler)
}

// initPostgreSQL initializes PostgreSQL connection pool
func initPostgreSQL(logger *slog.Logger) (*pgxpool.Pool, error) {
	// Get database URL from environment or use default
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://user:password@localhost:5432/jobqueue?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), dbConnectTimeout)
	defer cancel()

	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Configure connection pool
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

// initRedis initializes Redis connection
func initRedis(logger *slog.Logger) (*redis.Client, error) {
	// Get Redis address from environment or use default
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisPassword == "" {
		redisPassword = "" // No password by default
	}

	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       0,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	return client, nil
}

