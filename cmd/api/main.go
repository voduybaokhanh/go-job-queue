package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/voduybaokhanh/go-job-queue/internal/api"
	"github.com/voduybaokhanh/go-job-queue/internal/queue"
	"github.com/voduybaokhanh/go-job-queue/internal/storage"
)

const (
	// Default HTTP server address
	defaultAddr = ":8080"
	// Database connection timeout
	dbConnectTimeout = 10 * time.Second
	// Graceful shutdown timeout
	shutdownTimeout = 30 * time.Second
)

func main() {
	logger := setupLogger()
	logger.Info("Starting API server")

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

	// Create storage and queue
	storage := storage.NewPostgresStorage(pgPool)
	queue := queue.NewRedisQueue(redisClient)

	// Create API handler
	handler := api.NewHandler(storage, queue, logger)

	// Setup router
	router := api.NewRouter(handler)

	// Get server address from environment
	addr := os.Getenv("API_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Info("API server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("Received shutdown signal", "signal", sig.String())

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Shutdown server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	} else {
		logger.Info("Server shutdown complete")
	}
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

