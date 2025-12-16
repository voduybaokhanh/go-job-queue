package worker

import (
	"context"
	"fmt"

	"github.com/voduybaokhanh/go-job-queue/internal/job"
)

// DefaultHandler is a simple job handler that simulates job processing
type DefaultHandler struct{}

// NewDefaultHandler creates a new DefaultHandler
func NewDefaultHandler() JobHandler {
	return &DefaultHandler{}
}

// ProcessJob processes a job - this is a simple implementation that always succeeds
// In a real application, this would contain the actual business logic
func (h *DefaultHandler) ProcessJob(ctx context.Context, j *job.Job) error {
	// Simulate job processing based on job type
	// In a real implementation, you would have different handlers for different job types
	
	// For demonstration, we can simulate failures for testing retry logic
	// Uncomment the following to test retry logic:
	// if j.RetryCount < 1 {
	//     return fmt.Errorf("simulated failure for testing retry")
	// }

	// Default: success
	return nil
}

// ErrorHandler is a handler that always returns an error (for testing)
type ErrorHandler struct{}

// NewErrorHandler creates a new ErrorHandler
func NewErrorHandler() JobHandler {
	return &ErrorHandler{}
}

// ProcessJob always returns an error (for testing retry logic)
func (h *ErrorHandler) ProcessJob(ctx context.Context, j *job.Job) error {
	return fmt.Errorf("simulated processing error")
}

