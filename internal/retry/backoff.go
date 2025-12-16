package retry

import "time"

// CalculateBackoff calculates exponential backoff delay based on retry count.
// This is a pure function with no side effects - it only performs calculations.
//
// Formula: baseDelay * 2^retryCount
// The result is capped at maxDelay to prevent excessive delays.
//
// Parameters:
//   - retryCount: Current retry attempt (0-indexed)
//   - baseDelay: Base delay duration (e.g., 1 second)
//   - maxDelay: Maximum delay duration to cap the result
//
// Returns:
//   - Calculated backoff delay, capped at maxDelay
func CalculateBackoff(retryCount int, baseDelay time.Duration, maxDelay time.Duration) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}

	// Calculate exponential backoff: baseDelay * 2^retryCount
	// Using bit shift for efficiency: 1 << retryCount = 2^retryCount
	multiplier := time.Duration(1 << uint(retryCount))
	backoffDelay := baseDelay * multiplier

	// Cap at maximum delay
	if backoffDelay > maxDelay {
		return maxDelay
	}

	return backoffDelay
}

