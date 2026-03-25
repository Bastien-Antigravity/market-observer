package helpers

import (
	"fmt"
	"strings"
	"time"

	"market-observer/src/interfaces"
)

// -----------------------------------------------------------------------------
// Custom Error Types
// -----------------------------------------------------------------------------

type MarketObserverError struct {
	Message string
	Cause   error
}

func (e *MarketObserverError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *MarketObserverError) Unwrap() error {
	return e.Cause
}

// Helper to define distinct error types for type assertions if needed
type (
	ConfigurationError struct{ MarketObserverError }
	NetworkError       struct{ MarketObserverError }
	DataSourceError    struct{ MarketObserverError }
	DatabaseError      struct{ MarketObserverError }
	ValidationError    struct{ MarketObserverError }
)

// -----------------------------------------------------------------------------
// Retry Logic
// -----------------------------------------------------------------------------

// RetryWithBackoff attempts to execute the operation up to maxRetries times with exponential backoff.
func RetryWithBackoff(operation string, maxRetries int, baseDelay time.Duration, fn func() (interface{}, error)) (interface{}, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		res, err := fn()
		if err == nil {
			return res, nil
		}

		lastErr = err
		if attempt == maxRetries-1 {
			break
		}

		delay := baseDelay * (1 << attempt)
		fmt.Printf("Warning: Attempt %d/%d failed for %s: %v. Retrying in %v\n", attempt+1, maxRetries, operation, err, delay)
		time.Sleep(delay)
	}

	return nil, lastErr
}

// -----------------------------------------------------------------------------
// Error Handler
// -----------------------------------------------------------------------------

type ErrorHandler struct {
	Logger                 interfaces.Logger
	ErrorCount             int
	MaxErrorsBeforeRestart int
}

func NewErrorHandler(l interfaces.Logger) *ErrorHandler {
	return &ErrorHandler{
		Logger:                 l,
		ErrorCount:             0,
		MaxErrorsBeforeRestart: 10,
	}
}

// -----------------------------------------------------------------------------

func (e *ErrorHandler) ResetErrorCount() {
	e.ErrorCount = 0
}

// -----------------------------------------------------------------------------

// ExecuteWithRetry encapsulates logic to execute a function, retry on failure, and categorize errors.
func (e *ErrorHandler) ExecuteWithRetry(operation string, fn func() (interface{}, error), maxRetries int) (interface{}, error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		res, err := fn()
		if err == nil {
			// Success: Recover stats
			if e.ErrorCount > 0 {
				e.ErrorCount--
			}
			return res, nil
		}

		// Handle Error
		if attempt == maxRetries-1 {
			e.ErrorCount++
			e.Logger.Error(fmt.Sprintf("%s failed (attempt %d/%d): %v", operation, attempt+1, maxRetries, err))

			// Wrap into specific error types based on context if simpler heuristics apply
			lowerOp := strings.ToLower(operation)
			if strings.Contains(lowerOp, "network") || strings.Contains(lowerOp, "fetch") {
				return nil, &NetworkError{MarketObserverError{Message: fmt.Sprintf("%s failed", operation), Cause: err}}
			} else if strings.Contains(lowerOp, "database") || strings.Contains(lowerOp, "save") {
				return nil, &DatabaseError{MarketObserverError{Message: fmt.Sprintf("%s failed", operation), Cause: err}}
			} else {
				return nil, &MarketObserverError{Message: fmt.Sprintf("%s failed", operation), Cause: err}
			}
		}

		// Log warning and wait
		e.Logger.Warning(fmt.Sprintf("%s failed (attempt %d/%d): %v", operation, attempt+1, maxRetries, err))
		delay := time.Duration(1<<attempt) * time.Second
		time.Sleep(delay)
	}

	return nil, &MarketObserverError{Message: fmt.Sprintf("%s failed after %d attempts", operation, maxRetries)}
}

// -----------------------------------------------------------------------------

func (e *ErrorHandler) Handle(err error, context string) {
	if err != nil {
		e.Logger.Error(fmt.Sprintf("Error in %s: %v", context, err))
	}
}

func (e *ErrorHandler) HandleFatal(err error, context string) {
	if err != nil {
		e.Logger.Critical(fmt.Sprintf("FATAL in %s: %v", context, err))
		// Note: The library's Critical might not call os.Exit, but the intention is fatal.
	}
}
