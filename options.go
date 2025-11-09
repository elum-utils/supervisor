package supervisor

import (
	"log"
	"os"
	"time"
)

// Logger defines the interface for logging within the supervisor.
// It provides a simple printf-style logging method that can be implemented
// by various logging frameworks or custom loggers.
type Logger interface {
	Printf(format string, v ...any)
}

// Options configures the behavior of the Supervisor.
// All fields are optional and will use sensible defaults if not specified.
type Options struct {
	// RestartLimit defines the maximum number of service restarts allowed
	// within the RestartInterval time window. Default: 5
	RestartLimit int

	// RestartInterval defines the time window for counting restart attempts.
	// Restart attempts older than this interval are not counted toward the limit.
	// Default: 1 minute
	RestartInterval time.Duration

	// RestartDelay defines the delay between service restart attempts.
	// This prevents immediate restart loops and allows time for cleanup.
	// Default: 100 milliseconds
	RestartDelay time.Duration

	// ShutdownTimeout defines the maximum time to wait for services to stop
	// gracefully before forcing termination. Default: 30 seconds
	ShutdownTimeout time.Duration

	// Logger specifies the logging implementation to use.
	// If nil, a default logger writing to stdout is used.
	Logger Logger
}

// defaultOptions returns a Options struct with sensible defaults.
// It merges any provided options with the default values, ensuring
// that zero values in the provided options don't override defaults.
//
// Parameters:
//   - opts: Optional user-provided options to override defaults
//
// Returns:
//   - Options: A fully populated options struct with defaults applied
func defaultOptions(opts ...Options) Options {
	// Initialize with default values
	options := Options{
		RestartLimit:    5,                      // Maximum 5 restarts per interval
		RestartInterval: time.Minute,            // Count restarts within 1 minute window
		RestartDelay:    100 * time.Millisecond, // Brief delay between restart attempts
		ShutdownTimeout: 30 * time.Second,       // Allow 30 seconds for graceful shutdown
		Logger: log.New(
			os.Stdout,
			"[Supervisor] ",
			log.LstdFlags, // Include date and time in logs
		),
	}

	// Apply user-provided options if any
	if len(opts) > 0 {
		opt := opts[0]

		// Only override defaults if user provided positive values
		if opt.RestartLimit > 0 {
			options.RestartLimit = opt.RestartLimit
		}
		if opt.RestartInterval > 0 {
			options.RestartInterval = opt.RestartInterval
		}
		if opt.RestartDelay > 0 {
			options.RestartDelay = opt.RestartDelay
		}
		if opt.ShutdownTimeout > 0 {
			options.ShutdownTimeout = opt.ShutdownTimeout
		}
		if opt.Logger != nil {
			options.Logger = opt.Logger
		}
	}

	return options
}
