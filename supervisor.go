package supervisor

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ServiceFunc defines the signature for service functions that can be managed by the Supervisor.
// The function should respect the context cancellation and return an error if it stops unexpectedly.
type ServiceFunc func(ctx context.Context) error

// Supervisor manages the lifecycle of multiple services with fault tolerance.
// It provides automatic restart with configurable limits, graceful shutdown,
// and centralized error handling for all registered services.
type Supervisor struct {
	options  Options
	services map[string]ServiceFunc
	wg       sync.WaitGroup
	mu       sync.RWMutex
	context  context.Context
	cancel   context.CancelFunc
	running  bool
}

// --- Implementation of context.Context interface ---

// Deadline returns the time when the supervisor context will be canceled, if any.
func (s *Supervisor) Deadline() (time.Time, bool) { return s.context.Deadline() }

// Done returns a channel that's closed when the supervisor context is canceled.
func (s *Supervisor) Done() <-chan struct{} { return s.context.Done() }

// Err returns the error that caused the supervisor context to be canceled.
func (s *Supervisor) Err() error { return s.context.Err() }

// --- Constructor ---

// New creates a new Supervisor instance with the provided context and options.
// It automatically sets up signal handling for SIGINT and SIGTERM to initiate graceful shutdown.
//
// Parameters:
//   - ctx: The parent context for the supervisor and all managed services
//   - opts: Optional configuration options for the supervisor behavior
//
// Returns:
//   - *Supervisor: A new supervisor instance ready to manage services
func New(ctx context.Context, opts ...Options) *Supervisor {
	opt := defaultOptions(opts...)
	ctx, cancel := context.WithCancel(ctx)

	s := &Supervisor{
		options:  opt,
		services: make(map[string]ServiceFunc),
		context:  ctx,
		cancel:   cancel,
	}

	// Handle OS signals for graceful shutdown
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case sig := <-ch:
			s.options.Logger.Printf("Received signal: %s. Shutting down.", sig)
		case <-ctx.Done():
		}
		cancel()
	}()

	return s
}

// --- Service Registration ---

// Add registers a new service with the supervisor.
// If the supervisor is already running, the service will be started immediately.
//
// Parameters:
//   - name: Unique identifier for the service
//   - fn: Service function that will be managed by the supervisor
//
// Panics:
//   - If a service with the same name is already registered
func (s *Supervisor) Add(name string, fn ServiceFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.services[name]; exists {
		panic("service already registered: " + name)
	}

	s.services[name] = fn

	// If supervisor is already running, start the service immediately
	if s.running {
		s.wg.Add(1)
		go s.runService(s.context, name, fn)
	}
}

// --- Service Execution ---

// Run starts all registered services and blocks until all services have stopped.
// It provides graceful shutdown handling with configurable timeout and monitors
// service restarts to prevent infinite restart loops.
func (s *Supervisor) Run() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true

	services := make(map[string]ServiceFunc, len(s.services))
	for name, fn := range s.services {
		services[name] = fn
	}
	s.mu.Unlock()

	// Start all registered services
	for name, fn := range services {
		s.wg.Add(1)
		go s.runService(s.context, name, fn)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-s.context.Done():
		s.options.Logger.Printf("Supervisor received shutdown signal, waiting for services to stop...")

		select {
		case <-done:
			s.options.Logger.Printf("All services stopped gracefully.")
		case <-time.After(s.options.ShutdownTimeout):
			s.options.Logger.Printf("Shutdown timeout reached (%s), forcing exit.", s.options.ShutdownTimeout)
		}

	case <-done:
		s.options.Logger.Printf("All services completed.")
	}
}

// runService manages the execution of a single service with restart logic and panic recovery.
// It tracks restart attempts within the configured time window and stops all services
// if the restart limit is exceeded.
//
// Parameters:
//   - ctx: Context for service execution and cancellation
//   - name: Service name for logging and identification
//   - fn: Service function to execute
func (s *Supervisor) runService(ctx context.Context, name string, fn ServiceFunc) {
	defer s.wg.Done()

	restartTimestamps := make([]time.Time, 0)

	for {
		select {
		case <-ctx.Done():
			s.options.Logger.Printf("Service %s stopped (context canceled)", name)
			return
		default:
			func() {
				defer func() {
					if r := recover(); r != nil {
						s.options.Logger.Printf("Panic in service %s: %v", name, r)
					}
				}()

				err := fn(ctx)
				if err != nil {
					s.options.Logger.Printf("Service %s exited with error: %v", name, err)
				} else {
					s.options.Logger.Printf("Service %s exited normally", name)
					return
				}
			}()

			now := time.Now()
			restartTimestamps = append(restartTimestamps, now)

			// Filter restart timestamps to only include those within the restart interval
			valid := make([]time.Time, 0, len(restartTimestamps))
			for _, t := range restartTimestamps {
				if now.Sub(t) <= s.options.RestartInterval {
					valid = append(valid, t)
				}
			}
			restartTimestamps = valid

			// Check if restart limit has been exceeded
			if len(restartTimestamps) > s.options.RestartLimit {
				s.options.Logger.Printf("Service %s exceeded restart limit (%d within %s). Stopping all services.",
					name, s.options.RestartLimit, s.options.RestartInterval)
				s.cancel()
				return
			}

			// Wait before restarting, unless context is canceled
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.options.RestartDelay):
			}
		}
	}
}
