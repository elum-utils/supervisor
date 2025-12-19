// Package supervisor implements a supervisor for managing the lifecycle
// of multiple goroutines (services) with support for restart, graceful shutdown,
// and OS signal handling.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ServiceFunc is the type of function that represents a service.
// The function will be run in a separate goroutine and should exit
// when the provided context is canceled.
type ServiceFunc func(ctx context.Context) error

// Predefined errors that the supervisor may return.
var (
	ErrAlreadyExists = errors.New("service already exists") // Service with this name already exists
	ErrNotFound      = errors.New("service not found")      // Service not found
	ErrNotRunning    = errors.New("service not running")    // Service is not running
	ErrStopped       = errors.New("supervisor stopped")     // Supervisor is stopped
	ErrServicePanic  = errors.New("service panic")          // Service terminated with panic
)

// cmdAdd - command to add a new service
type cmdAdd struct {
	name string      // Service name
	fn   ServiceFunc // Service function
	resp chan error  // Response channel
}

// cmdStop - command to stop a service
type cmdStop struct {
	name string     // Service name
	resp chan error // Response channel
}

// cmdRestart - command to restart a service
type cmdRestart struct {
	name string     // Service name
	resp chan error // Response channel
}

// cmdStopAll - command to stop all services
type cmdStopAll struct {
	resp chan error // Response channel
}

// serviceInfo contains information about a running service.
type serviceInfo struct {
	name   string             // Service name
	fn     ServiceFunc        // Service function
	ctx    context.Context    // Service context
	cancel context.CancelFunc // Function to cancel service context
}

// Supervisor is the main supervisor structure.
// Manages the lifecycle of all services.
type Supervisor struct {
	options Options // Supervisor configuration

	ctx    context.Context    // Main supervisor context
	cancel context.CancelFunc // Function to cancel main context

	cmdCh chan any       // Channel for receiving control commands
	wg    sync.WaitGroup // WaitGroup for tracking all running services

	activeServices atomic.Int32 // Counter of active services (for atomic operations)
}

// Deadline returns the context's deadline (if set)
func (s *Supervisor) Deadline() (time.Time, bool) { return s.ctx.Deadline() }

// Done returns a channel that closes when the context is canceled
func (s *Supervisor) Done() <-chan struct{} { return s.ctx.Done() }

// Err returns the context error
func (s *Supervisor) Err() error { return s.ctx.Err() }

// New creates a new supervisor instance.
// ctx - parent context for managing supervisor lifecycle
// opts - optional supervisor configuration
func New(ctx context.Context, opts ...Options) *Supervisor {
	// Apply default options if no others provided
	opt := defaultOptions(opts...)
	// Create a child context with cancellation capability
	ctx, cancel := context.WithCancel(ctx)

	// Create supervisor
	s := &Supervisor{
		options: opt,
		ctx:     ctx,
		cancel:  cancel,
		cmdCh:   make(chan any, 64), // Buffered channel for commands
	}

	// Start the main command processing loop
	go s.loop()
	// Start OS signal handling
	go s.handleSignals()

	return s
}

// handleSignals handles operating system signals.
// SIGINT (Ctrl+C) and SIGTERM trigger graceful shutdown.
func (s *Supervisor) handleSignals() {
	// Create channel for receiving signals
	ch := make(chan os.Signal, 1)
	// Register signal handling
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	// Ensure signal handling stops when function exits
	defer signal.Stop(ch)

	// Wait for either OS signal or context cancellation
	select {
	case sig := <-ch:
		// Received termination signal
		s.options.Logger.Printf("Received signal: %s. Shutting down.", sig)
		s.cancel() // Initiate graceful shutdown
	case <-s.ctx.Done():
		// Supervisor context was canceled in another way
	}
}

// Add adds a new service to the supervisor.
// name - unique service name
// fn - service function
func (s *Supervisor) Add(name string, fn ServiceFunc) error {
	// Check if supervisor is already stopped
	if s.ctx.Err() != nil {
		return ErrStopped
	}
	// Create response channel
	resp := make(chan error, 1)
	// Send add command to main loop
	s.cmdCh <- cmdAdd{name: name, fn: fn, resp: resp}
	// Wait for response
	return <-resp
}

// Stop stops a service by name.
// name - service name to stop
func (s *Supervisor) Stop(name string) error {
	if s.ctx.Err() != nil {
		return ErrStopped
	}
	resp := make(chan error, 1)
	s.cmdCh <- cmdStop{name: name, resp: resp}
	return <-resp
}

// Restart restarts a service by name.
// name - service name to restart
func (s *Supervisor) Restart(name string) error {
	if s.ctx.Err() != nil {
		return ErrStopped
	}
	resp := make(chan error, 1)
	s.cmdCh <- cmdRestart{name: name, resp: resp}
	return <-resp
}

// StopAll stops all services and terminates the supervisor.
func (s *Supervisor) StopAll() error {
	if s.ctx.Err() != nil {
		return ErrStopped
	}
	resp := make(chan error, 1)
	s.cmdCh <- cmdStopAll{resp: resp}
	return <-resp
}

// Run starts the supervisor and blocks until all services complete.
// This method should be called after adding all services.
func (s *Supervisor) Run() {
	// Check if supervisor is already stopped
	if s.ctx.Err() != nil {
		return
	}

	// If no active services, exit immediately
	if s.activeServices.Load() == 0 {
		s.options.Logger.Printf("All services completed.")
		return
	}

	// Wait for context cancellation (stop signal)
	<-s.ctx.Done()

	// Start asynchronous waiting for all services to complete
	done := make(chan struct{})
	go func() {
		s.wg.Wait() // Wait for all goroutines to complete
		close(done)
	}()

	// Wait for either all services to complete or timeout expiration
	select {
	case <-done:
		// All services terminated gracefully
		s.options.Logger.Printf("All services stopped gracefully.")
	case <-time.After(s.options.ShutdownTimeout):
		// Graceful shutdown timeout reached
		s.options.Logger.Printf(
			"Shutdown timeout reached (%s), forcing exit.",
			s.options.ShutdownTimeout,
		)
	}
}

// All service operations are performed in this goroutine for safety.
func (s *Supervisor) loop() {
	// Registry of all registered services (running and stopped)
	services := map[string]ServiceFunc{}
	// Map of running services
	running := map[string]*serviceInfo{}

	// Infinite command processing loop
	for {
		select {
		case <-s.ctx.Done():
			// Supervisor context canceled - stop all services
			for _, info := range running {
				info.cancel()
			}
			return // Exit main loop

		case cmd := <-s.cmdCh:
			// Received command - process it
			switch c := cmd.(type) {

			case cmdAdd:
				// Add service command
				if _, ok := services[c.name]; ok {
					// Service with this name already exists
					c.resp <- ErrAlreadyExists
					continue
				}
				// Register service
				services[c.name] = c.fn
				// If supervisor is active, start service immediately
				if s.ctx.Err() == nil {
					s.startService(c.name, c.fn, running)
				}
				c.resp <- nil // Success

			case cmdStop:
				// Stop service command
				info, ok := running[c.name]
				if !ok {
					// Service is not running
					c.resp <- ErrNotRunning
					continue
				}
				// Cancel service context
				info.cancel()
				// Remove from running services map
				delete(running, c.name)
				c.resp <- nil

			case cmdRestart:
				// Restart service command
				fn, ok := services[c.name]
				if !ok {
					// Service not registered
					c.resp <- ErrNotFound
					continue
				}
				// If service is running - stop it first
				if info, ok := running[c.name]; ok {
					info.cancel()
					delete(running, c.name)
				}
				// Start service again
				s.startService(c.name, fn, running)
				c.resp <- nil

			case cmdStopAll:
				// Stop all services command
				for _, info := range running {
					info.cancel()
				}
				// Clear running services map
				running = map[string]*serviceInfo{}
				// Cancel supervisor context
				s.cancel()
				c.resp <- nil
			}
		}
	}
}

// startService starts a service in a separate goroutine.
// name - service name
// fn - service function
// running - map of running services (modified in place)
func (s *Supervisor) startService(
	name string,
	fn ServiceFunc,
	running map[string]*serviceInfo,
) {
	// Create child context for service
	ctx, cancel := context.WithCancel(s.ctx)
	// Create service information
	info := &serviceInfo{name: name, fn: fn, ctx: ctx, cancel: cancel}
	// Add to running services map
	running[name] = info

	// Increment counters
	s.activeServices.Add(1)
	s.wg.Add(1)

	// Start service in separate goroutine
	go s.runService(info)
}

// runService manages the lifecycle of a single service.
// Handles restarts on errors and panics.
func (s *Supervisor) runService(info *serviceInfo) {
	// Ensure counters are decremented when function exits
	defer func() {
		s.activeServices.Add(-1)
		s.wg.Done()
	}()

	// Restart history for rate limiting restarts
	var restarts []time.Time

	// Infinite service loop (with restarts)
	for {
		// Run single service execution iteration
		err := s.runOnce(info)
		if err == nil {
			// Service exited without error - exit
			s.options.Logger.Printf("Service %s exited normally", info.name)
			return
		}

		// Check if service can be restarted further
		if !s.canRestart(&restarts) {
			// Restart limit exceeded
			s.options.Logger.Printf(
				"Service %s exceeded restart limit (%d within %s). Stopping all services.",
				info.name,
				s.options.RestartLimit,
				s.options.RestartInterval,
			)
			// Stop all services
			s.cancel()
			return
		}

		// Wait before restarting
		select {
		case <-info.ctx.Done():
			// Service context canceled - exit
			return
		case <-time.After(s.options.RestartDelay):
			// Restart delay expired - continue loop
		}
	}
}

// runOnce executes a single service iteration with panic recovery.
func (s *Supervisor) runOnce(info *serviceInfo) (err error) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			// Convert panic to error
			err = fmt.Errorf("%w: %v", ErrServicePanic, r)
			s.options.Logger.Printf("Panic in service %s: %v", info.name, r)
		}
	}()

	// Execute service function
	err = info.fn(info.ctx)
	if err != nil {
		// Service exited with error (but not panic)
		s.options.Logger.Printf("Service %s exited with error: %v", info.name, err)
	}
	return err
}

// canRestart checks if a service can be restarted considering rate limits.
// times - pointer to slice of restart times (modified in place)
func (s *Supervisor) canRestart(times *[]time.Time) bool {
	now := time.Now()
	// Add current restart time
	*times = append(*times, now)

	// Filter restart times, keeping only those within the configured interval
	valid := (*times)[:0] // Use memory reuse trick
	for _, t := range *times {
		if now.Sub(t) <= s.options.RestartInterval {
			valid = append(valid, t)
		}
	}
	*times = valid
	// Check if restart limit is not exceeded
	return len(valid) <= s.options.RestartLimit
}
