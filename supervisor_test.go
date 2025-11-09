package supervisor

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// newTestLogger creates a buffer-backed logger for testing purposes.
// Returns both the buffer for assertions and the logger implementation.
func newTestLogger() (*bytes.Buffer, Logger) {
	buf := &bytes.Buffer{}
	return buf, &logWrapper{buf: buf}
}

// logWrapper is a thread-safe test logger that writes to a buffer with timestamps.
type logWrapper struct {
	buf *bytes.Buffer
	mu  sync.Mutex
}

func (l *logWrapper) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.WriteString(time.Now().Format("15:04:05") + " " + format + "\n")
}

// TestSupervisor_RunAndStop verifies that services start properly and stop gracefully.
func TestSupervisor_RunAndStop(t *testing.T) {
	buf, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{Logger: logger})

	var counter int32

	// Add a worker service that increments counter until context is canceled
	s.Add("worker", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				atomic.AddInt32(&counter, 1)
				time.Sleep(50 * time.Millisecond)
			}
		}
	})

	// Cancel supervisor after 300ms to trigger graceful shutdown
	go func() {
		time.Sleep(300 * time.Millisecond)
		s.cancel()
	}()

	s.Run()

	// Verify service was actually running
	if atomic.LoadInt32(&counter) == 0 {
		t.Error("service was not started")
	}

	// Verify logging occurred
	bufLen := 0
	func() {
		// Use mutex to safely read buffer length if needed
		if lw, ok := logger.(*logWrapper); ok {
			lw.mu.Lock()
			defer lw.mu.Unlock()
			bufLen = lw.buf.Len()
		} else {
			bufLen = buf.Len()
		}
	}()

	if bufLen == 0 {
		t.Error("logger was not called")
	}
}

// TestSupervisor_StopBySignal tests graceful shutdown when OS signals are received.
func TestSupervisor_StopBySignal(t *testing.T) {
	_, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{Logger: logger})

	var stopped int32
	s.Add("worker", func(ctx context.Context) error {
		<-ctx.Done()
		atomic.StoreInt32(&stopped, 1)
		return nil
	})

	// Send SIGINT signal after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		p, _ := os.FindProcess(os.Getpid())
		p.Signal(syscall.SIGINT)
	}()

	s.Run()

	if atomic.LoadInt32(&stopped) != 1 {
		t.Error("service did not stop after receiving signal")
	}
}

// TestSupervisor_PanicRestart verifies that services restart after panicking.
func TestSupervisor_PanicRestart(t *testing.T) {
	_, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{
		Logger:          logger,
		RestartLimit:    3,
		RestartInterval: time.Second,
		RestartDelay:    10 * time.Millisecond,
	})

	var runs int32
	s.Add("panicService", func(ctx context.Context) error {
		atomic.AddInt32(&runs, 1)
		panic("test panic")
	})

	// Stop the test after allowing multiple restart attempts
	go func() {
		time.Sleep(200 * time.Millisecond)
		s.cancel()
	}()

	s.Run()

	// Verify service was restarted after panic
	if atomic.LoadInt32(&runs) < 2 {
		t.Errorf("expected at least 2 runs, got %d", runs)
	}
}

// TestSupervisor_ErrorRestart verifies that services restart after returning errors.
func TestSupervisor_ErrorRestart(t *testing.T) {
	_, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{
		Logger:          logger,
		RestartLimit:    3,
		RestartInterval: time.Second,
		RestartDelay:    10 * time.Millisecond,
	})

	var runs int32
	s.Add("errorService", func(ctx context.Context) error {
		atomic.AddInt32(&runs, 1)
		return context.Canceled
	})

	go func() {
		time.Sleep(200 * time.Millisecond)
		s.cancel()
	}()

	s.Run()

	// Verify service was restarted after error
	if atomic.LoadInt32(&runs) < 2 {
		t.Errorf("expected at least 2 runs, got %d", runs)
	}
}

// TestSupervisor_RestartLimitExceeded tests the restart limit enforcement.
func TestSupervisor_RestartLimitExceeded(t *testing.T) {
	_, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{
		Logger:          logger,
		RestartLimit:    2,
		RestartInterval: time.Second,
		RestartDelay:    10 * time.Millisecond,
	})

	var runs int32
	s.Add("failService", func(ctx context.Context) error {
		atomic.AddInt32(&runs, 1)
		return context.Canceled
	})

	start := time.Now()
	s.Run()
	elapsed := time.Since(start)

	// Verify supervisor stops within reasonable time when restart limit is exceeded
	if elapsed > 3*time.Second {
		t.Error("supervisor did not stop when restart limit was exceeded")
	}
	// Verify multiple restart attempts were made before stopping
	if atomic.LoadInt32(&runs) <= 2 {
		t.Errorf("expected at least 3 restart attempts, got %d", runs)
	}
}

// TestSupervisor_AddDuplicate tests that duplicate service registration is prevented.
func TestSupervisor_AddDuplicate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when adding service with duplicate name")
		}
	}()

	ctx := context.Background()
	s := New(ctx)
	s.Add("dupService", func(ctx context.Context) error { return nil })
	s.Add("dupService", func(ctx context.Context) error { return nil })
}

// TestSupervisor_AddAfterRun tests dynamic service registration after supervisor has started.
func TestSupervisor_AddAfterRun(t *testing.T) {
	_, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{Logger: logger})

	started := make(chan struct{})
	s.Add("initial", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	})

	go func() {
		s.Run()
	}()

	// Wait for initial service to start, then add another service
	<-started
	s.Add("dynamic", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	// Allow time for dynamic service to start, then stop everything
	time.Sleep(100 * time.Millisecond)
	s.cancel()
}

// TestSupervisor_ShutdownTimeout tests the shutdown timeout behavior.
func TestSupervisor_ShutdownTimeout(t *testing.T) {
	_, logger := newTestLogger()
	ctx := context.Background()
	s := New(ctx, Options{
		Logger:          logger,
		ShutdownTimeout: 500 * time.Millisecond,
	})

	// Add a service that hangs during shutdown
	s.Add("hangService", func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(2 * time.Second) // Hang longer than shutdown timeout
		return nil
	})

	go func() {
		time.Sleep(200 * time.Millisecond)
		s.cancel()
	}()

	start := time.Now()
	s.Run()
	elapsed := time.Since(start)

	// Verify shutdown completes within timeout bounds (with some tolerance)
	if elapsed < 400*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("expected shutdown to complete with timeout, elapsed=%v", elapsed)
	}
}
