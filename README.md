# Supervisor

**Supervisor** is a lightweight and robust service manager for Go.
It runs and monitors multiple services, providing:

* automatic restarts on errors or panics,
* restart rate limiting,
* graceful shutdown with timeout,
* OS signal handling (`SIGINT`, `SIGTERM`),
* dynamic service registration at runtime,
* individual service control (stop/restart).

---

## 🚀 Installation

```bash
go get github.com/elum-utils/supervisor
```

---

## ⚙️ Basic Usage Example

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/elum-utils/supervisor"
)

func main() {
	ctx := context.Background()
	s := supervisor.New(ctx, supervisor.Options{
		Logger:          log.Default(),
		RestartLimit:    3,
		RestartInterval: 5 * time.Second,
		RestartDelay:    1 * time.Second,
		ShutdownTimeout: 2 * time.Second,
	})

	s.Add("worker", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				log.Println("worker stopped")
				return nil
			default:
				log.Println("working...")
				time.Sleep(500 * time.Millisecond)
			}
		}
	})

	s.Run()
}
```

---

## 🧩 Dynamic Service Registration

You can add new services even after the supervisor has started:

```go
s.Add("initial", func(ctx context.Context) error {
	<-ctx.Done()
	return nil
})

go s.Run()

// Later in runtime:
err := s.Add("dynamic", func(ctx context.Context) error {
	<-ctx.Done()
	return nil
})
if err != nil {
	log.Printf("Failed to add dynamic service: %v", err)
}
```

---

## 🎛️ Individual Service Management

### Stop a Specific Service
```go
// Stop a single service by name
err := s.Stop("worker")
if err != nil {
	if errors.Is(err, supervisor.ErrNotRunning) {
		log.Println("Service was not running")
	} else if errors.Is(err, supervisor.ErrStopped) {
		log.Println("Supervisor is stopped")
	} else {
		log.Printf("Error stopping service: %v", err)
	}
}
```

### Restart a Specific Service
```go
// Restart a service (stops if running, then starts again)
err := s.Restart("api-server")
if err != nil {
	if errors.Is(err, supervisor.ErrNotFound) {
		log.Println("Service not found")
	} else if errors.Is(err, supervisor.ErrStopped) {
		log.Println("Supervisor is stopped")
	} else {
		log.Printf("Error restarting service: %v", err)
	}
}
```

### Stop All Services
```go
// Stop all services and the supervisor
err := s.StopAll()
if err != nil && !errors.Is(err, supervisor.ErrStopped) {
	log.Printf("Error stopping all services: %v", err)
}
```

## ⚠️ Error Handling

The supervisor returns specific errors for different failure cases:

| Error | Description |
|-------|-------------|
| `ErrAlreadyExists` | Service with the same name already exists |
| `ErrNotFound` | Service not found (for restart operation) |
| `ErrNotRunning` | Service is not currently running (for stop operation) |
| `ErrStopped` | Supervisor is already stopped |
| `ErrServicePanic` | Service panicked (recovered and logged) |

---

## 🔁 Automatic Restart Behavior

Services are automatically restarted when they:

* panic, or
* return a non-nil error.

**Note:** Services that return `nil` (normal completion) are NOT restarted.

Example of restarting service:

```go
s.Add("unstable", func(ctx context.Context) error {
	// This service will restart after panic
	panic("oops!")
})

s.Add("error-prone", func(ctx context.Context) error {
	// This service will restart after error
	return fmt.Errorf("something went wrong")
})

s.Add("stable", func(ctx context.Context) error {
	// This service completes normally and won't restart
	return nil
})
```

Supervisor catches panics and errors, logs them, and restarts the service according to `RestartLimit` and `RestartDelay`.

---

## 🕊️ Graceful Shutdown

Supervisor stops all services when:

* the parent context is canceled,
* a termination signal (`SIGINT`, `SIGTERM`) is received,
* restart limits are exceeded,
* `StopAll()` is called.

A configurable `ShutdownTimeout` ensures the supervisor doesn't hang forever.

---

## ⚙️ Configuration Options

| Field             | Type            | Default                  | Description                                             |
| ----------------- | --------------- | ------------------------ | ------------------------------------------------------- |
| `Logger`          | `Logger`        | `log.Default()`          | Any type implementing `Printf(format string, v ...any)` |
| `RestartLimit`    | `int`           | `5`                      | Maximum restart attempts before stopping all services   |
| `RestartInterval` | `time.Duration` | `1 * time.Minute`        | Time window for counting restarts                       |
| `RestartDelay`    | `time.Duration` | `1 * time.Second`        | Delay before restarting a failed service                |
| `ShutdownTimeout` | `time.Duration` | `30 * time.Second`       | Max wait time for graceful shutdown                     |

---

## 📋 Complete Example with Service Management

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/elum-utils/supervisor"
)

func main() {
	ctx := context.Background()
	s := supervisor.New(ctx, supervisor.Options{
		Logger:          log.Default(),
		RestartLimit:    3,
		RestartInterval: 10 * time.Second,
		RestartDelay:    2 * time.Second,
	})

	// Add multiple services
	err := s.Add("api", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				log.Println("API: stopped")
				return nil
			default:
				log.Println("API: processing request")
				time.Sleep(1 * time.Second)
			}
		}
	})
	if err != nil {
		log.Fatalf("Failed to add API service: %v", err)
	}

	err = s.Add("worker", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				log.Println("Worker: stopped")
				return nil
			default:
				log.Println("Worker: processing job")
				time.Sleep(2 * time.Second)
			}
		}
	})
	if err != nil {
		log.Fatalf("Failed to add worker service: %v", err)
	}

	// Start supervisor in background
	go s.Run()

	// Let services run for a bit
	time.Sleep(5 * time.Second)

	// Restart a specific service
	err = s.Restart("api")
	if err != nil {
		if errors.Is(err, supervisor.ErrStopped) {
			log.Println("Supervisor is already stopped")
		} else {
			log.Printf("Failed to restart API: %v", err)
		}
	} else {
		log.Println("API service restarted")
	}

	// Wait and let it run
	time.Sleep(3 * time.Second)

	// Stop a specific service
	err = s.Stop("worker")
	if err != nil {
		if errors.Is(err, supervisor.ErrNotRunning) {
			log.Println("Worker service was not running")
		} else if errors.Is(err, supervisor.ErrStopped) {
			log.Println("Supervisor is stopped")
		} else {
			log.Printf("Failed to stop worker: %v", err)
		}
	} else {
		log.Println("Worker service stopped")
	}

	// Run for a bit longer, then stop everything
	time.Sleep(5 * time.Second)
	err = s.StopAll()
	if err != nil && !errors.Is(err, supervisor.ErrStopped) {
		log.Printf("Error stopping all services: %v", err)
	}
	log.Println("All services stopped")
}
```

---

## 🧬 ServiceFunc Interface

All services must implement the `ServiceFunc` interface:

```go
type ServiceFunc func(ctx context.Context) error
```

* The function receives a context that will be canceled when the service should stop.
* Return `nil` for normal completion (service won't restart).
* Return an error or panic for abnormal termination (service will restart).

---

## 🔒 Thread Safety

All public methods of `Supervisor` are thread-safe and can be called from multiple goroutines.

---

## 🧪 Testing

Fully covered by unit tests:

```bash
go test -race -v ./...
```

Tests verify:

* Service startup and shutdown
* Signal-based termination
* Panic and error recovery
* Restart limit enforcement
* Dynamic service addition
* Shutdown timeouts
* Individual service management (stop/restart)
* Concurrent access safety

---

## 🔧 Advanced Usage

### Using Multiple Options
```go
s := supervisor.New(ctx,
	supervisor.Options{
		Logger:          customLogger,
		RestartLimit:    10,
	},
	supervisor.Options{
		ShutdownTimeout: 10 * time.Second,
		RestartDelay:    500 * time.Millisecond,
	},
)
// Later options override earlier ones
```

### Early Termination
```go
// Stop the supervisor before all services complete
cancel() // cancel the parent context
// or
s.StopAll()
```

### Panic Recovery
The supervisor automatically recovers from panics in services and restarts them according to the restart policy.
