# Supervisor

**Supervisor** is a lightweight and robust service manager for Go.
It runs and monitors multiple services, providing:

* automatic restarts on errors or panics,
* restart rate limiting,
* graceful shutdown with timeout,
* OS signal handling (`SIGINT`, `SIGTERM`),
* and dynamic service registration at runtime.

---

## 🚀 Installation

```bash
go get github.com/elum-utils/supervisor
```

---

## ⚙️ Usage Example

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
s.Add("dynamic", func(ctx context.Context) error {
	<-ctx.Done()
	return nil
})
```

---

## 🔁 Automatic Restart

Services are automatically restarted when they:

* panic, or
* return a non-nil error.

Example:

```go
s.Add("unstable", func(ctx context.Context) error {
	panic("oops!")
})
```

Supervisor catches the panic, logs it, and restarts the service according to `RestartLimit` and `RestartDelay`.

---

## 🕊️ Graceful Shutdown

Supervisor stops all services when:

* the parent context is canceled,
* a termination signal (`SIGINT`, `SIGTERM`) is received,
* or restart limits are exceeded.

A configurable `ShutdownTimeout` ensures the supervisor doesn't hang forever.

---

## ⚙️ Options

| Field             | Type            | Description                                             |
| ----------------- | --------------- | ------------------------------------------------------- |
| `Logger`          | `Logger`        | Any type implementing `Printf(format string, v ...any)` |
| `RestartLimit`    | `int`           | Maximum restart attempts before stopping all services   |
| `RestartInterval` | `time.Duration` | Time window for counting restarts                       |
| `RestartDelay`    | `time.Duration` | Delay before restarting a failed service                |
| `ShutdownTimeout` | `time.Duration` | Max wait time for graceful shutdown                     |

---

## 🧪 Testing

Fully covered by unit tests:

```bash
go test -v ./...
```

Tests verify:

* service startup and shutdown,
* signal-based termination,
* panic and error recovery,
* restart limit enforcement,
* dynamic service addition,
* and shutdown timeouts.
