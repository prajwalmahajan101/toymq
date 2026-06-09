package client

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureLogger returns a slog.Logger that writes JSON records into
// the returned buffer, guarded by mu so concurrent goroutines can
// read it after Close.
func captureLogger(t *testing.T) (*slog.Logger, *bytes.Buffer, *sync.Mutex) {
	t.Helper()
	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	w := &lockedWriter{mu: &mu, w: &buf}
	logger := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logger, &buf, &mu
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func TestWithLogger_RecordsDialAndClose(t *testing.T) {
	addr, cleanup := acceptOne(t)
	defer cleanup()

	logger, buf, mu := captureLogger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, err := Dial(ctx, addr, WithLogger(logger))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-c.loopDone

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if !strings.Contains(out, `"msg":"dialed"`) {
		t.Fatalf("expected dialed record, got %q", out)
	}
	if !strings.Contains(out, `"msg":"closed"`) {
		t.Fatalf("expected closed record, got %q", out)
	}
}

func TestWithoutLogger_SilentByDefault(t *testing.T) {
	addr, cleanup := acceptOne(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, err := Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if c.logger != nil {
		t.Fatalf("Client.logger = %v, want nil by default", c.logger)
	}
	// Direct call to the helper must be a no-op even with a non-nil
	// message — the nil-check is the contract.
	c.log(slog.LevelDebug, "should-not-be-emitted")
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-c.loopDone
}
