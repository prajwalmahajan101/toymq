package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunSub_StreamsMaxMsgs(t *testing.T) {
	addr := startBroker(t)
	for _, p := range []string{"a", "b", "c"} {
		var out, errBuf bytes.Buffer
		run(context.Background(),
			[]string{"pub", "--addr", addr, "orders", p}, &out, &errBuf)
	}

	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"sub", "--addr", addr, "--max-msgs", "3", "orders", "c1"},
		&out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d stderr=%q", code, errBuf.String())
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 MSG lines, got %d: %v", len(lines), lines)
	}
	for i, want := range []string{`payload="a"`, `payload="b"`, `payload="c"`} {
		if !strings.Contains(lines[i], want) {
			t.Fatalf("line %d=%q want %q", i, lines[i], want)
		}
	}
}

func TestRunSub_CtxCancel(t *testing.T) {
	addr := startBroker(t)

	// Pre-publish one MSG so we can synchronously confirm Sub has
	// fully handshaked (broker pushed MSG, client printed it)
	// before cancelling. Avoids the prior 100ms-sleep race.
	var pubOut, pubErr bytes.Buffer
	if code := run(context.Background(),
		[]string{"pub", "--addr", addr, "orders", "x"}, &pubOut, &pubErr); code != exitOK {
		t.Fatalf("pub setup: code=%d stderr=%q", code, pubErr.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sub writes to out from its own goroutine; the test goroutine
	// reads. bytes.Buffer is not goroutine-safe, so guard with mu.
	var (
		mu     sync.Mutex
		out    bytes.Buffer
		errBuf bytes.Buffer
	)
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"sub", "--addr", addr, "--no-auto-ack", "orders", "c1"},
			&lockedWriter{mu: &mu, buf: &out}, &errBuf)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		seen := strings.Contains(out.String(), "MSG ")
		mu.Unlock()
		if seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sub did not produce MSG within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("code=%d stderr=%q", code, errBuf.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sub did not exit on ctx cancel")
	}
}

// lockedWriter serialises writes to an underlying buffer so a test
// goroutine can safely read while another writes.
type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func TestRunSub_NoAutoAckReplays(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	run(context.Background(),
		[]string{"pub", "--addr", addr, "orders", "x"}, &out, &errBuf)

	out.Reset()
	run(context.Background(),
		[]string{"sub", "--addr", addr, "--no-auto-ack", "--max-msgs", "1", "orders", "cg"},
		&out, &errBuf)

	out.Reset()
	code := run(context.Background(),
		[]string{"sub", "--addr", addr, "--max-msgs", "1", "orders", "cg"},
		&out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(out.String(), `payload="x"`) {
		t.Fatalf("no replay: stdout=%q", out.String())
	}
}

func TestRunSub_MissingArgs(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"sub", "orders"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
}
