package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestRunAck_AcknowledgesId(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	run(context.Background(),
		[]string{"pub", "--addr", addr, "orders", "x"}, &out, &errBuf)

	out.Reset()
	code := run(context.Background(),
		[]string{"ack", "--addr", addr, "orders", "cg", "0"}, &out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d stderr=%q", code, errBuf.String())
	}
	if got := strings.TrimRight(out.String(), "\n"); got != "OK 0" {
		t.Fatalf("stdout=%q", out.String())
	}

	// Verify no replay on a fresh subscription with the same id.
	out.Reset()
	errBuf.Reset()
	done := make(chan int, 1)
	subCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		done <- run(subCtx,
			[]string{"sub", "--addr", addr, "--max-msgs", "1", "orders", "cg"},
			&out, &errBuf)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sub did not exit")
	}
	if strings.Contains(out.String(), "MSG") {
		t.Fatalf("got replay: %q", out.String())
	}
}

func TestRunAck_BadMsgID(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"ack", "--addr", addr, "orders", "cg", "not-a-number"},
		&out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
}

func TestRunAck_MissingArgs(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"ack", "orders", "cg"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
}
