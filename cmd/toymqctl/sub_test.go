package main

import (
	"bytes"
	"context"
	"strings"
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		var out, errBuf bytes.Buffer
		done <- run(ctx, []string{"sub", "--addr", addr, "orders", "c1"}, &out, &errBuf)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("code=%d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sub did not exit on ctx cancel")
	}
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
