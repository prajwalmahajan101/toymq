package main

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"
)

func TestRunHelp(t *testing.T) {
	var out, errOut bytes.Buffer
	err := run(context.Background(), []string{"-h"}, &out, &errOut)
	if err != nil {
		t.Fatalf("run with -h returned err: %v", err)
	}
}

func TestRunInvalidFlag(t *testing.T) {
	var out, errOut bytes.Buffer
	err := run(context.Background(), []string{"-log-level", "trace"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected error for bad log-level")
	}
}

func TestRunStartsAndShutsDown(t *testing.T) {
	baseline := runtime.NumGoroutine()

	var out, errOut bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- run(ctx,
			[]string{
				"-addr", "127.0.0.1:0",
				"-data-dir", t.TempDir(),
				"-shutdown-timeout", "1s",
			},
			&out, &errOut,
		)
	}()

	// Give the server a moment to start listening.
	time.Sleep(100 * time.Millisecond)

	// Signal shutdown.
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not exit within 3s of ctx cancel")
	}

	// Allow stragglers a tick to wind down.
	time.Sleep(100 * time.Millisecond)
	now := runtime.NumGoroutine()
	if now > baseline+2 {
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, now)
	}
}
