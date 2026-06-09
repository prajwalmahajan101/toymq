package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRun_DialRefused(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"--addr", "127.0.0.1:1", "--producers", "1", "--msgs", "1"},
		&out, &errBuf)
	if code != exitErr {
		t.Fatalf("code=%d, want %d", code, exitErr)
	}
}

func TestRun_EndToEnd(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"--addr", addr, "--producers", "2", "--msgs", "200", "--size", "32"},
		&out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d stderr=%q", code, errBuf.String())
	}
	s := out.String()
	for _, want := range []string{"producers=2", "msgs=200", "elapsed", "throughput", "p50=", "errors      0"} {
		if !strings.Contains(s, want) {
			t.Fatalf("report missing %q: %s", want, s)
		}
	}
}

func TestRun_BadFlag(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"--bogus"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
}

func TestRun_NegativeProducers(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"--producers", "0"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(errBuf.String(), "--producers must be > 0") {
		t.Fatalf("stderr=%q", errBuf.String())
	}
}

func TestRun_NegativeMsgs(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"--msgs", "-1"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d", code)
	}
}

func TestRun_UnexpectedPositional(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"junk"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d", code)
	}
}
