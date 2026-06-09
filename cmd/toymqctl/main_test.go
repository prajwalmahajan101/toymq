package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRun_NoArgs(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := run(context.Background(), nil, &out, &errBuf); code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
	if !strings.Contains(errBuf.String(), "usage:") {
		t.Fatalf("stderr=%q", errBuf.String())
	}
}

func TestRun_UnknownVerb(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"bogus"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
	if !strings.Contains(errBuf.String(), `unknown command "bogus"`) {
		t.Fatalf("stderr=%q", errBuf.String())
	}
}

func TestRun_Help(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := run(context.Background(), []string{"--help"}, &out, &errBuf); code != exitOK {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(out.String(), "Commands:") {
		t.Fatalf("stdout=%q", out.String())
	}
}

func TestRoundTrip_PubSub(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	run(context.Background(),
		[]string{"pub", "--addr", addr, "orders", "hello"}, &out, &errBuf)
	out.Reset()
	code := run(context.Background(),
		[]string{"sub", "--addr", addr, "--max-msgs", "1", "orders", "c1"},
		&out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(out.String(), `payload="hello"`) {
		t.Fatalf("stdout=%q", out.String())
	}
}
