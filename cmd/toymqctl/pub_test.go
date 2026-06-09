package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunPub_OK(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"pub", "--addr", addr, "orders", "hello"}, &out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d stderr=%q", code, errBuf.String())
	}
	if got := strings.TrimRight(out.String(), "\n"); got != "OK 0" {
		t.Fatalf("stdout=%q", out.String())
	}
}

func TestRunPub_Dup(t *testing.T) {
	addr := startBroker(t)
	var out, errBuf bytes.Buffer
	_ = run(context.Background(),
		[]string{"pub", "--addr", addr, "--key", "k1", "orders", "x"}, &out, &errBuf)
	out.Reset()
	code := run(context.Background(),
		[]string{"pub", "--addr", addr, "--key", "k1", "orders", "x"}, &out, &errBuf)
	if code != exitOK {
		t.Fatalf("code=%d", code)
	}
	if got := strings.TrimRight(out.String(), "\n"); got != "DUP 0" {
		t.Fatalf("stdout=%q", out.String())
	}
}

func TestRunPub_DialRefused(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(),
		[]string{"pub", "--addr", "127.0.0.1:1", "orders", "x"}, &out, &errBuf)
	if code != exitErr {
		t.Fatalf("code=%d, want %d", code, exitErr)
	}
}

func TestRunPub_MissingArgs(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"pub", "orders"}, &out, &errBuf)
	if code != exitUsage {
		t.Fatalf("code=%d, want %d", code, exitUsage)
	}
}
