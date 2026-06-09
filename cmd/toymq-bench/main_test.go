package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRun_DefaultsPrint(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := run(context.Background(), nil, &out, &errBuf); code != exitOK {
		t.Fatalf("code=%d stderr=%q", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "producers=4") {
		t.Fatalf("stdout=%q", out.String())
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
