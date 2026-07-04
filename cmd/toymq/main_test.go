package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestRunBrokerOpenFails(t *testing.T) {
	// Point -data-dir at a regular file so broker.New errors trying
	// to ReadDir the topics subdirectory.
	dir := t.TempDir()
	asFile := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(asFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var out, errOut bytes.Buffer
	err := run(context.Background(),
		[]string{"-addr", "127.0.0.1:0", "-data-dir", asFile, "-shutdown-timeout", "1s"},
		&out, &errOut,
	)
	if err == nil {
		t.Fatal("run with bad data-dir: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "broker") {
		t.Errorf("err = %v, want it to mention broker", err)
	}
}

func TestRunBindFails(t *testing.T) {
	// Bind a listener ourselves, then ask run to bind the same addr.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer l.Close()
	addr := l.Addr().String()

	var out, errOut bytes.Buffer
	err = run(context.Background(),
		[]string{"-addr", addr, "-data-dir", t.TempDir(), "-shutdown-timeout", "1s"},
		&out, &errOut,
	)
	if err == nil {
		t.Fatal("run with already-bound addr: expected error, got nil")
	}
}

func TestBuildLogger(t *testing.T) {
	cases := []struct {
		level   string
		format  string
		enabled slog.Level
		denied  slog.Level
	}{
		{"debug", "text", slog.LevelDebug, slog.LevelDebug - 1},
		{"info", "text", slog.LevelInfo, slog.LevelDebug},
		{"warn", "text", slog.LevelWarn, slog.LevelInfo},
		{"error", "text", slog.LevelError, slog.LevelWarn},
		{"debug", "json", slog.LevelDebug, slog.LevelDebug - 1},
		{"info", "json", slog.LevelInfo, slog.LevelDebug},
		{"warn", "json", slog.LevelWarn, slog.LevelInfo},
		{"error", "json", slog.LevelError, slog.LevelWarn},
	}
	for _, tc := range cases {
		t.Run(tc.level+"_"+tc.format, func(t *testing.T) {
			var buf bytes.Buffer
			logger := buildLogger(&buf, tc.level, tc.format)
			if logger == nil {
				t.Fatal("buildLogger returned nil")
			}
			ctx := context.Background()
			if !logger.Enabled(ctx, tc.enabled) {
				t.Errorf("level %q: should accept %v", tc.level, tc.enabled)
			}
			if logger.Enabled(ctx, tc.denied) {
				t.Errorf("level %q: should reject %v", tc.level, tc.denied)
			}
			// Emit at the threshold and verify the format by inspecting
			// the produced bytes — json starts with `{`, text doesn't.
			logger.Log(ctx, tc.enabled, "probe")
			out := buf.String()
			isJSON := strings.HasPrefix(out, "{")
			wantJSON := tc.format == "json"
			if isJSON != wantJSON {
				t.Errorf("format %q: output %q (json=%v) does not match expectation", tc.format, out, wantJSON)
			}
		})
	}

	// Unknown level / format fall through to info/text defaults.
	t.Run("unknown_level_falls_back_to_info", func(t *testing.T) {
		logger := buildLogger(io.Discard, "trace", "text")
		if logger.Enabled(context.Background(), slog.LevelDebug) {
			t.Error("unknown level should not accept debug")
		}
		if !logger.Enabled(context.Background(), slog.LevelInfo) {
			t.Error("unknown level should accept info (the default)")
		}
	})
}

// TestRunClientRoundTrip starts the binary on a known port, sends a
// PUB, reads OK, then signals shutdown. Exercises run() end-to-end
// through server.Serve and the session pipeline.
func TestRunClientRoundTrip(t *testing.T) {
	// Pick a free port up front so the client knows what to dial.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close() // release for run() to bind

	var out, errOut bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx,
			[]string{"-addr", addr, "-data-dir", t.TempDir(), "-shutdown-timeout", "1s"},
			&out, &errOut,
		)
	}()

	// Wait for the server to bind. Poll Dial with a short timeout.
	var conn net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err = net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("dial did not succeed in 2s: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Default posture requires the HELLO handshake (ADR 0020).
	go func() { conn.Write([]byte("HELLO 1\nPUB orders - - 5\nhello\n")) }()
	br := bufio.NewReader(conn)
	hello, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read HELLO resp: %v", err)
	}
	if strings.TrimRight(hello, "\r\n") != "HELLO 1 OK" {
		t.Fatalf("handshake = %q, want HELLO 1 OK", hello)
	}
	pubResp, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read PUB resp: %v", err)
	}
	if !strings.HasPrefix(pubResp, "OK ") {
		t.Fatalf("got %q want OK ...", pubResp)
	}

	conn.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
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
