package config

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(nil, io.Discard)
	if err != nil {
		t.Fatalf("Parse(nil): %v", err)
	}
	if cfg.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want %q", cfg.Addr, DefaultAddr)
	}
	if cfg.DataDir != DefaultDataDir {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, DefaultDataDir)
	}
	if cfg.LogLevel != DefaultLogLevel {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, DefaultLogLevel)
	}
	if cfg.LogFormat != DefaultLogFormat {
		t.Errorf("LogFormat = %q, want %q", cfg.LogFormat, DefaultLogFormat)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}
	if cfg.DedupeCap != DefaultDedupeCap {
		t.Errorf("DedupeCap = %d, want %d", cfg.DedupeCap, DefaultDedupeCap)
	}
}

func TestParseOverrides(t *testing.T) {
	args := []string{
		"-addr", "127.0.0.1:9999",
		"-data-dir", "/tmp/x",
		"-log-level", "debug",
		"-log-format", "json",
		"-shutdown-timeout", "2s",
		"-dedupe-cap", "128",
	}
	cfg, err := Parse(args, io.Discard)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Addr != "127.0.0.1:9999" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.DataDir != "/tmp/x" {
		t.Errorf("DataDir = %q", cfg.DataDir)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q", cfg.LogFormat)
	}
	if cfg.ShutdownTimeout != 2*time.Second {
		t.Errorf("ShutdownTimeout = %v", cfg.ShutdownTimeout)
	}
	if cfg.DedupeCap != 128 {
		t.Errorf("DedupeCap = %d", cfg.DedupeCap)
	}
}

func TestParseValidation(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantInErr string
	}{
		{"empty addr", []string{"-addr", ""}, "addr must not be empty"},
		{"empty data-dir", []string{"-data-dir", ""}, "data-dir must not be empty"},
		{"bad log-level", []string{"-log-level", "trace"}, "log-level"},
		{"bad log-format", []string{"-log-format", "yaml"}, "log-format"},
		{"zero shutdown timeout", []string{"-shutdown-timeout", "0"}, "shutdown-timeout"},
		{"negative shutdown timeout", []string{"-shutdown-timeout", "-1s"}, "shutdown-timeout"},
		{"zero dedupe cap", []string{"-dedupe-cap", "0"}, "dedupe-cap"},
		{"negative dedupe cap", []string{"-dedupe-cap", "-1"}, "dedupe-cap"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.args, io.Discard)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantInErr) {
				t.Errorf("err = %v, want substring %q", err, tc.wantInErr)
			}
		})
	}
}

func TestParseUnknownFlag(t *testing.T) {
	_, err := Parse([]string{"-bogus"}, io.Discard)
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}
