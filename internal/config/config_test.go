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
	if cfg.RecvWindow != DefaultRecvWindow {
		t.Errorf("RecvWindow = %d, want %d", cfg.RecvWindow, DefaultRecvWindow)
	}
	if cfg.FsyncMode != DefaultFsyncMode {
		t.Errorf("FsyncMode = %q, want %q", cfg.FsyncMode, DefaultFsyncMode)
	}
	if cfg.FsyncInterval != DefaultFsyncInterval {
		t.Errorf("FsyncInterval = %v, want %v", cfg.FsyncInterval, DefaultFsyncInterval)
	}
	if !cfg.RequireHello {
		t.Error("RequireHello default = false, want true")
	}
	if cfg.AuthTokenFile != "" || cfg.TLSAddr != "" || cfg.TLSCert != "" || cfg.TLSKey != "" {
		t.Error("auth/TLS defaults should be empty")
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
		"-recv-window", "32",
		"-fsync", "batched",
		"-fsync-interval", "10ms",
		"-segment-bytes", "1048576",
		"-retain-bytes", "8388608",
		"-retain-duration", "24h",
	}
	cfg, err := Parse(args, io.Discard)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.SegmentBytes != 1048576 {
		t.Errorf("SegmentBytes = %d, want 1048576", cfg.SegmentBytes)
	}
	if cfg.RetainBytes != 8388608 {
		t.Errorf("RetainBytes = %d, want 8388608", cfg.RetainBytes)
	}
	if cfg.RetainDuration != 24*time.Hour {
		t.Errorf("RetainDuration = %v, want 24h", cfg.RetainDuration)
	}
	if cfg.FsyncMode != "batched" {
		t.Errorf("FsyncMode = %q, want batched", cfg.FsyncMode)
	}
	if cfg.FsyncInterval != 10*time.Millisecond {
		t.Errorf("FsyncInterval = %v, want 10ms", cfg.FsyncInterval)
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
	if cfg.RecvWindow != 32 {
		t.Errorf("RecvWindow = %d", cfg.RecvWindow)
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
		{"zero recv window", []string{"-recv-window", "0"}, "recv-window"},
		{"negative recv window", []string{"-recv-window", "-1"}, "recv-window"},
		{"bad fsync mode", []string{"-fsync", "sometimes"}, "fsync"},
		{"batched zero interval", []string{"-fsync", "batched", "-fsync-interval", "0"}, "fsync-interval"},
		{"batched negative interval", []string{"-fsync", "batched", "-fsync-interval", "-1ms"}, "fsync-interval"},
		{"cert without key", []string{"-tls-cert", "c.pem"}, "tls-cert and tls-key"},
		{"key without cert", []string{"-tls-key", "k.pem"}, "tls-cert and tls-key"},
		{"tls-addr without cert", []string{"-tls-addr", ":6790"}, "tls-addr requires"},
		{"negative segment-bytes", []string{"-segment-bytes", "-1"}, "segment-bytes"},
		{"negative retain-bytes", []string{"-retain-bytes", "-1"}, "retain-bytes"},
		{"negative retain-duration", []string{"-retain-duration", "-1s"}, "retain-duration"},
		{"retain-bytes without segment-bytes", []string{"-retain-bytes", "1024"}, "require -segment-bytes"},
		{"retain-duration without segment-bytes", []string{"-retain-duration", "1h"}, "require -segment-bytes"},
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
