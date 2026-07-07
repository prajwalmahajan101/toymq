package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/internal/tracing"
	"github.com/prajwalmahajan101/toymq/internal/wal"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cfg, err := config.Parse(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("config: %w", err)
	}

	logger := buildLogger(stdout, cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)

	logger.Info("starting toymq",
		"addr", cfg.Addr,
		"data-dir", cfg.DataDir,
		"shutdown-timeout", cfg.ShutdownTimeout,
		"dedupe-cap", cfg.DedupeCap,
		"metrics-addr", cfg.MetricsAddr,
		"otlp-endpoint", cfg.OTLPEndpoint,
	)

	// Observability (off by default per ADR 0015). Both blocks are
	// no-ops when the corresponding flag is empty.
	var (
		mtr *metrics.Metrics
		reg = metrics.NewRegistry()
	)
	if cfg.MetricsAddr != "" {
		mtr = metrics.New(reg)
	}

	tp, err := tracing.New(ctx, cfg.OTLPEndpoint, cfg.ServiceVersion, cfg.TraceSampleRatio)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutCtx); err != nil {
			logger.Warn("tracer shutdown", "err", err)
		}
	}()

	// FsyncMode was validated in config.Parse, so ParseSyncMode cannot fail here.
	syncMode, _ := wal.ParseSyncMode(cfg.FsyncMode)
	syncCfg := broker.SyncConfig{Mode: syncMode, Interval: cfg.FsyncInterval}
	retCfg := broker.RetentionConfig{
		SegmentBytes:   uint64(cfg.SegmentBytes),
		RetainBytes:    uint64(cfg.RetainBytes),
		RetainDuration: cfg.RetainDuration,
	}

	b, err := broker.NewWithObservability(cfg.DataDir, cfg.DedupeCap, cfg.DefaultPartitions, cfg.RecvWindow, 30*time.Second, 1*time.Second, syncCfg, retCfg, mtr, tp.Tracer())
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			logger.Error("broker close", "err", err)
		}
	}()

	// Handshake / auth / TLS options (ADR 0020), shared by both listeners.
	serverOpts := []server.Option{server.WithRequireHello(cfg.RequireHello)}
	if cfg.AuthTokenFile != "" {
		tokens, err := server.LoadTokens(cfg.AuthTokenFile)
		if err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		serverOpts = append(serverOpts, server.WithTokens(tokens))
		logger.Info("auth enabled", "tokens", len(tokens))
	}

	// Plain listener on -addr, plus an optional TLS listener on -tls-addr
	// that runs side-by-side so clients can migrate one at a time.
	servers := []*server.Server{
		server.NewWithObservability(cfg.Addr, b, mtr, serverOpts...),
	}
	if cfg.TLSAddr != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return fmt.Errorf("tls: load keypair: %w", err)
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		tlsOpts := make([]server.Option, 0, len(serverOpts)+1)
		tlsOpts = append(tlsOpts, serverOpts...)
		tlsOpts = append(tlsOpts, server.WithTLS(tlsCfg))
		servers = append(servers, server.NewWithObservability(cfg.TLSAddr, b, mtr, tlsOpts...))
	}

	// Metrics HTTP server (optional). Lives on a separate goroutine
	// keyed by cfg.MetricsAddr so the broker's TCP wire-protocol
	// port stays unmuxed.
	var metricsSrv *http.Server
	metricsErr := make(chan error, 1)
	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		})
		metricsSrv = &http.Server{
			Addr:              cfg.MetricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			logger.Info("metrics listening", "addr", cfg.MetricsAddr)
			err := metricsSrv.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				metricsErr <- err
			}
			close(metricsErr)
		}()
	}

	// One Serve goroutine per listener; all report to serveErr.
	serveErr := make(chan error, len(servers))
	for _, srv := range servers {
		go func(srv *server.Server) {
			serveErr <- srv.Serve(ctx)
		}(srv)
	}

	// Wait for any Serve to error out or for ctx to cancel
	// (SIGINT / SIGTERM). Either way, run graceful shutdown.
	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		// A Serve returned nil (listener closed) — clean exit.
		return nil
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	for _, srv := range servers {
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.Warn("shutdown drain", "err", err)
		}
	}
	for range servers {
		if err := <-serveErr; err != nil {
			return fmt.Errorf("serve after shutdown: %w", err)
		}
	}

	if metricsSrv != nil {
		if err := metricsSrv.Shutdown(shutCtx); err != nil {
			logger.Warn("metrics shutdown", "err", err)
		}
		// Drain the goroutine's err channel.
		if err, ok := <-metricsErr; ok && err != nil {
			logger.Warn("metrics serve", "err", err)
		}
	}

	return nil
}

func buildLogger(w io.Writer, level, format string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	default:
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}
