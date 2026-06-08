package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/internal/server"
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
	)

	b, err := broker.New(cfg.DataDir, cfg.DedupeCap)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			logger.Error("broker close", "err", err)
		}
	}()

	srv := server.New(cfg.Addr, b)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ctx)
	}()

	// Wait for either Serve to error out or for ctx to cancel
	// (SIGINT / SIGTERM). Either way, run graceful shutdown.
	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		// Serve returned nil — clean exit, no further work.
		return nil
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Warn("shutdown drain", "err", err)
	}
	if err := <-serveErr; err != nil {
		return fmt.Errorf("serve after shutdown: %w", err)
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
