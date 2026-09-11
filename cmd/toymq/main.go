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
	"github.com/prajwalmahajan101/toymq/internal/logging"
	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/replication"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/internal/tracing"
	"github.com/prajwalmahajan101/toymq/internal/wal"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	filestorage "github.com/prajwalmahajan101/toyraft/pkg/storage/file"
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

	b, err := broker.NewWithObservability(cfg.DataDir, cfg.DedupeCap, cfg.DefaultPartitions, cfg.RecvWindow, 30*time.Second, 1*time.Second, syncCfg, retCfg, cfg.DLQAfterNacks, mtr, tp.Tracer())
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			logger.Error("broker close", "err", err)
		}
	}()

	// Replication (v3 M1, ADR 0028). Off by default; -replicate attaches an
	// embedded single-node raft so every mutating command flows
	// Propose→Apply. Node.Stop must run before broker Close (the deferred
	// b.Close above), so this defer — registered later — runs first.
	if cfg.Replicate {
		node, err := attachRaft(ctx, b, cfg, logger)
		if err != nil {
			return fmt.Errorf("replication: %w", err)
		}
		defer func() {
			if err := node.Stop(); err != nil {
				logger.Warn("raft stop", "err", err)
			}
		}()
	}

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

// attachRaft builds the embedded raft node, starts it, and switches the broker
// into replicated mode. Two shapes:
//
//   - single-node (--peers empty): Peers is [self] (trivially leader) over a
//     no-op transport, because neither shipped toyraft transport can express a
//     self-only cluster (see internal/replication/transport.go). We block until
//     self-leadership so the first PUB does not race ErrNotLeader.
//   - multi-node (--peers set): full membership over the async-wrapped http
//     peer transport. A follower never self-leads, so we do NOT wait for
//     leadership here; the leader-gate (broker propose → *raft.ErrNotLeader,
//     surfaced as NOTLEADER) rejects writes until this node is or knows the
//     leader.
func attachRaft(ctx context.Context, b *broker.Broker, cfg *config.Config, logger *slog.Logger) (raft.Node, error) {
	raftDir := cfg.ResolvedRaftDir()
	store, err := filestorage.New(raftDir)
	if err != nil {
		return nil, fmt.Errorf("open raft storage at %q: %w", raftDir, err)
	}

	peers, transport, err := raftPeersAndTransport(cfg)
	if err != nil {
		return nil, err
	}

	sm := replication.NewBrokerSM(b)
	node, err := raft.New(raft.Config{
		NodeID:       raft.NodeID(cfg.NodeID),
		Peers:        peers,
		Storage:      store,
		Transport:    transport,
		StateMachine: sm,
	})
	if err != nil {
		return nil, fmt.Errorf("build raft node: %w", err)
	}
	if err := node.Start(ctx); err != nil {
		return nil, fmt.Errorf("start raft node: %w", err)
	}

	if cfg.Peers == "" {
		// Single-node campaigns and wins within one election timeout; block so
		// the first PUB does not race a not-yet-leader node.
		if err := waitForLeader(ctx, node, 5*time.Second); err != nil {
			_ = node.Stop()
			return nil, err
		}
	}

	b.AttachRaft(node)
	logger.Info("replication enabled", "node-id", cfg.NodeID, "raft-dir", raftDir,
		"peers", cfg.Peers, "raft-addr", cfg.RaftAddr)
	return node, nil
}

// raftPeersAndTransport returns the raft membership and transport for cfg:
// [self] + no-op transport when --peers is empty, else the full membership +
// the async-wrapped http peer transport.
func raftPeersAndTransport(cfg *config.Config) ([]raft.NodeID, raft.Transport, error) {
	if cfg.Peers == "" {
		return []raft.NodeID{raft.NodeID(cfg.NodeID)}, replication.NewSingleNodeTransport(), nil
	}
	membership := cfg.ClusterPeers() // includes self
	peers := make([]raft.NodeID, 0, len(membership))
	for id := range membership {
		peers = append(peers, raft.NodeID(id))
	}
	transport, err := replication.NewHTTPTransport(cfg.NodeID, cfg.RaftAddr, cfg.PeerURLsExcludingSelf())
	if err != nil {
		return nil, nil, fmt.Errorf("build raft transport: %w", err)
	}
	return peers, transport, nil
}

// waitForLeader polls node.Status until it reports Leader or timeout elapses.
func waitForLeader(ctx context.Context, node raft.Node, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if node.Status().Role == raft.Leader {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("raft node did not reach leader within %s", timeout)
		case <-tick.C:
		}
	}
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
	// Wrap with the correlation handler so *Context log calls carry
	// trace_id/span_id when a span is active (ADR 0027). No-op otherwise.
	h = logging.NewCorrelationHandler(h)
	return slog.New(h)
}
