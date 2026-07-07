// Package integration holds end-to-end tests that drive ToyMQ over a
// real TCP socket. The system under test is constructed in-process
// (broker + server in this test binary's address space) so timing
// knobs like the visibility timeout can be shortened without
// exposing test-only flags on the production binary. See
// docs/adr/0010-integration-test-architecture.md.
package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/server"
)

const (
	defaultDedupeCap = 1024
	// defaultRecvWindow is generous so pre-M5 tests (which deliver many
	// messages before acking) are unaffected by flow control; tests that
	// exercise the window set a small one via withRecvWindow (ADR 0022).
	defaultRecvWindow        = 4096
	defaultVisibility        = 100 * time.Millisecond
	defaultRedeliverInterval = 20 * time.Millisecond
	defaultAddrPollInterval  = 2 * time.Millisecond
	defaultAddrPollTimeout   = time.Second
	defaultShutdownTimeout   = 2 * time.Second
)

type harnessOpts struct {
	dedupeCap         int
	recvWindow        int
	visibility        time.Duration
	redeliverInterval time.Duration
	defaultPartitions int
	dataDir           string
}

type harnessOpt func(*harnessOpts)

func withVisibility(d time.Duration) harnessOpt {
	return func(o *harnessOpts) { o.visibility = d }
}

func withRedeliverInterval(d time.Duration) harnessOpt {
	return func(o *harnessOpts) { o.redeliverInterval = d }
}

// withRecvWindow sets the per-consumer receive window (ADR 0022). Used by
// flow-control tests that need a small, observable window.
func withRecvWindow(n int) harnessOpt {
	return func(o *harnessOpts) { o.recvWindow = n }
}

// withDefaultPartitions makes auto-created topics use n partitions (ADR
// 0021). Tests that need explicit per-topic counts can send CREATE instead.
func withDefaultPartitions(n int) harnessOpt {
	return func(o *harnessOpts) { o.defaultPartitions = n }
}

type harness struct {
	addr     string
	dataDir  string
	broker   *broker.Broker
	server   *server.Server
	cancel   context.CancelFunc
	serveErr chan error
	opts     harnessOpts
}

func startBroker(t *testing.T, opts ...harnessOpt) *harness {
	t.Helper()
	resolved := harnessOpts{
		dedupeCap:         defaultDedupeCap,
		recvWindow:        defaultRecvWindow,
		visibility:        defaultVisibility,
		redeliverInterval: defaultRedeliverInterval,
		defaultPartitions: 1,
	}
	for _, opt := range opts {
		opt(&resolved)
	}
	if resolved.dataDir == "" {
		resolved.dataDir = t.TempDir()
	}

	h := buildHarness(t, resolved)

	// Owning cleanup: regardless of whether the test calls shutdown
	// or restart, this captures the same *harness and dispatches to
	// whatever broker/server it currently holds. After shutdown,
	// broker is nil and the cleanup is a no-op; after restart, the
	// fields point to the fresh pair and this still does the right
	// thing.
	t.Cleanup(func() {
		if h.broker != nil {
			h.shutdown(t)
		}
	})

	return h
}

func buildHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()
	parts := opts.defaultPartitions
	if parts < 1 {
		parts = 1
	}
	window := opts.recvWindow
	if window < 1 {
		window = defaultRecvWindow
	}
	b, err := broker.NewWithObservability(opts.dataDir, opts.dedupeCap, parts, window, opts.visibility, opts.redeliverInterval, broker.SyncConfig{}, broker.RetentionConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("broker.NewWithObservability: %v", err)
	}

	srv := server.New("127.0.0.1:0", b)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	addr, err := waitForAddr(srv, defaultAddrPollTimeout)
	if err != nil {
		cancel()
		_ = b.Close()
		t.Fatalf("server did not bind: %v", err)
	}

	return &harness{
		addr:     addr,
		dataDir:  opts.dataDir,
		broker:   b,
		server:   srv,
		cancel:   cancel,
		serveErr: serveErr,
		opts:     opts,
	}
}

func waitForAddr(srv *server.Server, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		if addr := srv.Addr(); addr != "" {
			return addr, nil
		}
		if time.Now().After(deadline) {
			return "", errors.New("timed out waiting for server.Addr")
		}
		time.Sleep(defaultAddrPollInterval)
	}
}

// shutdown stops the server and closes the broker. Idempotent.
func (h *harness) shutdown(t *testing.T) {
	t.Helper()
	if h.broker == nil {
		return
	}

	shutCtx, cancelShut := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancelShut()
	if err := h.server.Shutdown(shutCtx); err != nil {
		t.Errorf("server.Shutdown: %v", err)
	}

	h.cancel()
	if err := <-h.serveErr; err != nil {
		t.Errorf("server.Serve returned: %v", err)
	}

	if err := h.broker.Close(); err != nil {
		t.Errorf("broker.Close: %v", err)
	}

	h.broker = nil
	h.server = nil
}

// restart shuts the current broker+server and brings up a fresh pair
// on the same data dir. The harness pointer is reused so callers
// can keep their existing reference.
func (h *harness) restart(t *testing.T) {
	t.Helper()
	opts := h.opts
	h.shutdown(t)
	*h = *buildHarness(t, opts)
}
