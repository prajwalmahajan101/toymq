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
	defaultDedupeCap         = 1024
	defaultVisibility        = 100 * time.Millisecond
	defaultRedeliverInterval = 20 * time.Millisecond
	defaultAddrPollInterval  = 2 * time.Millisecond
	defaultAddrPollTimeout   = time.Second
	defaultShutdownTimeout   = 2 * time.Second
)

type harnessOpts struct {
	dedupeCap         int
	visibility        time.Duration
	redeliverInterval time.Duration
	dataDir           string
}

type harnessOpt func(*harnessOpts)

func withVisibility(d time.Duration) harnessOpt {
	return func(o *harnessOpts) { o.visibility = d }
}

func withRedeliverInterval(d time.Duration) harnessOpt {
	return func(o *harnessOpts) { o.redeliverInterval = d }
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
		visibility:        defaultVisibility,
		redeliverInterval: defaultRedeliverInterval,
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
	b, err := broker.NewWithTimings(opts.dataDir, opts.dedupeCap, opts.visibility, opts.redeliverInterval)
	if err != nil {
		t.Fatalf("broker.NewWithTimings: %v", err)
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
