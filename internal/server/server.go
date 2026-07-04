package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/metrics"
)

const (
	emfileBackoffMin = 5 * time.Millisecond
	emfileBackoffMax = time.Second
)

// Server wraps the TCP listener and per-connection Session
// lifecycle. One Server per broker process; goroutine ownership is
// documented in ADR 0008.
type Server struct {
	addr      string
	broker    *broker.Broker
	auth      authConfig
	tlsConfig *tls.Config // nil => plaintext listener

	mu       sync.Mutex
	listener net.Listener

	closeOnce sync.Once
	wg        sync.WaitGroup

	// metrics is optional; nil means "metrics off". Helpers on
	// *metrics.Metrics already nil-check.
	metrics *metrics.Metrics
}

// Option configures a Server at construction. The zero-value Server
// (no options) keeps pre-M3 behaviour: no handshake, no auth.
type Option func(*Server)

// WithRequireHello makes the HELLO handshake mandatory (v) or optional
// (compat migration window). See ADR 0020.
func WithRequireHello(v bool) Option {
	return func(s *Server) { s.auth.requireHello = v }
}

// WithTokens enables bearer-token auth: HELLO must carry a matching
// AUTH token. An empty slice leaves auth disabled.
func WithTokens(tokens []string) Option {
	return func(s *Server) { s.auth.tokens = tokens }
}

// WithTLS makes this Server terminate TLS: Serve wraps its listener with
// tls.NewListener(cfg). Pass a config with a loaded certificate and
// MinVersion >= TLS 1.2. nil leaves the listener plaintext.
func WithTLS(cfg *tls.Config) Option {
	return func(s *Server) { s.tlsConfig = cfg }
}

// New constructs an unstarted Server bound to addr against broker b.
// Call Serve to start accepting; Shutdown to drain.
func New(addr string, b *broker.Broker, opts ...Option) *Server {
	s := &Server{addr: addr, broker: b}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewWithObservability is New plus a *Metrics pointer. The Server
// uses it to maintain the toymq_active_sessions gauge. nil m yields
// the same behaviour as New.
func NewWithObservability(addr string, b *broker.Broker, m *metrics.Metrics, opts ...Option) *Server {
	s := New(addr, b, opts...)
	s.metrics = m
	return s
}

// Addr returns the actual bound address. Useful when New was given
// ":0" and the kernel picked a port - tests need this to dial.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) closeListenerOnce() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		l := s.listener
		s.mu.Unlock()
		if l != nil {
			err = l.Close()
		}
	})

	return err
}

// Serve binds the listener, then accepts connections and spawns one
// Session per conn under the internal WaitGroup. Returns nil when
// the listener closes cleanly (via Shutdown); returns an error for
// fatal Accept failures.
func (s *Server) Serve(ctx context.Context) error {
	// Count Serve itself in the wg. This forces wg.Add(1) to happen
	// before listener assignment, which is the synchronization point
	// callers use (via Addr()) to know the server is ready. Without
	// this, the per-session Add(1) inside the accept loop is
	// unsynchronized with wg.Wait() in Shutdown.
	s.wg.Add(1)
	defer s.wg.Done()

	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	if s.tlsConfig != nil {
		l = tls.NewListener(l, s.tlsConfig)
	}

	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	slog.Info("listening", "addr", l.Addr().String(), "tls", s.tlsConfig != nil)

	// serveCtx is cancelled when the ctx cancels OR Serve exits.
	// The watcher uses it to know when to give up watching
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()

	ctxWatcherDone := make(chan struct{})

	go func() {
		defer close(ctxWatcherDone)
		<-serveCtx.Done()
		_ = s.closeListenerOnce()
	}()

	defer func() {
		cancelServe()
		<-ctxWatcherDone
	}()

	backoff := time.Duration(0)

	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if errors.Is(err, syscall.EMFILE) {
				if backoff == 0 {
					backoff = emfileBackoffMin
				}
				slog.Warn("accept: fd exhaustion, backing off", "delay", backoff)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > emfileBackoffMax {
					backoff = emfileBackoffMax
				}
				continue
			}
			return fmt.Errorf("accept: %w", err)
		}
		backoff = 0

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.metrics.IncSessions()
			defer s.metrics.DecSessions()
			sess := NewSession(c, s.broker, s.auth)
			sess.Run(ctx)
		}(conn)

	}
}

// Shutdown closes the listener and waits for in-flight Sessions to
// drain, bounded by ctx. Returns ctx.Err() on deadline; nil on a
// clean drain.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutdown started")
	_ = s.closeListenerOnce()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		slog.Info("shutdown drained")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
