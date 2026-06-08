package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
)

const (
	emfileBackoffMin = 5 * time.Millisecond
	emfileBackoffMax = time.Second
)

type Server struct {
	addr   string
	broker *broker.Broker

	mu       sync.Mutex
	listener net.Listener

	closeOnce sync.Once
	wg        sync.WaitGroup
}

func New(addr string, b *broker.Broker) *Server {
	return &Server{addr: addr, broker: b}
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

	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

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
				slog.Warn("accept: fd exhuastion, backing off", "delay", backoff)
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
			sess := NewSession(c, s.broker)
			sess.Run(ctx)
		}(conn)

	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	_ = s.closeListenerOnce()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
