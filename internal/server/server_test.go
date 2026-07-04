package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/testcerts"
)

func setupServer(t *testing.T) (*Server, *broker.Broker) {
	t.Helper()
	b, err := broker.New(t.TempDir(), 16)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	s := New("127.0.0.1:0", b)
	return s, b
}

// runServer starts s.Serve in a goroutine and returns a channel that
// will deliver the Serve return value. Blocks until the listener has
// actually bound (so callers can dial s.Addr() safely).
func runServer(t *testing.T, ctx context.Context, s *Server) <-chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Serve(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind in 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errCh
}

func TestServerPubOK(t *testing.T) {
	s, _ := setupServer(t)
	ctx := t.Context()
	errCh := runServer(t, ctx, s)

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	go func() { conn.Write([]byte("PUB orders - 5\nhello\n")) }()

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("got %q want OK <id>", line)
	}
	conn.Close()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

func TestServerShutdownDrainsClients(t *testing.T) {
	s, _ := setupServer(t)
	ctx := t.Context()
	errCh := runServer(t, ctx, s)

	var conns []net.Conn
	for i := range 3 {
		c, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
	}

	for _, c := range conns {
		c.Close()
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

func TestServerShutdownTimesOutOnSlowClient(t *testing.T) {
	s, _ := setupServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := runServer(t, ctx, s)

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Don't close the conn — session lingers; Shutdown should time out.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shutCancel()
	if err := s.Shutdown(shutCtx); err == nil {
		t.Fatal("Shutdown returned nil, expected timeout")
	} else if !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("Shutdown err = %v, want deadline exceeded", err)
	}

	cancel() // force session shutdown via Serve's ctx
	<-errCh
}

func TestServerCtxCancelExitsServe(t *testing.T) {
	s, _ := setupServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := runServer(t, ctx, s)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not exit within 2s of ctx cancel")
	}
}

func TestServerGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	b, err := broker.New(t.TempDir(), 16)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	s := New("127.0.0.1:0", b)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx) }()

	for s.Addr() == "" {
		time.Sleep(time.Millisecond)
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			c, err := net.Dial("tcp", s.Addr())
			if err != nil {
				return
			}
			fmt.Fprintf(c, "PUB orders - 2\nok\n")
			bufio.NewReader(c).ReadString('\n')
			c.Close()
		})
	}
	wg.Wait()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	shutCancel()
	cancel()
	<-errCh
	_ = b.Close()

	time.Sleep(100 * time.Millisecond)

	now := runtime.NumGoroutine()
	if now > baseline+2 {
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, now)
	}
}

// TestServerTLSRoundTrip verifies the Server terminates TLS: a client
// dialing over tls with the test root does a full PUB round-trip. Uses
// compat mode (no HELLO required) so the test focuses on the transport.
func TestServerTLSRoundTrip(t *testing.T) {
	b, err := broker.New(t.TempDir(), 16)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	certPEM, keyPEM, err := testcerts.GenerateSelfSigned("127.0.0.1")
	if err != nil {
		t.Fatalf("gen cert: %v", err)
	}
	srvTLS, err := testcerts.ServerConfig(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("server tls: %v", err)
	}
	cliTLS, err := testcerts.ClientConfig(certPEM)
	if err != nil {
		t.Fatalf("client tls: %v", err)
	}

	s := New("127.0.0.1:0", b, WithTLS(srvTLS))
	errCh := runServer(t, t.Context(), s)

	conn, err := tls.Dial("tcp", s.Addr(), cliTLS)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	go func() { conn.Write([]byte("PUB orders - 5\nhello\n")) }()

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read over tls: %v", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("tls PUB resp = %q, want OK", strings.TrimRight(line, "\r\n"))
	}

	_ = conn.Close()
	if err := s.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	<-errCh
}
