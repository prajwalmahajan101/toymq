package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/internal/testcerts"
)

// fakeHandshake accepts one connection, hands the HELLO line to onHello,
// and writes whatever onHello returns as the response.
func fakeHandshake(t *testing.T, onHello func(hello string) string) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		line, _ := br.ReadString('\n')
		_, _ = conn.Write([]byte(onHello(strings.TrimRight(line, "\r\n"))))
		// hold the conn briefly so the client reads the response
		time.Sleep(50 * time.Millisecond)
	}()
	return ln.Addr().String(), func() { _ = ln.Close(); <-done }
}

func TestDialHandshakeAuthRejected(t *testing.T) {
	addr, cleanup := fakeHandshake(t, func(string) string { return "ERR AUTH invalid or missing token\n" })
	defer cleanup()

	_, err := Dial(context.Background(), addr, WithAuth("wrong"))
	if err == nil {
		t.Fatal("Dial: expected handshake error, got nil")
	}
	if !errors.Is(err, ErrAuth) {
		t.Errorf("err = %v, want errors.Is(ErrAuth)", err)
	}
	if !errors.Is(err, ErrHandshake) {
		t.Errorf("err = %v, want errors.Is(ErrHandshake)", err)
	}
}

func TestDialSendsAuthToken(t *testing.T) {
	got := make(chan string, 1)
	addr, cleanup := fakeHandshake(t, func(hello string) string {
		got <- hello
		return "HELLO 1 OK\n"
	})
	defer cleanup()

	c, err := Dial(context.Background(), addr, WithAuth("s3cret"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	select {
	case hello := <-got:
		if hello != "HELLO 1 AUTH s3cret" {
			t.Fatalf("server saw %q, want %q", hello, "HELLO 1 AUTH s3cret")
		}
	case <-time.After(time.Second):
		t.Fatal("server never received HELLO")
	}
}

func TestDialWithTLSRoundTrip(t *testing.T) {
	b, err := broker.NewWithTimings(t.TempDir(), testDedupeCap, testVisibility, testRedeliverInterval)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	certPEM, keyPEM, err := testcerts.GenerateSelfSigned("127.0.0.1")
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	srvTLS, _ := testcerts.ServerConfig(certPEM, keyPEM)
	cliTLS, _ := testcerts.ClientConfig(certPEM)

	srv := server.New("127.0.0.1:0", b, server.WithRequireHello(true), server.WithTLS(srvTLS))
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()
	addr, err := waitForAddr(srv, testAddrPollTimeout)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	t.Cleanup(func() {
		sc, cc := context.WithTimeout(context.Background(), testShutdownTimeout)
		defer cc()
		_ = srv.Shutdown(sc)
		cancel()
		<-serveErr
		_ = b.Close()
	})

	c, err := Dial(context.Background(), addr, WithTLS(cliTLS))
	if err != nil {
		t.Fatalf("Dial TLS: %v", err)
	}
	defer c.Close()

	id, dup, err := c.Pub(context.Background(), "orders", "k1", []byte("secure"))
	if err != nil {
		t.Fatalf("Pub over TLS: %v", err)
	}
	if dup {
		t.Fatal("unexpected dup")
	}
	_ = id
}
