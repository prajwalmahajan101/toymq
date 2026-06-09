package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// proxyListener accepts conns, dials the real broker, pipes bytes
// both ways, and exposes Kill to sever all active links.
type proxyListener struct {
	ln       net.Listener
	upstream string

	mu     sync.Mutex
	conns  []net.Conn
	killed bool
	killCh chan struct{}
	doneCh chan struct{}
}

func startProxy(t *testing.T, upstream string) *proxyListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &proxyListener{
		ln:       ln,
		upstream: upstream,
		killCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.Kill()
	})
	return p
}

func (p *proxyListener) addr() string { return p.ln.Addr().String() }

func (p *proxyListener) Kill() {
	p.mu.Lock()
	if p.killed {
		p.mu.Unlock()
		return
	}
	p.killed = true
	close(p.killCh)
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

func (p *proxyListener) serve() {
	defer close(p.doneCh)
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, server)
		killed := p.killed
		p.mu.Unlock()
		if killed {
			_ = client.Close()
			_ = server.Close()
			continue
		}
		go func() { _, _ = io.Copy(server, client); _ = client.Close(); _ = server.Close() }()
		go func() { _, _ = io.Copy(client, server); _ = client.Close(); _ = server.Close() }()
	}
}

func TestPub_TransportFailure(t *testing.T) {
	upstream := startBroker(t)
	proxy := startProxy(t, upstream)

	c, err := Dial(context.Background(), proxy.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if _, _, err := c.Pub(context.Background(), "orders", "", []byte("ok")); err != nil {
		t.Fatalf("Pub: %v", err)
	}

	proxy.Kill()

	// Give the read loop time to notice the conn die.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.isClosed() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, _, err = c.Pub(context.Background(), "orders", "", []byte("x"))
	if err == nil {
		t.Fatal("Pub: want error after transport kill")
	}
	if !errors.Is(err, ErrTransport) && !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrTransport or ErrClosed, got %v", err)
	}

	if c.Err() == nil {
		t.Fatal("Err: want non-nil after transport kill")
	}
	if !errors.Is(c.Err(), ErrTransport) {
		t.Fatalf("Err: want ErrTransport, got %v", c.Err())
	}
}

func TestErr_NilOnCallerClose(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	_ = c.Close()
	if c.Err() != nil {
		t.Fatalf("Err: want nil on caller close, got %v", c.Err())
	}
}

func TestSecondCallAfterTransportFailure(t *testing.T) {
	upstream := startBroker(t)
	proxy := startProxy(t, upstream)

	c, _ := Dial(context.Background(), proxy.addr())
	defer c.Close()

	proxy.Kill()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.isClosed() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, _, _ = c.Pub(context.Background(), "orders", "", []byte("x"))
	_, _, err := c.Pub(context.Background(), "orders", "", []byte("x"))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("second call after blowup: want ErrClosed, got %v", err)
	}
}
