package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// acceptOne starts a listener on 127.0.0.1:0, accepts exactly one
// connection in the background, answers the HELLO handshake (so Dial
// succeeds), and then closes it. Returns the listener address and a
// cleanup func. The buffered "HELLO 1 OK" is delivered before EOF, so
// the client's handshake read completes and readLoop then sees EOF —
// exactly the lifecycle these tests exercise.
func acceptOne(t *testing.T) (addr string, cleanup func()) {
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
		br := bufio.NewReader(conn)
		_, _ = br.ReadString('\n') // consume the client's HELLO line
		_, _ = conn.Write([]byte("HELLO 1 OK\n"))
		_ = conn.Close()
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func TestDialClose_Roundtrip(t *testing.T) {
	addr, cleanup := acceptOne(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, err := Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDial_CtxCancelled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = Dial(ctx, ln.Addr().String())
	if err == nil {
		t.Fatal("Dial: expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial: want context.Canceled, got %v", err)
	}
}

func TestDial_RefusedConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := Dial(ctx, addr); err == nil {
		t.Fatal("Dial: expected refusal error, got nil")
	}
}

func TestClose_Idempotent(t *testing.T) {
	addr, cleanup := acceptOne(t)
	defer cleanup()

	c, err := Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestClose_Concurrent(t *testing.T) {
	addr, cleanup := acceptOne(t)
	defer cleanup()

	c, err := Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
	}
	wg.Wait()

	if !c.isClosed() {
		t.Fatal("isClosed: want true after Close")
	}
}

func TestReadLoop_ExitsOnClose(t *testing.T) {
	addr, cleanup := acceptOne(t)
	defer cleanup()

	c, err := Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = c.Close()

	select {
	case <-c.loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not exit within 2s")
	}
}
