package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/server"
)

// startAuthBroker boots a broker that requires HELLO + a bearer token.
func startAuthBroker(t *testing.T, tokens []string) string {
	t.Helper()
	b, err := broker.NewWithTimings(t.TempDir(), testDedupeCap, testVisibility, testRedeliverInterval)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	srv := server.New("127.0.0.1:0", b, server.WithRequireHello(true), server.WithTokens(tokens))
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()
	addr, err := waitForAddr(srv, testAddrPollTimeout)
	if err != nil {
		cancel()
		_ = b.Close()
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
	return addr
}

func TestPubAuthToken(t *testing.T) {
	addr := startAuthBroker(t, []string{"good-token"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	t.Run("good token succeeds", func(t *testing.T) {
		code := runPub(ctx, []string{"-addr", addr, "-auth-token", "good-token", "orders", "hi"}, io.Discard, io.Discard)
		if code != exitOK {
			t.Fatalf("pub with good token exit = %d, want %d", code, exitOK)
		}
	})

	t.Run("bad token fails", func(t *testing.T) {
		code := runPub(ctx, []string{"-addr", addr, "-auth-token", "wrong", "orders", "hi"}, io.Discard, io.Discard)
		if code != exitErr {
			t.Fatalf("pub with bad token exit = %d, want %d", code, exitErr)
		}
	})

	t.Run("no token fails", func(t *testing.T) {
		code := runPub(ctx, []string{"-addr", addr, "orders", "hi"}, io.Discard, io.Discard)
		if code != exitErr {
			t.Fatalf("pub without token exit = %d, want %d", code, exitErr)
		}
	})
}
