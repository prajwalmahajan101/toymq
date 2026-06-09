package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/server"
)

const (
	testDedupeCap         = 1024
	testVisibility        = 100 * time.Millisecond
	testRedeliverInterval = 20 * time.Millisecond
	testAddrPollInterval  = 2 * time.Millisecond
	testAddrPollTimeout   = time.Second
	testShutdownTimeout   = 2 * time.Second
)

// startBroker boots an in-process broker+server on 127.0.0.1:0 and
// returns the listening address. Cleanup is registered on t.
func startBroker(t *testing.T) string {
	t.Helper()

	dataDir := t.TempDir()
	b, err := broker.NewWithTimings(dataDir, testDedupeCap, testVisibility, testRedeliverInterval)
	if err != nil {
		t.Fatalf("broker.NewWithTimings: %v", err)
	}

	srv := server.New("127.0.0.1:0", b)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	addr, err := waitForAddr(srv, testAddrPollTimeout)
	if err != nil {
		cancel()
		_ = b.Close()
		t.Fatalf("server did not bind: %v", err)
	}

	t.Cleanup(func() {
		shutCtx, cancelShut := context.WithTimeout(context.Background(), testShutdownTimeout)
		defer cancelShut()
		_ = srv.Shutdown(shutCtx)
		cancel()
		<-serveErr
		_ = b.Close()
	})

	return addr
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
		time.Sleep(testAddrPollInterval)
	}
}
