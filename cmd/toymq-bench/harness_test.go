package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/internal/testcerts"
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

// startBrokerTLS boots an in-process broker+server terminating TLS with a
// self-signed 127.0.0.1 cert and mandating the HELLO handshake. It returns
// the listening address and the path to a PEM CA file the bench can pass via
// --tls-ca. Cleanup is registered on t.
func startBrokerTLS(t *testing.T) (addr, caFile string) {
	t.Helper()

	dataDir := t.TempDir()
	b, err := broker.NewWithTimings(dataDir, testDedupeCap, testVisibility, testRedeliverInterval)
	if err != nil {
		t.Fatalf("broker.NewWithTimings: %v", err)
	}

	certPEM, keyPEM, err := testcerts.GenerateSelfSigned("127.0.0.1")
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	srvCfg, err := testcerts.ServerConfig(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("server tls: %v", err)
	}
	// The self-signed cert is its own CA; write it so the bench can trust it.
	caFile = filepath.Join(t.TempDir(), "ca.pem")
	if werr := os.WriteFile(caFile, certPEM, 0o600); werr != nil {
		t.Fatalf("write ca: %v", werr)
	}

	srv := server.New("127.0.0.1:0", b, server.WithRequireHello(true), server.WithTLS(srvCfg))
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	addr, err = waitForAddr(srv, testAddrPollTimeout)
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

	return addr, caFile
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
