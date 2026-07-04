package integration

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/internal/testcerts"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// securedBroker starts an in-process broker+server with the given
// handshake policy and (optionally) TLS, returning the dial address and
// a client TLS config (nil for plaintext).
func securedBroker(t *testing.T, tokens []string, useTLS bool) (addr string, cliTLS *tls.Config) {
	t.Helper()
	b, err := broker.NewWithTimings(t.TempDir(), defaultDedupeCap, defaultVisibility, defaultRedeliverInterval)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	opts := []server.Option{server.WithRequireHello(true)}
	if len(tokens) > 0 {
		opts = append(opts, server.WithTokens(tokens))
	}
	if useTLS {
		certPEM, keyPEM, err := testcerts.GenerateSelfSigned("127.0.0.1")
		if err != nil {
			t.Fatalf("cert: %v", err)
		}
		srvCfg, err := testcerts.ServerConfig(certPEM, keyPEM)
		if err != nil {
			t.Fatalf("server tls: %v", err)
		}
		cliTLS, err = testcerts.ClientConfig(certPEM)
		if err != nil {
			t.Fatalf("client tls: %v", err)
		}
		opts = append(opts, server.WithTLS(srvCfg))
	}

	srv := server.New("127.0.0.1:0", b, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	deadline := time.Now().Add(defaultAddrPollTimeout)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(defaultAddrPollInterval)
	}

	t.Cleanup(func() {
		sc, cc := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cc()
		_ = srv.Shutdown(sc)
		cancel()
		<-serveErr
		_ = b.Close()
	})
	return srv.Addr(), cliTLS
}

// TestHelloAuthTLSMatrix drives the owned risk matrix for v2 M3:
// {plain, tls} × {no-auth, auth} × {good, bad, missing token}.
func TestHelloAuthTLSMatrix(t *testing.T) {
	const goodToken = "s3cret-token"

	type expect int
	const (
		ok expect = iota
		authFail
	)

	cases := []struct {
		name   string
		useTLS bool
		authOn bool
		token  string // client token ("" = none)
		want   expect
	}{
		{"plain/no-auth", false, false, "", ok},
		{"plain/auth/good", false, true, goodToken, ok},
		{"plain/auth/bad", false, true, "wrong", authFail},
		{"plain/auth/missing", false, true, "", authFail},
		{"tls/no-auth", true, false, "", ok},
		{"tls/auth/good", true, true, goodToken, ok},
		{"tls/auth/bad", true, true, "wrong", authFail},
		{"tls/auth/missing", true, true, "", authFail},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var tokens []string
			if tc.authOn {
				tokens = []string{goodToken}
			}
			addr, cliTLS := securedBroker(t, tokens, tc.useTLS)

			var opts []client.Option
			if tc.token != "" {
				opts = append(opts, client.WithAuth(tc.token))
			}
			if tc.useTLS {
				opts = append(opts, client.WithTLS(cliTLS))
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := client.Dial(ctx, addr, opts...)

			switch tc.want {
			case ok:
				if err != nil {
					t.Fatalf("Dial: unexpected error: %v", err)
				}
				defer c.Close()
				id, dup, perr := c.Pub(ctx, "orders", "k1", []byte("payload"))
				if perr != nil {
					t.Fatalf("Pub: %v", perr)
				}
				if dup {
					t.Fatal("unexpected dup")
				}
				_ = id
			case authFail:
				if err == nil {
					c.Close()
					t.Fatal("Dial: expected auth failure, got success")
				}
				if !errors.Is(err, client.ErrAuth) {
					t.Fatalf("Dial err = %v, want errors.Is(ErrAuth)", err)
				}
			}
		})
	}
}

// TestRequireHelloFalseCompat proves the migration escape hatch: with
// require-hello off, an un-migrated raw-line client that skips HELLO and
// sends PUB directly still works.
func TestRequireHelloFalseCompat(t *testing.T) {
	b, err := broker.NewWithTimings(t.TempDir(), defaultDedupeCap, defaultVisibility, defaultRedeliverInterval)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	srv := server.New("127.0.0.1:0", b, server.WithRequireHello(false))
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()
	deadline := time.Now().Add(defaultAddrPollTimeout)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("bind")
		}
		time.Sleep(defaultAddrPollInterval)
	}
	t.Cleanup(func() {
		sc, cc := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cc()
		_ = srv.Shutdown(sc)
		cancel()
		<-serveErr
		_ = b.Close()
	})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// No HELLO — straight to PUB, the pre-M3 wire.
	go func() { conn.Write([]byte("PUB orders - 5\nhello\n")) }()
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("compat raw PUB resp = %q, want OK", strings.TrimRight(line, "\r\n"))
	}
}
