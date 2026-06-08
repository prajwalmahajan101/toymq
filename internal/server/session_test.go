package server

import (
	"bufio"
	"context"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
)

func setupBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b, err := broker.New(t.TempDir(), 16)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func runSession(t *testing.T, ctx context.Context, b *broker.Broker, serverConn net.Conn) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	sess := NewSession(serverConn, b)
	go func() {
		defer close(done)
		sess.Run(ctx)
	}()
	return done
}

func readLine(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("readLine: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

func TestSessionPubOK(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("PUB orders - 5\nhello\n"))
	}()

	line := readLine(t, br)
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("got %q, want OK <id>", line)
	}

	clientConn.Close()
	select {
	case <-sessDone:
	case <-time.After(time.Second):
		t.Fatal("session did not exit after client close")
	}
}

func TestSessionPubDup(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("PUB orders key1 5\nhello\n"))
		clientConn.Write([]byte("PUB orders key1 5\nhello\n"))
	}()

	first := readLine(t, br)
	if !strings.HasPrefix(first, "OK ") {
		t.Fatalf("first got %q, want OK <id>", first)
	}
	second := readLine(t, br)
	if !strings.HasPrefix(second, "DUP ") {
		t.Fatalf("second got %q, want DUP <id>", second)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionSubMsgAck(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)

	// Pre-publish a message directly through the broker so the SUB sees backlog.
	if _, _, err := b.Publish("orders", "", []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	go func() {
		clientConn.Write([]byte("SUB orders c1\n"))
	}()

	sub := readLine(t, br)
	if sub != "OK 0" {
		t.Fatalf("SUB resp got %q, want OK 0", sub)
	}

	// Expect MSG frame: "MSG orders <id> 2\nhi\n"
	msgHeader := readLine(t, br)
	if !strings.HasPrefix(msgHeader, "MSG orders ") {
		t.Fatalf("MSG header got %q", msgHeader)
	}
	payload := readLine(t, br)
	if payload != "hi" {
		t.Fatalf("payload got %q want hi", payload)
	}

	// ACK it.
	parts := strings.Fields(msgHeader)
	msgID := parts[2]
	go func() {
		clientConn.Write([]byte("ACK c1 " + msgID + "\n"))
	}()

	ack := readLine(t, br)
	if !strings.HasPrefix(ack, "OK "+msgID) {
		t.Fatalf("ACK resp got %q want OK %s", ack, msgID)
	}

	clientConn.Close()
	select {
	case <-sessDone:
	case <-time.After(time.Second):
		t.Fatal("session did not exit")
	}
}

func TestSessionAckBeforeSub(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("ACK c1 0\n"))
	}()
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR NO_SUB") {
		t.Fatalf("got %q want ERR NO_SUB ...", line)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionInvalidCommand(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("WAT garbage\n"))
	}()
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR INVALID") {
		t.Fatalf("got %q want ERR INVALID ...", line)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionGoroutineLeak(t *testing.T) {
	b := setupBroker(t)
	baseline := runtime.NumGoroutine()

	for range 20 {
		clientConn, serverConn := net.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		sessDone := runSession(t, ctx, b, serverConn)
		clientConn.Close()
		<-sessDone
		cancel()
	}

	// Give any laggard broker goroutines a beat to wind down.
	time.Sleep(50 * time.Millisecond)

	now := runtime.NumGoroutine()
	if now > baseline+2 {
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, now)
	}
}
