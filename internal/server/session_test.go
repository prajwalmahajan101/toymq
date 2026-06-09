package server

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
)

// setupBrokerWithBlockedTopic creates a broker whose dataDir already
// has a regular file where a topic directory would need to live. Any
// attempt to PUB / SUB to that topic will fail inside wal.Open's
// MkdirAll, exercising the error paths in handlePub / handleSub.
func setupBrokerWithBlockedTopic(t *testing.T, blockedName string) *broker.Broker {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "topics"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Pre-create a *file* at topics/<blockedName> so wal.Open's
	// MkdirAll fails with "not a directory".
	if err := os.WriteFile(filepath.Join(dir, "topics", blockedName), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b, err := broker.New(dir, 16)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

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

func TestSessionSubMsgNack(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)

	if _, _, err := b.Publish("orders", "", []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	go func() {
		clientConn.Write([]byte("SUB orders c1\n"))
	}()

	if sub := readLine(t, br); sub != "OK 0" {
		t.Fatalf("SUB resp got %q, want OK 0", sub)
	}

	// First delivery.
	msgHeader := readLine(t, br)
	if !strings.HasPrefix(msgHeader, "MSG orders ") {
		t.Fatalf("MSG header got %q", msgHeader)
	}
	if payload := readLine(t, br); payload != "hi" {
		t.Fatalf("payload got %q want hi", payload)
	}
	msgID := strings.Fields(msgHeader)[2]

	// NACK it. Three lines come back in some order: the redelivered
	// MSG header + payload, and the OK ack for the NACK command.
	// In practice the broker pushes the redelivery onto sendCh
	// synchronously inside handleNack before sendResp queues the OK,
	// so the writer picks the MSG up first — but the test reads all
	// three lines and asserts content rather than strict ordering.
	go func() {
		clientConn.Write([]byte("NACK c1 " + msgID + "\n"))
	}()
	lines := []string{readLine(t, br), readLine(t, br), readLine(t, br)}
	var sawMSG, sawPayload, sawOK bool
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "MSG orders "+msgID):
			sawMSG = true
		case l == "hi":
			sawPayload = true
		case strings.HasPrefix(l, "OK "+msgID):
			sawOK = true
		}
	}
	if !sawMSG || !sawPayload || !sawOK {
		t.Fatalf("expected MSG+payload+OK, got %#v", lines)
	}

	clientConn.Close()
	select {
	case <-sessDone:
	case <-time.After(time.Second):
		t.Fatal("session did not exit")
	}
}

func TestSessionNackBeforeSub(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("NACK c1 0\n"))
	}()
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR NO_SUB") {
		t.Fatalf("got %q want ERR NO_SUB ...", line)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionAckUnknownMsg(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)

	// SUB first so currentTopic is set, then ACK a msgID never delivered.
	go func() {
		clientConn.Write([]byte("SUB orders c1\n"))
		clientConn.Write([]byte("ACK c1 999\n"))
	}()
	if sub := readLine(t, br); sub != "OK 0" {
		t.Fatalf("SUB resp got %q", sub)
	}
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR ACK_FAILED") {
		t.Fatalf("got %q want ERR ACK_FAILED ...", line)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionNackUnknownMsg(t *testing.T) {
	b := setupBroker(t)
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("SUB orders c1\n"))
		clientConn.Write([]byte("NACK c1 999\n"))
	}()
	if sub := readLine(t, br); sub != "OK 0" {
		t.Fatalf("SUB resp got %q", sub)
	}
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR NACK_FAILED") {
		t.Fatalf("got %q want ERR NACK_FAILED ...", line)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionPubBrokerFailure(t *testing.T) {
	b := setupBrokerWithBlockedTopic(t, "blocked")
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("PUB blocked - 5\nhello\n"))
	}()
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR PUB_FAILED") {
		t.Fatalf("got %q want ERR PUB_FAILED ...", line)
	}

	clientConn.Close()
	<-sessDone
}

func TestSessionSubBrokerFailure(t *testing.T) {
	b := setupBrokerWithBlockedTopic(t, "blocked")
	clientConn, serverConn := net.Pipe()
	ctx := t.Context()
	sessDone := runSession(t, ctx, b, serverConn)

	br := bufio.NewReader(clientConn)
	go func() {
		clientConn.Write([]byte("SUB blocked c1\n"))
	}()
	line := readLine(t, br)
	if !strings.HasPrefix(line, "ERR SUB_FAILED") {
		t.Fatalf("got %q want ERR SUB_FAILED ...", line)
	}

	clientConn.Close()
	<-sessDone
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
