package integration

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	defaultReadTimeout = 2 * time.Second
)

type msgFrame struct {
	topic   string
	msgID   uint64
	payload []byte
}

// testClient wraps a single TCP connection to the broker. Methods
// take *testing.T so failures fail the test directly. The pending
// queue absorbs MSG frames that arrive on the wire while a test is
// waiting for an OK / DUP / ERR — without it, asynchronous deliveries
// would race with response reads.
type testClient struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	pending []msgFrame
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c := &testClient{
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}
	t.Cleanup(c.close)
	return c
}

func (c *testClient) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// ---- writes -----------------------------------------------------------------

func (c *testClient) pub(t *testing.T, topic, key string, payload []byte) {
	t.Helper()
	keyField := key
	if keyField == "" {
		keyField = "-"
	}
	if _, err := fmt.Fprintf(c.w, "PUB %s %s %d\n", topic, keyField, len(payload)); err != nil {
		t.Fatalf("write PUB header: %v", err)
	}
	if _, err := c.w.Write(payload); err != nil {
		t.Fatalf("write PUB payload: %v", err)
	}
	if err := c.w.WriteByte('\n'); err != nil {
		t.Fatalf("write PUB trailer: %v", err)
	}
	if err := c.w.Flush(); err != nil {
		t.Fatalf("flush PUB: %v", err)
	}
}

func (c *testClient) sub(t *testing.T, topic, consumerID string) {
	t.Helper()
	if _, err := fmt.Fprintf(c.w, "SUB %s %s\n", topic, consumerID); err != nil {
		t.Fatalf("write SUB: %v", err)
	}
	if err := c.w.Flush(); err != nil {
		t.Fatalf("flush SUB: %v", err)
	}
}

func (c *testClient) ack(t *testing.T, consumerID string, id uint64) {
	t.Helper()
	if _, err := fmt.Fprintf(c.w, "ACK %s %d\n", consumerID, id); err != nil {
		t.Fatalf("write ACK: %v", err)
	}
	if err := c.w.Flush(); err != nil {
		t.Fatalf("flush ACK: %v", err)
	}
}

func (c *testClient) nack(t *testing.T, consumerID string, id uint64) {
	t.Helper()
	if _, err := fmt.Fprintf(c.w, "NACK %s %d\n", consumerID, id); err != nil {
		t.Fatalf("write NACK: %v", err)
	}
	if err := c.w.Flush(); err != nil {
		t.Fatalf("flush NACK: %v", err)
	}
}

// ---- reads ------------------------------------------------------------------

// readLine reads a single line with the default deadline. MSG frames
// are NOT auto-buffered here; use expectMsg/expectOK/etc.
func (c *testClient) readLine(t *testing.T) string {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(defaultReadTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	line, err := c.r.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// readResponseLine reads lines, draining any MSG frames into pending,
// until it sees a non-MSG line.
func (c *testClient) readResponseLine(t *testing.T) string {
	t.Helper()
	for {
		line := c.readLine(t)
		if strings.HasPrefix(line, "MSG ") {
			c.absorbMsg(t, line)
			continue
		}
		return line
	}
}

func (c *testClient) absorbMsg(t *testing.T, header string) {
	t.Helper()
	frame := parseMsgHeader(t, header)
	frame.payload = c.readPayload(t, len(frame.payload))
	c.pending = append(c.pending, frame)
}

// parseMsgHeader returns a frame with topic/msgID set and payload
// sized but not yet filled (length encoded in cap len).
func parseMsgHeader(t *testing.T, line string) msgFrame {
	t.Helper()
	fields := strings.Fields(line)
	if len(fields) != 4 || fields[0] != "MSG" {
		t.Fatalf("bad MSG header %q", line)
	}
	id, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		t.Fatalf("parse MSG id %q: %v", fields[2], err)
	}
	n, err := strconv.Atoi(fields[3])
	if err != nil || n < 0 {
		t.Fatalf("parse MSG len %q: %v", fields[3], err)
	}
	return msgFrame{topic: fields[1], msgID: id, payload: make([]byte, n)}
}

func (c *testClient) readPayload(t *testing.T, n int) []byte {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(defaultReadTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		t.Fatalf("read MSG payload: %v", err)
	}
	nl, err := c.r.ReadByte()
	if err != nil || nl != '\n' {
		t.Fatalf("read MSG trailer: byte=%q err=%v", nl, err)
	}
	return buf
}

func (c *testClient) expectOK(t *testing.T) uint64 {
	t.Helper()
	line := c.readResponseLine(t)
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("expected OK, got %q", line)
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(line, "OK "), 10, 64)
	if err != nil {
		t.Fatalf("parse OK id %q: %v", line, err)
	}
	return id
}

func (c *testClient) expectDup(t *testing.T) uint64 {
	t.Helper()
	line := c.readResponseLine(t)
	if !strings.HasPrefix(line, "DUP ") {
		t.Fatalf("expected DUP, got %q", line)
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(line, "DUP "), 10, 64)
	if err != nil {
		t.Fatalf("parse DUP id %q: %v", line, err)
	}
	return id
}

// expectMsg returns the next MSG frame: drains pending first, then
// reads from the network.
func (c *testClient) expectMsg(t *testing.T) msgFrame {
	t.Helper()
	if len(c.pending) > 0 {
		head := c.pending[0]
		c.pending = c.pending[1:]
		return head
	}
	line := c.readLine(t)
	if !strings.HasPrefix(line, "MSG ") {
		t.Fatalf("expected MSG, got %q", line)
	}
	frame := parseMsgHeader(t, line)
	frame.payload = c.readPayload(t, len(frame.payload))
	return frame
}

// expectNoMsg confirms that no MSG arrives within d. Pending must
// already be empty.
func (c *testClient) expectNoMsg(t *testing.T, d time.Duration) {
	t.Helper()
	if len(c.pending) > 0 {
		t.Fatalf("expected no MSG but pending queue has %d", len(c.pending))
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	line, err := c.r.ReadString('\n')
	if err == nil {
		t.Fatalf("expected no MSG, got line %q", strings.TrimRight(line, "\r\n"))
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return
	}
	t.Fatalf("expected timeout, got %v", err)
}
