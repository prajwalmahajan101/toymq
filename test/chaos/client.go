//go:build chaos

// Package chaos hosts the long-running SIGKILL soak test for ToyMQ.
// All files are gated by the `chaos` build tag so `go test ./...`
// keeps skipping them. Run with:
//
//	go test -tags chaos -v ./test/chaos/...
//
// See docs/adr/0012-chaos-test-architecture.md for design notes.
package chaos

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	defaultReadTimeout  = 2 * time.Second
	defaultWriteTimeout = 2 * time.Second
)

// chaosClient is a thin wire-protocol wrapper used by the chaos
// producer and consumer. Unlike internal/integration's testClient,
// every method returns an error rather than failing a *testing.T —
// the chaos loops must survive transport failures and reconnect.
type chaosClient struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func dial(addr string, timeout time.Duration) (*chaosClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &chaosClient{
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}, nil
}

func (c *chaosClient) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// ---- writes -----------------------------------------------------------------

func (c *chaosClient) writePub(topic, key string, payload []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout)); err != nil {
		return err
	}
	defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()

	keyField := key
	if keyField == "" {
		keyField = "-"
	}
	if _, err := fmt.Fprintf(c.w, "PUB %s %s %d\n", topic, keyField, len(payload)); err != nil {
		return fmt.Errorf("write pub header: %w", err)
	}
	if _, err := c.w.Write(payload); err != nil {
		return fmt.Errorf("write pub payload: %w", err)
	}
	if err := c.w.WriteByte('\n'); err != nil {
		return fmt.Errorf("write pub trailer: %w", err)
	}
	return c.w.Flush()
}

func (c *chaosClient) writeSub(topic, consumerID string) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout)); err != nil {
		return err
	}
	defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()

	if _, err := fmt.Fprintf(c.w, "SUB %s %s\n", topic, consumerID); err != nil {
		return fmt.Errorf("write sub: %w", err)
	}
	return c.w.Flush()
}

func (c *chaosClient) writeAck(consumerID string, id uint64) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout)); err != nil {
		return err
	}
	defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()

	if _, err := fmt.Fprintf(c.w, "ACK %s %d\n", consumerID, id); err != nil {
		return fmt.Errorf("write ack: %w", err)
	}
	return c.w.Flush()
}

// ---- reads ------------------------------------------------------------------

// frame is the parsed result of one server-pushed line. Exactly one
// of okID, dupID, errFrame, msg is meaningful; the others are zero.
type frame struct {
	kind     frameKind
	okID     uint64
	dupID    uint64
	errCode  string
	errMsg   string
	msgTopic string
	msgID    uint64
	payload  []byte
}

type frameKind int

const (
	frameUnknown frameKind = iota
	frameOK
	frameDup
	frameErr
	frameMsg
)

// readFrame reads a single response/push frame. Returns io.EOF when
// the peer closed cleanly; any other error means the connection is
// useless.
func (c *chaosClient) readFrame() (frame, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(defaultReadTimeout)); err != nil {
		return frame{}, err
	}

	line, err := c.r.ReadString('\n')
	if err != nil {
		return frame{}, err
	}
	line = strings.TrimRight(line, "\r\n")

	switch {
	case strings.HasPrefix(line, "OK "):
		id, err := strconv.ParseUint(strings.TrimPrefix(line, "OK "), 10, 64)
		if err != nil {
			return frame{}, fmt.Errorf("parse OK id %q: %w", line, err)
		}
		return frame{kind: frameOK, okID: id}, nil

	case strings.HasPrefix(line, "DUP "):
		id, err := strconv.ParseUint(strings.TrimPrefix(line, "DUP "), 10, 64)
		if err != nil {
			return frame{}, fmt.Errorf("parse DUP id %q: %w", line, err)
		}
		return frame{kind: frameDup, dupID: id}, nil

	case strings.HasPrefix(line, "ERR "):
		rest := strings.TrimPrefix(line, "ERR ")
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) != 2 {
			return frame{}, fmt.Errorf("malformed ERR %q", line)
		}
		return frame{kind: frameErr, errCode: parts[0], errMsg: parts[1]}, nil

	case strings.HasPrefix(line, "MSG "):
		fields := strings.Fields(line)
		if len(fields) != 4 {
			return frame{}, fmt.Errorf("bad MSG header %q", line)
		}
		id, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return frame{}, fmt.Errorf("parse MSG id %q: %w", fields[2], err)
		}
		plen, err := strconv.Atoi(fields[3])
		if err != nil || plen < 0 {
			return frame{}, fmt.Errorf("parse MSG len %q: %w", fields[3], err)
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return frame{}, fmt.Errorf("read MSG payload: %w", err)
		}
		nl, err := c.r.ReadByte()
		if err != nil {
			return frame{}, fmt.Errorf("read MSG trailer: %w", err)
		}
		if nl != '\n' {
			return frame{}, errors.New("MSG trailer not newline")
		}
		return frame{kind: frameMsg, msgTopic: fields[1], msgID: id, payload: payload}, nil
	}

	return frame{}, fmt.Errorf("unknown frame %q", line)
}
