package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
)

// Client is a single TCP connection to a ToyMQ broker. It is safe
// for concurrent use: Pub calls serialize through an internal write
// mutex, and a single read goroutine fans responses back to callers.
type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	writeMu sync.Mutex

	closeOnce sync.Once
	done      chan struct{}

	readErr  error         // set by readLoop; read under writeMu
	loopDone chan struct{} // closed when readLoop returns

	pending *pendingQueue

	subMu      sync.Mutex
	subActive  bool
	consumerID string
	deliveryCh chan Delivery

	// logger is nil by default; set via WithLogger. All log call
	// sites go through Client.log so the nil-check stays in one
	// place.
	logger *slog.Logger

	// traceparentFn is nil unless WithTraceparentFunc was set. When set,
	// Pub/Sub prepend a TRACEPARENT line built from it (ADR 0026).
	traceparentFn func(context.Context) (string, string)
}

// traceparentLine returns the "TRACEPARENT ...\n" prefix line to send
// before a PUB/SUB, or "" when propagation is off or the context carries
// no active span. Kept here so the nil-check lives in one place.
func (c *Client) traceparentLine(ctx context.Context) string {
	if c.traceparentFn == nil {
		return ""
	}
	tp, ts := c.traceparentFn(ctx)
	if tp == "" {
		return ""
	}
	if ts == "" {
		return "TRACEPARENT " + tp + "\n"
	}
	return "TRACEPARENT " + tp + " TRACESTATE " + ts + "\n"
}

// log emits a record at level if a logger is configured; otherwise
// it is a no-op. Keeps the silent-by-default contract from ADR 0013
// in one place.
func (c *Client) log(level slog.Level, msg string, args ...any) {
	if c.logger == nil {
		return
	}
	c.logger.Log(context.Background(), level, msg, args...)
}

// Dial opens a TCP connection to addr and returns a ready Client.
// The provided context bounds the dial; once Dial returns, the
// context is no longer consulted.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := newConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	conn, err := dialConn(ctx, addr, cfg.tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c := &Client{
		conn:          conn,
		r:             bufio.NewReader(conn),
		w:             bufio.NewWriter(conn),
		done:          make(chan struct{}),
		pending:       newPendingQueue(),
		logger:        cfg.logger,
		traceparentFn: cfg.traceparentFn,
	}

	// HELLO handshake, synchronous and before readLoop starts (readLoop
	// owns c.r once running). See ADR 0020.
	if err := c.handshake(cfg.authToken); err != nil {
		_ = conn.Close()
		return nil, err
	}

	c.loopDone = make(chan struct{})
	go c.readLoop()
	c.log(slog.LevelDebug, "dialed", "addr", addr)
	return c, nil
}

// dialConn opens a plaintext or TLS connection depending on tlsCfg.
func dialConn(ctx context.Context, addr string, tlsCfg *tls.Config) (net.Conn, error) {
	if tlsCfg != nil {
		d := tls.Dialer{Config: tlsCfg}
		return d.DialContext(ctx, "tcp", addr)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// handshake writes "HELLO 1 [AUTH <token>]" and reads the server's
// response directly (before readLoop owns c.r). It returns ErrHandshake
// (wrapping ErrAuth on an AUTH rejection) on any failure.
func (c *Client) handshake(token string) error {
	line := "HELLO 1\n"
	if token != "" {
		line = "HELLO 1 AUTH " + token + "\n"
	}
	if _, err := c.w.WriteString(line); err != nil {
		return fmt.Errorf("%w: write HELLO: %w", ErrTransport, err)
	}
	if err := c.w.Flush(); err != nil {
		return fmt.Errorf("%w: flush HELLO: %w", ErrTransport, err)
	}

	resp, err := c.r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("%w: read HELLO response: %w", ErrTransport, err)
	}
	resp = strings.TrimRight(resp, "\r\n")

	if resp == "HELLO 1 OK" {
		return nil
	}
	// ERR <code> <reason>
	if rest, ok := strings.CutPrefix(resp, "ERR "); ok {
		code, reason, _ := strings.Cut(rest, " ")
		if code == "AUTH" {
			return fmt.Errorf("%w: %w: %s", ErrHandshake, ErrAuth, reason)
		}
		return fmt.Errorf("%w: %s %s", ErrHandshake, code, reason)
	}
	return fmt.Errorf("%w: unexpected response %q", ErrHandshake, resp)
}

// readLoop reads frames until the connection dies, then closes the
// Client. It is the only goroutine that touches c.r and the only
// goroutine that closes c.deliveryCh.
func (c *Client) readLoop() {
	defer close(c.loopDone)
	defer func() {
		c.subMu.Lock()
		if c.deliveryCh != nil {
			close(c.deliveryCh)
			c.deliveryCh = nil
		}
		c.subMu.Unlock()
	}()
	for {
		f, err := readFrame(c.r)
		if err != nil {
			c.writeMu.Lock()
			if !c.isClosed() {
				c.readErr = fmt.Errorf("%w: %w", ErrTransport, err)
				c.log(slog.LevelWarn, "transport lost", "err", err)
			}
			c.writeMu.Unlock()
			errFrame := frame{kind: frameErr, errCode: "TRANSPORT", errMsg: err.Error()}
			c.pending.drainErr(errFrame)
			_ = c.Close()
			return
		}
		c.dispatch(f)
	}
}

// dispatch routes one frame to its sink. Responses (OK/DUP/ERR) go
// to the pending FIFO; MSG frames go to the delivery channel if a
// subscription is active.
func (c *Client) dispatch(f frame) {
	switch f.kind {
	case frameOK, frameDup, frameErr:
		c.pending.deliver(f)
	case frameMsg:
		c.subMu.Lock()
		ch := c.deliveryCh
		cid := c.consumerID
		c.subMu.Unlock()
		if ch == nil {
			return
		}
		d := Delivery{
			Topic:     f.msgTopic,
			Partition: f.msgPartition,
			MsgID:     f.msgID,
			Payload:   f.payload,
			Ack:       c.makeAck(cid, f.msgPartition, f.msgID),
			Nack:      c.makeNack(cid, f.msgPartition, f.msgID),
		}
		select {
		case ch <- d:
		case <-c.done:
		}
	}
}

// Close releases the connection. Safe to call repeatedly and from
// multiple goroutines; only the first call performs work. Subsequent
// calls return nil.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
		c.log(slog.LevelDebug, "closed")
	})
	return nil
}

// Err returns the transport error that closed the Client, if any.
// Returns nil if the Client is still open or was closed by the caller.
func (c *Client) Err() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.readErr
}

// isClosed reports whether Close has been called. Non-blocking.
func (c *Client) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
