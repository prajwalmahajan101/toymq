package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
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
}

// Dial opens a TCP connection to addr and returns a ready Client.
// The provided context bounds the dial; once Dial returns, the
// context is no longer consulted.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := newConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c := &Client{
		conn:    conn,
		r:       bufio.NewReader(conn),
		w:       bufio.NewWriter(conn),
		done:    make(chan struct{}),
		pending: newPendingQueue(),
	}

	c.loopDone = make(chan struct{})
	go c.readLoop()
	return c, nil
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
			Topic:   f.msgTopic,
			MsgID:   f.msgID,
			Payload: f.payload,
			Ack:     c.makeAck(cid, f.msgID),
			Nack:    c.makeNack(cid, f.msgID),
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
