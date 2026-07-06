package client

import (
	"context"
	"errors"
	"fmt"
)

// Pause asks the broker to suspend delivery for this connection's current
// subscription — every partition of a SUB #* (ADR 0022). It is independent
// of the automatic receive window: use it when the consumer's own
// downstream is saturated regardless of acks. A PAUSE without a prior SUB
// returns ErrServer (NO_SUB). Paused state is per-connection and does not
// survive a reconnect.
func (c *Client) Pause(ctx context.Context) error {
	return c.sendControl(ctx, "PAUSE")
}

// Resume lifts a prior Pause, letting delivery continue up to the receive
// window.
func (c *Client) Resume(ctx context.Context) error {
	return c.sendControl(ctx, "RESUME")
}

// sendControl writes an argument-less control verb and awaits its OK,
// mirroring sendAckLike's write-then-await-pending flow. The OK carries id
// 0 (the frame is not tied to a message), so the id is not checked.
func (c *Client) sendControl(ctx context.Context, verb string) error {
	if c.isClosed() {
		return ErrClosed
	}

	p := c.pending.push()

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return ErrClosed
	}
	if _, werr := fmt.Fprintf(c.w, "%s\n", verb); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return fmt.Errorf("%w: write %s: %w", ErrTransport, verb, werr)
	}
	if werr := c.w.Flush(); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return fmt.Errorf("%w: flush %s: %w", ErrTransport, verb, werr)
	}
	c.writeMu.Unlock()

	select {
	case <-ctx.Done():
		c.pending.cancel(p)
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	case f := <-p.resp:
		switch f.kind {
		case frameOK:
			return nil
		case frameErr:
			if f.errCode == "TRANSPORT" {
				return fmt.Errorf("%w: %s", ErrTransport, f.errMsg)
			}
			return fmt.Errorf("%w: %s %s", ErrServer, f.errCode, f.errMsg)
		default:
			return errors.New("client: unexpected frame for " + verb + " response")
		}
	}
}
