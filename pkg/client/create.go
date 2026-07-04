package client

import (
	"context"
	"errors"
	"fmt"
)

// Create sends CREATE <topic> PARTITIONS <n> and blocks for the broker's
// OK. It is idempotent: creating an existing topic with the same partition
// count succeeds; a different count returns ErrServer (ADR 0021).
func (c *Client) Create(ctx context.Context, topic string, partitions int) error {
	if c.isClosed() {
		return ErrClosed
	}
	if partitions < 1 {
		return fmt.Errorf("%w: partitions must be >= 1", ErrServer)
	}

	p := c.pending.push()

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return ErrClosed
	}
	if _, werr := fmt.Fprintf(c.w, "CREATE %s PARTITIONS %d\n", topic, partitions); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return fmt.Errorf("%w: write CREATE: %w", ErrTransport, werr)
	}
	if werr := c.w.Flush(); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return fmt.Errorf("%w: flush CREATE: %w", ErrTransport, werr)
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
			return errors.New("client: unexpected frame for CREATE response")
		}
	}
}
