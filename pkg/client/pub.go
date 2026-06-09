package client

import (
	"context"
	"errors"
	"fmt"
)

// Pub publishes payload to topic. If dedupeKey is non-empty and the
// broker has seen it before, dup is true and msgID echoes the prior
// OK. Otherwise msgID is the freshly assigned id.
func (c *Client) Pub(ctx context.Context, topic, dedupeKey string, payload []byte) (msgID uint64, dup bool, err error) {
	if c.isClosed() {
		return 0, false, ErrClosed
	}

	key := dedupeKey
	if key == "" {
		key = "-"
	}

	p := c.pending.push()

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return 0, false, ErrClosed
	}
	if _, werr := fmt.Fprintf(c.w, "PUB %s %s %d\n", topic, key, len(payload)); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return 0, false, fmt.Errorf("%w: write PUB header: %w", ErrTransport, werr)
	}
	if _, werr := c.w.Write(payload); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return 0, false, fmt.Errorf("%w: write PUB payload: %w", ErrTransport, werr)
	}
	if werr := c.w.WriteByte('\n'); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return 0, false, fmt.Errorf("%w: write PUB trailer: %w", ErrTransport, werr)
	}
	if werr := c.w.Flush(); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return 0, false, fmt.Errorf("%w: flush PUB: %w", ErrTransport, werr)
	}
	c.writeMu.Unlock()

	select {
	case <-ctx.Done():
		c.pending.cancel(p)
		return 0, false, ctx.Err()
	case <-c.done:
		return 0, false, ErrClosed
	case f := <-p.resp:
		return resolvePubResp(f)
	}
}

func resolvePubResp(f frame) (uint64, bool, error) {
	switch f.kind {
	case frameOK:
		return f.okID, false, nil
	case frameDup:
		return f.dupID, true, nil
	case frameErr:
		if f.errCode == "TRANSPORT" {
			return 0, false, fmt.Errorf("%w: %s", ErrTransport, f.errMsg)
		}
		return 0, false, fmt.Errorf("%w: %s %s", ErrServer, f.errCode, f.errMsg)
	}
	return 0, false, errors.New("client: unexpected frame for PUB response")
}
