package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ReplicationInfo is the parsed INFO replication response (v3 M4, ADR 0033).
// Raw holds every key:value line verbatim so callers (e.g. the TUI) can read
// fields this struct does not model; the typed fields cover the common ones.
type ReplicationInfo struct {
	Role string // "leader" | "follower" | "candidate" | "standalone"
	Raw  map[string]string
}

// Info queries the broker's replication state (v3 M4, ADR 0033). It is a
// local read, served on any node without a leader redirect. In standalone
// mode Role is "standalone" and only that key is present.
func (c *Client) Info(ctx context.Context) (ReplicationInfo, error) {
	if c.isClosed() {
		return ReplicationInfo{}, ErrClosed
	}

	p := c.pending.push()

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return ReplicationInfo{}, ErrClosed
	}
	if _, werr := c.w.WriteString("INFO replication\n"); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return ReplicationInfo{}, fmt.Errorf("%w: write INFO: %w", ErrTransport, werr)
	}
	if werr := c.w.Flush(); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return ReplicationInfo{}, fmt.Errorf("%w: flush INFO: %w", ErrTransport, werr)
	}
	c.writeMu.Unlock()

	select {
	case <-ctx.Done():
		c.pending.cancel(p)
		return ReplicationInfo{}, ctx.Err()
	case <-c.done:
		return ReplicationInfo{}, ErrClosed
	case f := <-p.resp:
		return resolveInfoResp(f)
	}
}

func resolveInfoResp(f frame) (ReplicationInfo, error) {
	switch f.kind {
	case frameInfo:
		raw := make(map[string]string, len(f.infoLines))
		for _, l := range f.infoLines {
			k, v, ok := strings.Cut(l, ":")
			if !ok {
				return ReplicationInfo{}, fmt.Errorf("client: malformed INFO line %q", l)
			}
			raw[k] = v
		}
		return ReplicationInfo{Role: raw["role"], Raw: raw}, nil
	case frameErr:
		return ReplicationInfo{}, serverErr(f)
	}
	return ReplicationInfo{}, errors.New("client: unexpected frame for INFO response")
}
