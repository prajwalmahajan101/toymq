package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Pub publishes payload to topic. If dedupeKey is non-empty and the
// broker has seen it before, dup is true and msgID echoes the prior OK;
// otherwise msgID is the freshly assigned (partition-local) id. routingKey
// selects the partition by hash when non-empty; empty round-robins. To pin
// a partition explicitly, pass topic as "<topic>#<n>" (ADR 0021).
func (c *Client) Pub(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte) (msgID uint64, dup bool, err error) {
	return c.PubDelay(ctx, topic, dedupeKey, routingKey, payload, 0)
}

// PubDelay is Pub with a delivery delay: the broker holds the message
// from delivery for delayMs milliseconds, then delivers it in normal
// per-partition order (ADR 0025). delayMs == 0 is identical to Pub.
func (c *Client) PubDelay(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte, delayMs uint64) (msgID uint64, dup bool, err error) {
	return c.pubFull(ctx, topic, dedupeKey, routingKey, payload, delayMs, 0, 0)
}

// PubWait is Pub with a replication barrier (v3 M4, ADR 0033): the leader
// returns OK only after waitReplicas followers durably hold the write's log
// index, or waitTimeoutMs elapses. On timeout it returns a *WaitTimeoutError
// carrying the assigned MsgID — the write is committed and quorum-durable,
// only the requested replication factor was not reached in time. waitReplicas
// == 0 is identical to Pub (leader-only).
func (c *Client) PubWait(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte, waitReplicas int, waitTimeoutMs uint64) (msgID uint64, dup bool, err error) {
	return c.pubFull(ctx, topic, dedupeKey, routingKey, payload, 0, waitReplicas, waitTimeoutMs)
}

// pubFull is the shared PUB write path carrying the optional DELAY and WAIT
// trailing tokens.
func (c *Client) pubFull(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte, delayMs uint64, waitReplicas int, waitTimeoutMs uint64) (msgID uint64, dup bool, err error) {
	if c.isClosed() {
		return 0, false, ErrClosed
	}

	key := dedupeKey
	if key == "" {
		key = "-"
	}
	rkey := routingKey
	if rkey == "" {
		rkey = "-"
	}

	header := fmt.Sprintf("PUB %s %s %s %d", topic, key, rkey, len(payload))
	if delayMs > 0 {
		header += fmt.Sprintf(" DELAY %d", delayMs)
	}
	if waitReplicas > 0 {
		header += fmt.Sprintf(" WAIT %d %d", waitReplicas, waitTimeoutMs)
	}
	header += "\n"

	traceLine := c.traceparentLine(ctx)

	p := c.pending.push()

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		return 0, false, ErrClosed
	}
	if traceLine != "" {
		if _, werr := c.w.WriteString(traceLine); werr != nil {
			c.writeMu.Unlock()
			c.pending.cancel(p)
			return 0, false, fmt.Errorf("%w: write TRACEPARENT: %w", ErrTransport, werr)
		}
	}
	if _, werr := c.w.WriteString(header); werr != nil {
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
		// WAIT_TIMEOUT carries the assigned MsgID (the write landed); surface
		// it as a typed error so callers can recover the id (ADR 0033).
		if f.errCode == "WAIT_TIMEOUT" {
			id, perr := strconv.ParseUint(strings.TrimSpace(f.errMsg), 10, 64)
			if perr != nil {
				return 0, false, fmt.Errorf("client: malformed WAIT_TIMEOUT id %q: %w", f.errMsg, perr)
			}
			return id, false, &WaitTimeoutError{MsgID: id}
		}
		return 0, false, serverErr(f)
	}
	return 0, false, errors.New("client: unexpected frame for PUB response")
}
