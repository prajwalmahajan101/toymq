package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Sub subscribes consumerID to topic and returns a channel of
// Deliveries. Only one subscription per Client; a second call
// returns ErrSubInUse. The channel closes when the Client closes.
func (c *Client) Sub(ctx context.Context, topic, consumerID string) (<-chan Delivery, error) {
	if c.isClosed() {
		return nil, ErrClosed
	}

	c.subMu.Lock()
	if c.subActive {
		c.subMu.Unlock()
		return nil, ErrSubInUse
	}
	c.subActive = true
	c.consumerID = consumerID
	c.deliveryCh = make(chan Delivery, 64)
	ch := c.deliveryCh
	c.subMu.Unlock()

	p := c.pending.push()

	c.writeMu.Lock()
	if c.isClosed() {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		c.rollbackSub()
		return nil, ErrClosed
	}
	if _, werr := fmt.Fprintf(c.w, "SUB %s %s\n", topic, consumerID); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		c.rollbackSub()
		return nil, fmt.Errorf("%w: write SUB: %w", ErrTransport, werr)
	}
	if werr := c.w.Flush(); werr != nil {
		c.writeMu.Unlock()
		c.pending.cancel(p)
		c.rollbackSub()
		return nil, fmt.Errorf("%w: flush SUB: %w", ErrTransport, werr)
	}
	c.writeMu.Unlock()

	select {
	case <-ctx.Done():
		c.pending.cancel(p)
		c.rollbackSub()
		return nil, ctx.Err()
	case <-c.done:
		c.rollbackSub()
		return nil, ErrClosed
	case f := <-p.resp:
		switch f.kind {
		case frameOK:
			c.log(slog.LevelDebug, "subscribed", "topic", topic, "consumer-id", consumerID)
			return ch, nil
		case frameErr:
			c.rollbackSub()
			if f.errCode == "TRANSPORT" {
				return nil, fmt.Errorf("%w: %s", ErrTransport, f.errMsg)
			}
			return nil, fmt.Errorf("%w: %s %s", ErrServer, f.errCode, f.errMsg)
		default:
			c.rollbackSub()
			return nil, errors.New("client: unexpected frame for SUB response")
		}
	}
}

func (c *Client) rollbackSub() {
	c.subMu.Lock()
	c.subActive = false
	c.consumerID = ""
	c.deliveryCh = nil
	c.subMu.Unlock()
}

// Ack sends ACK consumerID partition msgID and blocks for the broker's OK
// response. Use when the caller already knows the consumer/partition/msg-id
// triple and does not need to receive the MSG first (e.g. one-shot CLI
// tools or operator scripts). For the streaming case, Delivery.Ack is more
// convenient. MsgIDs are partition-local, so partition is required (ADR
// 0021).
func (c *Client) Ack(ctx context.Context, consumerID string, partition int, msgID uint64) error {
	return c.sendAckLike(ctx, "ACK", consumerID, partition, msgID)
}

// Nack is the negative-acknowledge counterpart of Ack.
func (c *Client) Nack(ctx context.Context, consumerID string, partition int, msgID uint64) error {
	return c.sendAckLike(ctx, "NACK", consumerID, partition, msgID)
}

func (c *Client) makeAck(consumerID string, partition int, msgID uint64) func(context.Context) error {
	return func(ctx context.Context) error { return c.Ack(ctx, consumerID, partition, msgID) }
}

func (c *Client) makeNack(consumerID string, partition int, msgID uint64) func(context.Context) error {
	return func(ctx context.Context) error { return c.Nack(ctx, consumerID, partition, msgID) }
}

func (c *Client) sendAckLike(ctx context.Context, verb, consumerID string, partition int, msgID uint64) error {
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
	if _, werr := fmt.Fprintf(c.w, "%s %s %d %d\n", verb, consumerID, partition, msgID); werr != nil {
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
			if f.okID != msgID {
				return fmt.Errorf("%w: %s id mismatch: want %d got %d", ErrServer, verb, msgID, f.okID)
			}
			return nil
		case frameErr:
			if f.errCode == "TRANSPORT" {
				return fmt.Errorf("%w: %s", ErrTransport, f.errMsg)
			}
			return fmt.Errorf("%w: %s %s", ErrServer, f.errCode, f.errMsg)
		default:
			return errors.New("client: unexpected frame for ACK/NACK response")
		}
	}
}
