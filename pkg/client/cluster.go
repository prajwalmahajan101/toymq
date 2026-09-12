package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ClusterClient is a redirect-following wrapper over a single-connection
// Client for a replicated toymq cluster (v3 M3, ADR 0032). The bare Client
// stays a dumb one-connection transport; all routing lives here.
//
// It holds one live connection to the believed leader and mirrors the
// request surface (Pub/Ack/Nack/Create/Sub). When an operation returns
// *NotLeaderError it resolves the hint to a member address — by node id when
// known, otherwise by round-robin sweep — reconnects, and replays the op,
// bounded by an exponential backoff attempt cap so a redirect storm during a
// raft election terminates with a surfaced error instead of spinning.
//
// SubStale opts into a follower-local read and does not redirect. Mid-stream
// failover (leader dies after Sub) surfaces as a closed delivery channel; the
// caller re-subscribes. Auto-resubscribe is out of scope (v4).
type ClusterClient struct {
	addrs []string          // ordered dial addresses (id prefix stripped)
	byID  map[string]string // raft node id -> dial address (from id@addr form)
	opts  []Option
	bo    *backoff
	done  chan struct{}

	mu     sync.Mutex
	cur    *Client
	idx    int
	closed bool
}

// DialCluster connects to a replicated cluster. Each member is either a plain
// "host:port" or an id-tagged "nodeID@host:port"; the id form lets a
// NOTLEADER hint (a raft node id) jump straight to the right member instead of
// sweeping. opts are the same per-connection options as Dial (auth, TLS,
// logger). It dials members in order until one connects.
func DialCluster(ctx context.Context, members []string, opts ...Option) (*ClusterClient, error) {
	if len(members) == 0 {
		return nil, errors.New("client: DialCluster needs at least one member")
	}
	cc := &ClusterClient{
		byID: make(map[string]string),
		opts: opts,
		bo:   newBackoff(20*time.Millisecond, 500*time.Millisecond, 10),
		done: make(chan struct{}),
	}
	for _, m := range members {
		id, addr := splitMember(m)
		cc.addrs = append(cc.addrs, addr)
		if id != "" {
			cc.byID[id] = addr
		}
	}
	if _, err := cc.conn(ctx); err != nil {
		return nil, err
	}
	return cc, nil
}

// splitMember parses "nodeID@host:port" into (id, addr); a plain "host:port"
// yields ("", addr). Only the first '@' splits, so IPv6 addrs without an id
// prefix are left intact.
func splitMember(m string) (id, addr string) {
	if id, addr, ok := strings.Cut(m, "@"); ok {
		return id, addr
	}
	return "", m
}

// conn returns the live connection, dialing the current address on demand.
func (cc *ClusterClient) conn(ctx context.Context) (*Client, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.closed {
		return nil, ErrClosed
	}
	if cc.cur != nil {
		return cc.cur, nil
	}
	c, err := Dial(ctx, cc.addrs[cc.idx], cc.opts...)
	if err != nil {
		return nil, err
	}
	cc.cur = c
	return c, nil
}

// redirect drops the current connection and repositions to the next target:
// the hinted node id's address when known, else the next member round-robin.
func (cc *ClusterClient) redirect(hint string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.cur != nil {
		_ = cc.cur.Close()
		cc.cur = nil
	}
	if addr, ok := cc.byID[hint]; ok && hint != "" {
		for i, a := range cc.addrs {
			if a == addr {
				cc.idx = i
				return
			}
		}
	}
	cc.idx = (cc.idx + 1) % len(cc.addrs)
}

// redirectLoop runs fn against the current leader, following NOTLEADER
// redirects and dial failures until fn succeeds, a non-redirect error
// surfaces, or the backoff attempt cap is hit.
func (cc *ClusterClient) redirectLoop(ctx context.Context, fn func(*Client) error) error {
	attempt := 0
	for {
		c, err := cc.conn(ctx)
		if err != nil {
			if errors.Is(err, ErrClosed) {
				return err // the ClusterClient itself is closed — stop.
			}
			// Dial failed (dead/unreachable member) — sweep to the next.
			attempt++
			if attempt >= cc.bo.attempts {
				return fmt.Errorf("cluster: no reachable member after %d attempts: %w", cc.bo.attempts, err)
			}
			if !cc.bo.wait(attempt, cc.done) {
				return ErrClosed
			}
			cc.redirect("")
			continue
		}
		err = fn(c)
		if err == nil {
			return nil
		}
		// Retry on a leader redirect, or when the connected node died under us
		// (transport/closed) — a mid-write leader failover. Any other error is
		// the operation's own and surfaces to the caller.
		var nl *NotLeaderError
		hint := ""
		switch {
		case errors.As(err, &nl):
			hint = nl.Hint
		case errors.Is(err, ErrTransport), errors.Is(err, ErrClosed):
			// advance to the next member
		default:
			return err
		}
		attempt++
		if attempt >= cc.bo.attempts {
			return fmt.Errorf("cluster: retry limit (%d) reached, last leader hint %q: %w", cc.bo.attempts, hint, err)
		}
		if !cc.bo.wait(attempt, cc.done) {
			return ErrClosed
		}
		cc.redirect(hint)
	}
}

// Pub publishes to the leader, following redirects. Mirrors Client.Pub.
func (cc *ClusterClient) Pub(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte) (msgID uint64, dup bool, err error) {
	return cc.PubDelay(ctx, topic, dedupeKey, routingKey, payload, 0)
}

// PubDelay is Pub with a visibility delay. Mirrors Client.PubDelay.
func (cc *ClusterClient) PubDelay(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte, delayMs uint64) (msgID uint64, dup bool, err error) {
	loopErr := cc.redirectLoop(ctx, func(c *Client) error {
		msgID, dup, err = c.PubDelay(ctx, topic, dedupeKey, routingKey, payload, delayMs)
		return err
	})
	return msgID, dup, loopErr
}

// Create creates a topic on the leader, following redirects. Mirrors Client.Create.
func (cc *ClusterClient) Create(ctx context.Context, topic string, partitions int) error {
	return cc.redirectLoop(ctx, func(c *Client) error {
		return c.Create(ctx, topic, partitions)
	})
}

// Ack acknowledges on the leader, following redirects. Mirrors Client.Ack.
func (cc *ClusterClient) Ack(ctx context.Context, consumerID string, partition int, msgID uint64) error {
	return cc.redirectLoop(ctx, func(c *Client) error {
		return c.Ack(ctx, consumerID, partition, msgID)
	})
}

// Nack negatively acknowledges on the leader, following redirects. Mirrors Client.Nack.
func (cc *ClusterClient) Nack(ctx context.Context, consumerID string, partition int, msgID uint64) error {
	return cc.redirectLoop(ctx, func(c *Client) error {
		return c.Nack(ctx, consumerID, partition, msgID)
	})
}

// Sub subscribes on the leader, following redirects (a default SUB on a
// follower returns NOTLEADER). The returned channel is bound to the leader
// connection; on leader failover it closes and the caller re-subscribes.
func (cc *ClusterClient) Sub(ctx context.Context, topic, consumerID string) (<-chan Delivery, error) {
	var ch <-chan Delivery
	err := cc.redirectLoop(ctx, func(c *Client) error {
		var e error
		ch, e = c.Sub(ctx, topic, consumerID)
		return e
	})
	return ch, err
}

// SubStale subscribes with the STALE flag against the currently-connected
// member and does NOT redirect, accepting the documented non-linearizable
// follower-local read (ADR 0032).
func (cc *ClusterClient) SubStale(ctx context.Context, topic, consumerID string) (<-chan Delivery, error) {
	c, err := cc.conn(ctx)
	if err != nil {
		return nil, err
	}
	return c.SubStale(ctx, topic, consumerID)
}

// Err reports the transport error of the live connection, if any. A closed
// delivery channel after Sub is a mid-stream leader failover when Err wraps
// ErrTransport; the caller re-subscribes (ADR 0032). Returns nil when there is
// no live connection.
func (cc *ClusterClient) Err() error {
	cc.mu.Lock()
	c := cc.cur
	cc.mu.Unlock()
	if c == nil {
		return nil
	}
	return c.Err()
}

// Close closes the live connection and aborts any in-flight backoff wait.
func (cc *ClusterClient) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.closed {
		return nil
	}
	cc.closed = true
	close(cc.done)
	if cc.cur != nil {
		err := cc.cur.Close()
		cc.cur = nil
		return err
	}
	return nil
}
