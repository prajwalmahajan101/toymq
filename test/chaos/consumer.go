//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	consumerInitialBackoff = 50 * time.Millisecond
	consumerMaxBackoff     = time.Second
	consumerDialTimeout    = time.Second
)

// consumer subscribes with a stable ID and ACKs every MSG it
// receives. On transport failure it reconnects and re-SUBs; the
// broker replays from lastAcked+1.
type consumer struct {
	addr       string
	topic      string
	consumerID string
	stderr     io.Writer

	mu      sync.Mutex
	seen    map[uint64]int // MsgID -> delivery count
	errors  int64
	resubs  int64
}

func newConsumer(addr, topic, consumerID string, stderr io.Writer) *consumer {
	return &consumer{
		addr:       addr,
		topic:      topic,
		consumerID: consumerID,
		stderr:     stderr,
		seen:       make(map[uint64]int),
	}
}

func (c *consumer) run(ctx context.Context) {
	var (
		client  *chaosClient
		backoff = consumerInitialBackoff
	)
	defer func() {
		if client != nil {
			client.close()
		}
	}()

	connectAndSub := func() bool {
		for {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			nc, err := dial(c.addr, consumerDialTimeout)
			if err == nil {
				if err := nc.writeSub(c.topic, c.consumerID); err == nil {
					f, err := nc.readFrame()
					if err == nil && f.kind == frameOK {
						client = nc
						backoff = consumerInitialBackoff
						c.bumpResubs()
						return true
					}
				}
				nc.close()
			}
			c.bumpErrors()
			select {
			case <-ctx.Done():
				return false
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > consumerMaxBackoff {
				backoff = consumerMaxBackoff
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if client == nil && !connectAndSub() {
			return
		}

		f, err := client.readFrame()
		if err != nil {
			// Timeout is expected during the gap between SIGKILL and
			// the next message — just retry the read on the same conn
			// if it's still alive; otherwise reconnect.
			if isTimeout(err) {
				continue
			}
			client.close()
			client = nil
			c.bumpErrors()
			continue
		}

		switch f.kind {
		case frameMsg:
			c.recordDelivery(f.msgID)
			if err := client.writeAck(c.consumerID, f.msgID); err != nil {
				client.close()
				client = nil
				c.bumpErrors()
				continue
			}
			// Drain the ACK's OK response.
			ackResp, err := client.readFrame()
			if err != nil || ackResp.kind != frameOK {
				if err != nil && !isTimeout(err) {
					client.close()
					client = nil
					c.bumpErrors()
				}
			}
		default:
			// Anything other than MSG on a steady-state subscription
			// means we're out of sync.
			fmt.Fprintf(c.stderr, "consumer: unexpected frame kind %d\n", f.kind)
			client.close()
			client = nil
		}
	}
}

func isTimeout(err error) bool {
	type timeout interface{ Timeout() bool }
	var te timeout
	if errors.As(err, &te) {
		return te.Timeout()
	}
	return false
}

func (c *consumer) recordDelivery(id uint64) {
	c.mu.Lock()
	c.seen[id]++
	c.mu.Unlock()
}

func (c *consumer) bumpErrors() {
	c.mu.Lock()
	c.errors++
	c.mu.Unlock()
}

func (c *consumer) bumpResubs() {
	c.mu.Lock()
	c.resubs++
	c.mu.Unlock()
}

// snapshot returns a copy of the consumer's recorded state. Safe to
// call after run has returned.
func (c *consumer) snapshot() consumerStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make(map[uint64]int, len(c.seen))
	for k, v := range c.seen {
		seen[k] = v
	}
	return consumerStats{
		seen:   seen,
		errors: c.errors,
		resubs: c.resubs,
	}
}

type consumerStats struct {
	seen   map[uint64]int
	errors int64
	resubs int64
}
