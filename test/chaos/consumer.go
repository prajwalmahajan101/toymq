//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toymq/pkg/client"
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

	mu     sync.Mutex
	seen   map[uint64]int
	errors int64
	resubs int64
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
		cli     *client.Client
		ch      <-chan client.Delivery
		backoff = consumerInitialBackoff
	)
	defer func() {
		if cli != nil {
			_ = cli.Close()
		}
	}()

	connectAndSub := func() bool {
		for {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			dialCtx, cancel := context.WithTimeout(ctx, consumerDialTimeout)
			nc, err := client.Dial(dialCtx, c.addr)
			cancel()
			if err == nil {
				dch, err := nc.Sub(ctx, c.topic, c.consumerID)
				if err == nil {
					cli = nc
					ch = dch
					backoff = consumerInitialBackoff
					c.bumpResubs()
					return true
				}
				_ = nc.Close()
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

		if cli == nil && !connectAndSub() {
			return
		}

		select {
		case <-ctx.Done():
			return
		case d, ok := <-ch:
			if !ok {
				// Client closed (transport failure or our own close).
				_ = cli.Close()
				cli = nil
				ch = nil
				c.bumpErrors()
				continue
			}
			c.recordDelivery(d.MsgID)
			if err := d.Ack(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				fmt.Fprintf(c.stderr, "consumer: ack: %v\n", err)
				_ = cli.Close()
				cli = nil
				ch = nil
				c.bumpErrors()
			}
		}
	}
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
