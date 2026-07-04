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
	producerInitialBackoff = 50 * time.Millisecond
	producerMaxBackoff     = time.Second
	producerDialTimeout    = time.Second
)

// producer drives a steady PUB stream with monotonically increasing
// dedupe keys. On transport errors it reconnects and retries the
// same key, so an interrupted PUB cannot be lost from the producer's
// point of view.
type producer struct {
	addr     string
	topic    string
	interval time.Duration
	payload  []byte
	stderr   io.Writer

	mu       sync.Mutex
	okMsgIDs []uint64
	dupHits  int64
	retries  int64
	keysSent int64
}

func newProducer(addr, topic string, interval time.Duration, payloadSize int, stderr io.Writer) *producer {
	return &producer{
		addr:     addr,
		topic:    topic,
		interval: interval,
		payload:  bytesOfSize(payloadSize),
		stderr:   stderr,
	}
}

func bytesOfSize(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'p'
	}
	return b
}

func (p *producer) run(ctx context.Context) {
	var (
		c       *client.Client
		backoff       = producerInitialBackoff
		nextKey int64 = 1
	)
	defer func() {
		if c != nil {
			_ = c.Close()
		}
	}()

	connect := func() bool {
		for {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			dialCtx, cancel := context.WithTimeout(ctx, producerDialTimeout)
			nc, err := client.Dial(dialCtx, p.addr)
			cancel()
			if err == nil {
				c = nc
				backoff = producerInitialBackoff
				return true
			}
			p.bumpRetries()
			select {
			case <-ctx.Done():
				return false
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > producerMaxBackoff {
				backoff = producerMaxBackoff
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if c == nil && !connect() {
			return
		}

		key := fmt.Sprintf("chaos-%d", nextKey)
		id, dup, err := c.Pub(ctx, p.topic, key, "", p.payload)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			_ = c.Close()
			c = nil
			p.bumpRetries()
			continue
		}

		if dup {
			p.recordDup(id)
		} else {
			p.recordOK(id)
		}
		nextKey++

		select {
		case <-ctx.Done():
			return
		case <-time.After(p.interval):
		}
	}
}

func (p *producer) recordOK(id uint64) {
	p.mu.Lock()
	p.okMsgIDs = append(p.okMsgIDs, id)
	p.keysSent++
	p.mu.Unlock()
}

func (p *producer) recordDup(id uint64) {
	p.mu.Lock()
	p.okMsgIDs = append(p.okMsgIDs, id)
	p.dupHits++
	p.keysSent++
	p.mu.Unlock()
}

func (p *producer) bumpRetries() {
	p.mu.Lock()
	p.retries++
	p.mu.Unlock()
}

func (p *producer) snapshot() producerStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]uint64, len(p.okMsgIDs))
	copy(ids, p.okMsgIDs)
	return producerStats{
		okMsgIDs: ids,
		keysSent: p.keysSent,
		dupHits:  p.dupHits,
		retries:  p.retries,
	}
}

type producerStats struct {
	okMsgIDs []uint64
	keysSent int64
	dupHits  int64
	retries  int64
}
