//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	producerInitialBackoff = 50 * time.Millisecond
	producerMaxBackoff     = time.Second
	producerDialTimeout    = time.Second
)

// producer drives a steady PUB stream with monotonically increasing
// dedupe keys. On transport errors it reconnects and retries the
// same key, so an interrupted PUB cannot be lost from the producer's
// point of view (the broker's in-memory dedupe LRU may or may not
// remember it across a SIGKILL — see ADR 0012).
type producer struct {
	addr     string
	topic    string
	interval time.Duration
	payload  []byte
	stderr   io.Writer

	mu        sync.Mutex
	okMsgIDs  []uint64
	dupHits   int64
	retries   int64
	keysSent  int64
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

// run blocks until ctx is cancelled. The current key is retained
// across reconnects until it gets a definitive response (OK / DUP).
func (p *producer) run(ctx context.Context) {
	var (
		client  *chaosClient
		backoff = producerInitialBackoff
		nextKey int64 = 1
	)
	defer func() {
		if client != nil {
			client.close()
		}
	}()

	connect := func() bool {
		for {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			c, err := dial(p.addr, producerDialTimeout)
			if err == nil {
				client = c
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

		if client == nil && !connect() {
			return
		}

		key := fmt.Sprintf("chaos-%d", nextKey)
		if err := client.writePub(p.topic, key, p.payload); err != nil {
			client.close()
			client = nil
			p.bumpRetries()
			continue
		}

		f, err := client.readFrame()
		if err != nil {
			client.close()
			client = nil
			p.bumpRetries()
			continue
		}

		switch f.kind {
		case frameOK:
			p.recordOK(f.okID)
			nextKey++
		case frameDup:
			p.recordDup(f.dupID)
			nextKey++
		case frameErr:
			fmt.Fprintf(p.stderr, "producer: ERR %s %s\n", f.errCode, f.errMsg)
			// Advance to avoid an infinite retry on a bad key/topic.
			// In practice the broker never ERRs on chaos PUBs.
			nextKey++
		default:
			fmt.Fprintf(p.stderr, "producer: unexpected frame kind %d\n", f.kind)
			client.close()
			client = nil
		}

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

// snapshot returns a copy of the producer's recorded state. Safe to
// call after run has returned.
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
