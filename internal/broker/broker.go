package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

const (
	defaultVisibilityTimeout = 30 * time.Second
	defaultRedeliverInterval = 1 * time.Second
	defaultPersistInterval   = 100 * time.Millisecond
)

// Broker is the in-process facade over the lazy topic registry. It
// owns the persist and redelivery loops and the topic-recovery walk
// performed at New. See ADR 0005.
type Broker struct {
	dataDir   string
	dedupeCap int

	visibilityTimeout time.Duration
	redeliverInterval time.Duration

	mu     sync.RWMutex
	topics map[string]*Topic

	persistCtx    context.Context
	persistCancel context.CancelFunc
	persistDone   chan struct{}

	redeliverCtx    context.Context
	redeliverCancel context.CancelFunc
	redeliverDone   chan struct{}
}

// New opens (or recovers) a Broker rooted at dataDir with per-topic
// dedupe LRU capacity dedupeCap. Uses production timings (30s
// visibility, 1s redeliver tick); tests use NewWithTimings.
func New(dataDir string, dedupeCap int) (*Broker, error) {
	return NewWithTimings(dataDir, dedupeCap, defaultVisibilityTimeout, defaultRedeliverInterval)
}

// NewWithTimings constructs a Broker with explicit visibility and
// redeliver-tick durations. Use this when integration tests need
// faster redelivery than the 30 s production default.
func NewWithTimings(dataDir string, dedupeCap int, visibility, redeliverInterval time.Duration) (*Broker, error) {
	persistCtx, persistCancel := context.WithCancel(context.Background())
	redeliverCtx, redeliverCancel := context.WithCancel(context.Background())

	b := &Broker{
		dataDir:           dataDir,
		dedupeCap:         dedupeCap,
		visibilityTimeout: visibility,
		redeliverInterval: redeliverInterval,
		topics:            make(map[string]*Topic),
		persistCtx:        persistCtx,
		persistCancel:     persistCancel,
		persistDone:       make(chan struct{}),
		redeliverCtx:      redeliverCtx,
		redeliverCancel:   redeliverCancel,
		redeliverDone:     make(chan struct{}),
	}

	topicsDir := filepath.Join(b.dataDir, "topics")
	entries, err := os.ReadDir(topicsDir)
	if err != nil && !os.IsNotExist(err) {
		persistCancel()
		redeliverCancel()
		return nil, fmt.Errorf("read topics dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		t, err := b.getOrCreateTopic(e.Name())
		if err != nil {
			persistCancel()
			redeliverCancel()
			return nil, err
		}
		if err := t.loadOffsets(b.dataDir); err != nil {
			persistCancel()
			redeliverCancel()
			return nil, fmt.Errorf("load offsets for %s: %w", e.Name(), err)
		}
	}

	go b.runPersistLoop(100 * time.Millisecond)
	go b.runRedeliverLoop(b.redeliverInterval)

	slog.Info("broker opened",
		"data-dir", b.dataDir,
		"topics-recovered", len(b.topics),
		"dedupe-cap", dedupeCap,
	)
	return b, nil
}

func (b *Broker) runPersistLoop(interval time.Duration) {
	defer close(b.persistDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flushDirty()
		case <-b.persistCtx.Done():
			b.flushDirty()
			return
		}
	}
}

func (b *Broker) flushDirty() {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, t := range topics {
		if !topicHasDirty(t) {
			continue
		}
		if err := t.flushOffsets(b.dataDir); err != nil {
			slog.Error("flush offsets", "topic", t.name, "err", err)
		}
	}
}

func topicHasDirty(t *Topic) bool {
	t.consumersMu.RLock()
	defer t.consumersMu.RUnlock()
	for _, c := range t.consumers {
		if c.persistDirty.Load() {
			return true
		}
	}
	return false
}

func (b *Broker) getOrCreateTopic(name string) (*Topic, error) {
	b.mu.RLock()
	t, ok := b.topics[name]
	b.mu.RUnlock()

	if ok {
		return t, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		return t, nil
	}

	topicDir := filepath.Join(b.dataDir, "topics", name)
	log, err := wal.Open(topicDir)
	if err != nil {
		return nil, fmt.Errorf("open wal for topic %q:%w", name, err)
	}

	t = newTopic(name, log, b.dedupeCap)
	b.topics[name] = t
	slog.Info("topic created", "topic", name)
	return t, nil
}

// Publish appends payload to topic and returns (msgID, duplicate?,
// err). A non-empty key activates dedupe: a second publish with the
// same key returns the original MsgID and duplicate=true without a
// new WAL write.
func (b *Broker) Publish(topic, key string, payload []byte) (uint64, bool, error) {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return 0, false, err
	}
	return t.Publish(key, payload)
}

// Close cancels the redelivery and persist loops (in that order so
// any Attempts bumps land in the final flush), then closes every
// open WAL. Returns the first error encountered.
func (b *Broker) Close() error {
	b.redeliverCancel()
	<-b.redeliverDone

	b.persistCancel()
	<-b.persistDone

	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, t := range b.topics {
		if err := t.log.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	slog.Info("broker closed")
	return firstErr
}

// Subscribe attaches consumerID to topic. Subsequent Inflight
// snapshots stream into sendCh until ctx cancels. A second Subscribe
// for the same consumerID detaches the previous Subscription before
// the new delivery goroutine starts.
func (b *Broker) Subscribe(ctx context.Context, topic, consumerID string, sendCh chan<- *Inflight) (*Subscription, error) {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return nil, err
	}
	return t.Subscribe(ctx, consumerID, sendCh)
}

// Ack records that consumerID successfully processed msgID on topic.
// Advances lastAcked when msgID is contiguous; otherwise records in
// aboveLast. Marks the consumer dirty for the next persist tick.
func (b *Broker) Ack(topic, consumerID string, msgID uint64) error {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return err
	}
	c := t.getOrCreateConsumer(consumerID)
	return c.Ack(msgID)
}

// Nack pushes a fresh Inflight snapshot back onto sendCh for
// immediate redelivery (non-blocking; the redelivery ticker covers
// the buffer-full case) and bumps Attempts.
func (b *Broker) Nack(topic, consumerID string, msgID uint64, sendCh chan<- *Inflight) error {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return err
	}
	c := t.getOrCreateConsumer(consumerID)
	return c.Nack(msgID, sendCh)
}
