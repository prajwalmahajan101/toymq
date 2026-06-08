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

type Broker struct {
	dataDir   string
	dedupeCap int

	mu     sync.RWMutex
	topics map[string]*Topic

	persistCtx    context.Context
	persistCancel context.CancelFunc
	persistDone   chan struct{}
}

func New(dataDir string, dedupeCap int) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())

	b := &Broker{
		dataDir:       dataDir,
		dedupeCap:     dedupeCap,
		topics:        make(map[string]*Topic),
		persistCtx:    ctx,
		persistCancel: cancel,
		persistDone:   make(chan struct{}),
	}

	topicsDir := filepath.Join(b.dataDir, "topics")
	entries, err := os.ReadDir(topicsDir)
	if err != nil && !os.IsNotExist(err) {
		cancel()
		return nil, fmt.Errorf("read topics dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		t, err := b.getOrCreateTopic(e.Name())
		if err != nil {
			cancel()
			return nil, err
		}
		if err := t.loadOffsets(b.dataDir); err != nil {
			cancel()
			return nil, fmt.Errorf("load offsets for %s: %w", e.Name(), err)
		}
	}

	go b.runPersistLoop(100 * time.Millisecond)

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
	return t, nil
}

func (b *Broker) Publish(topic, key string, payload []byte) (uint64, bool, error) {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return 0, false, err
	}
	return t.Publish(key, payload)
}

func (b *Broker) Close() error {
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
	return firstErr
}

func (b *Broker) Subscribe(ctx context.Context, topic, consumerID string, sendCh chan<- *Inflight) (*Subscription, error) {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return nil, err
	}
	return t.Subscribe(ctx, consumerID, sendCh)
}

func (b *Broker) Ack(topic, consumerID string, msgID uint64) error {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return err
	}
	c := t.getOrCreateConsumer(consumerID)
	return c.Ack(msgID)
}

func (b *Broker) Nack(topic, consumerID string, msgID uint64, sendCh chan<- *Inflight) error {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return err
	}
	c := t.getOrCreateConsumer(consumerID)
	return c.Nack(msgID, sendCh)
}
