package broker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

type Broker struct {
	dataDir   string
	dedupeCap int

	mu     sync.RWMutex
	topics map[string]*Topic
}

func New(dataDir string, dedupeCap int) (*Broker, error) {
	b := &Broker{
		dataDir:   dataDir,
		dedupeCap: dedupeCap,
		topics:    make(map[string]*Topic),
	}

	topicsDir := filepath.Join(b.dataDir, "topics")
	entries, err := os.ReadDir(topicsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return b, nil
		}
		return nil, fmt.Errorf("read topics dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := b.getOrCreateTopic(e.Name()); err != nil {
			return nil, err
		}
	}

	return b, nil
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
