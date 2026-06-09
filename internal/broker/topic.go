package broker

import (
	"context"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

type Subscription struct {
	consumerID string
	sendCh     chan<- *Inflight
	cancel     context.CancelFunc
	done       chan struct{}
}

type Topic struct {
	name   string
	log    *wal.Log
	dedupe *DedupeIndex

	pubMu sync.Mutex

	consumersMu sync.RWMutex
	consumers   map[string]*Consumer
}

func newTopic(name string, log *wal.Log, dedupeCap int) *Topic {
	return &Topic{
		name:      name,
		log:       log,
		dedupe:    NewDedupeIndex(dedupeCap),
		consumers: make(map[string]*Consumer),
	}
}

func (t *Topic) Publish(key string, payload []byte) (msgID uint64, duplicate bool, err error) {
	t.pubMu.Lock()
	defer t.pubMu.Unlock()

	if key != "" {
		if existing, ok := t.dedupe.Lookup(key); ok {
			return existing, true, nil
		}
	}

	rec := wal.Record{
		TsNs:      uint64(time.Now().UnixNano()),
		DedupeKey: key,
		Payload:   payload,
	}

	id, _, err := t.log.Append(rec)
	if err != nil {
		return 0, false, err
	}
	if key != "" {
		t.dedupe.Insert(key, id)
	}

	return id, false, nil
}

func (t *Topic) getOrCreateConsumer(id string) *Consumer {
	t.consumersMu.RLock()
	c, ok := t.consumers[id]
	t.consumersMu.RUnlock()

	if ok {
		return c
	}

	t.consumersMu.Lock()
	defer t.consumersMu.Unlock()
	if c, ok := t.consumers[id]; ok {
		return c
	}
	c = newConsumer(id, t)
	t.consumers[id] = c
	return c
}

func (t *Topic) runDelivery(ctx context.Context, c *Consumer, sub *Subscription, sendCh chan<- *Inflight) {
	defer close(sub.done)

	c.mu.Lock()
	var startID uint64
	if c.hasAcked {
		startID = c.lastAcked + 1
	}
	// fresh consumer (never acked) starts at MsgID 0 regardless of
	// any prior aboveLast entries — those came from out-of-order
	// acks above lastAcked and don't shift the start point.
	c.mu.Unlock()

	reader, err := t.log.NewReader(startID)
	if err != nil {
		return
	}
	defer reader.Close()

	for {
		rec, err := reader.Next(ctx)
		if err != nil {
			return
		}

		inf := &Inflight{
			MsgID:       rec.MsgID,
			Topic:       t.name,
			Payload:     rec.Payload,
			DeliveredAt: time.Now(),
			Attempts:    1,
		}

		c.mu.Lock()
		c.inflight[rec.MsgID] = inf
		snapshot := *inf
		c.mu.Unlock()

		select {
		case sendCh <- &snapshot:
		case <-ctx.Done():
			// rollback: if we never delivered, drop the inflight entry
			c.mu.Lock()
			delete(c.inflight, rec.MsgID)
			c.mu.Unlock()
			return
		}
	}
}

func (t *Topic) Subscribe(ctx context.Context, consumerID string, sendCh chan<- *Inflight) (*Subscription, error) {
	c := t.getOrCreateConsumer(consumerID)

	subCtx, cancel := context.WithCancel(ctx)

	sub := &Subscription{
		consumerID: consumerID,
		sendCh:     sendCh,
		cancel:     cancel,
		done:       make(chan struct{}),
	}

	c.mu.Lock()
	prev := c.sub
	c.sub = sub
	c.mu.Unlock()

	if prev != nil {
		prev.cancel()
		<-prev.done
	}
	go t.runDelivery(subCtx, c, sub, sendCh)
	return sub, nil
}
