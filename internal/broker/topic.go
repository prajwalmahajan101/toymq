package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/wal"
)

// Subscription is the per-Subscribe handle. cancel stops the
// delivery goroutine; done closes when that goroutine exits.
type Subscription struct {
	consumerID string
	sendCh     chan<- *Inflight
	cancel     context.CancelFunc
	done       chan struct{}
}

// Topic owns the WAL, dedupe LRU, and consumer registry for one
// logical stream. Created lazily by Broker.getOrCreateTopic; never
// constructed directly outside the package.
type Topic struct {
	name   string
	log    *wal.Log
	dedupe *DedupeIndex

	pubMu sync.Mutex

	consumersMu sync.RWMutex
	consumers   map[string]*Consumer
}

func newTopic(name string, log *wal.Log, dedupe *DedupeIndex) *Topic {
	return &Topic{
		name:      name,
		log:       log,
		dedupe:    dedupe,
		consumers: make(map[string]*Consumer),
	}
}

// rebuildIndexes replays one recovered WAL record into a topic's
// in-memory indexes. It is the single materialisation seam: today it
// runs during startup recovery (via wal.WithRecoveryVisitor), and in
// v3 it is the reuse point for raft.StateMachine.Restore / snapshot
// application, where the replicated log is the source of truth (ADR
// 0018). Keep it deterministic — no wall-clock, no I/O.
func rebuildIndexes(dedupe *DedupeIndex, rec wal.Record) {
	if rec.DedupeKey != "" {
		dedupe.Insert(rec.DedupeKey, rec.MsgID)
	}
}

// Publish appends payload under the topic. A non-empty key activates
// dedupe — a second call with the same key returns the original
// MsgID and duplicate=true without a new WAL write.
func (t *Topic) Publish(key string, payload []byte) (msgID uint64, duplicate bool, err error) {
	return t.publishCtx(context.Background(), key, payload, nil)
}

// publishCtx is Publish with a context (carrying any active OTel
// span) and a *Metrics pointer for the WAL latency histogram. The
// public Publish wraps it with a Background ctx and nil metrics for
// callers that don't carry observability.
func (t *Topic) publishCtx(ctx context.Context, key string, payload []byte, m *metrics.Metrics) (msgID uint64, duplicate bool, err error) {
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

	start := time.Now()
	id, _, err := t.log.Append(rec)
	if err != nil {
		return 0, false, err
	}
	m.ObserveWALAppend(t.name, time.Since(start).Seconds())

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

func (t *Topic) runDelivery(ctx context.Context, c *Consumer, sub *Subscription, sendCh chan<- *Inflight, m *metrics.Metrics) {
	defer close(sub.done)
	defer m.DecSubs()

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
		inflightLen := len(c.inflight)
		c.mu.Unlock()
		m.SetInflight(t.name, c.ID, inflightLen)

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

// Subscribe binds consumerID to a fresh delivery goroutine that
// tails the WAL from lastAcked+1 and forwards Inflight snapshots to
// sendCh. A second Subscribe for the same consumerID detaches the
// previous Subscription before the new goroutine starts.
func (t *Topic) Subscribe(ctx context.Context, consumerID string, sendCh chan<- *Inflight) (*Subscription, error) {
	return t.subscribe(ctx, consumerID, sendCh, nil)
}

func (t *Topic) subscribe(ctx context.Context, consumerID string, sendCh chan<- *Inflight, m *metrics.Metrics) (*Subscription, error) {
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

	c.mu.Lock()
	var startID uint64
	if c.hasAcked {
		startID = c.lastAcked + 1
	}
	c.mu.Unlock()
	slog.Info("consumer subscribed",
		"topic", t.name,
		"consumer-id", consumerID,
		"from-msg-id", startID,
	)

	m.IncSubs()
	go t.runDelivery(subCtx, c, sub, sendCh, m)
	return sub, nil
}
