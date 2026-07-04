package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/tracing"
	"github.com/prajwalmahajan101/toymq/internal/wal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

	// sync selects the WAL fsync strategy applied to every topic's log
	// (ADR 0019). Zero value = SyncPerMessage, today's behaviour.
	sync SyncConfig

	// metrics and tracer are optional; nil means "observability
	// off". Helpers on *Metrics already nil-check, and the noop
	// TracerProvider returns no-op spans, so call sites stay
	// branch-free.
	metrics *metrics.Metrics
	tracer  trace.Tracer
}

// SyncConfig is the WAL durability strategy the broker applies when it
// opens each topic's log. Mode's zero value (wal.SyncPerMessage)
// preserves ADR 0002 behaviour; Interval applies only to batched.
type SyncConfig struct {
	Mode     wal.SyncMode
	Interval time.Duration
}

// New opens (or recovers) a Broker rooted at dataDir with per-topic
// dedupe LRU capacity dedupeCap. Uses production timings (30s
// visibility, 1s redeliver tick); tests use NewWithTimings.
func New(dataDir string, dedupeCap int) (*Broker, error) {
	return NewWithTimings(dataDir, dedupeCap, defaultVisibilityTimeout, defaultRedeliverInterval)
}

// NewWithObservability is NewWithTimings plus the WAL SyncConfig and
// optional metrics + tracer wiring. cmd/toymq calls this with the
// configured fsync mode and non-nil m/tr when --metrics-addr or
// --otlp-endpoint is set; tests pass a zero SyncConfig and nil m/tr and
// get the same behaviour as New.
func NewWithObservability(dataDir string, dedupeCap int, visibility, redeliverInterval time.Duration, sc SyncConfig, m *metrics.Metrics, tr trace.Tracer) (*Broker, error) {
	b, err := newBroker(dataDir, dedupeCap, visibility, redeliverInterval, sc)
	if err != nil {
		return nil, err
	}
	b.metrics = m
	b.tracer = tr
	b.metrics.SetTopicCount(len(b.topics))
	return b, nil
}

// NewWithTimings constructs a Broker with explicit visibility and
// redeliver-tick durations and the default (per-message) fsync mode.
// Use this when integration tests need faster redelivery than the 30 s
// production default.
func NewWithTimings(dataDir string, dedupeCap int, visibility, redeliverInterval time.Duration) (*Broker, error) {
	return newBroker(dataDir, dedupeCap, visibility, redeliverInterval, SyncConfig{})
}

// newBroker is the shared constructor: it applies sc to every topic log
// it recovers, so the configured fsync mode is in effect from the first
// recovered topic (not just topics created after startup).
func newBroker(dataDir string, dedupeCap int, visibility, redeliverInterval time.Duration, sc SyncConfig) (*Broker, error) {
	persistCtx, persistCancel := context.WithCancel(context.Background())
	redeliverCtx, redeliverCancel := context.WithCancel(context.Background())

	b := &Broker{
		dataDir:           dataDir,
		dedupeCap:         dedupeCap,
		visibilityTimeout: visibility,
		redeliverInterval: redeliverInterval,
		sync:              sc,
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
		err := t.flushOffsets(b.dataDir)
		if err != nil {
			slog.Error("flush offsets", "topic", t.name, "err", err)
		}
		b.metrics.IncOffsetsFlush(t.name, err == nil)
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
	dedupe := NewDedupeIndex(b.dedupeCap)
	log, err := wal.Open(topicDir,
		wal.WithSyncMode(b.sync.Mode, b.sync.Interval),
		wal.WithRecoveryVisitor(func(rec wal.Record) {
			rebuildIndexes(dedupe, rec)
		}))
	if err != nil {
		return nil, fmt.Errorf("open wal for topic %q:%w", name, err)
	}

	t = newTopic(name, log, dedupe)
	b.topics[name] = t
	b.metrics.SetTopicCount(len(b.topics))
	slog.Info("topic created", "topic", name)
	return t, nil
}

// Publish appends payload to topic and returns (msgID, duplicate?,
// err). A non-empty key activates dedupe: a second publish with the
// same key returns the original MsgID and duplicate=true without a
// new WAL write. Equivalent to PublishCtx(context.Background(), ...).
func (b *Broker) Publish(topic, key string, payload []byte) (uint64, bool, error) {
	return b.PublishCtx(context.Background(), topic, key, payload)
}

// PublishCtx is Publish with a context that carries the OTel span.
// The broker creates a "broker.publish" span when a tracer is wired;
// otherwise the span is a no-op and ctx is only used as the cancel
// boundary for future tracing additions.
func (b *Broker) PublishCtx(ctx context.Context, topic, key string, payload []byte) (uint64, bool, error) {
	ctx, span := b.startSpan(ctx, "broker.publish",
		tracing.AttrTopic.String(topic),
		tracing.AttrPayloadBytes.Int(len(payload)),
	)
	defer span.End()

	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return 0, false, err
	}
	id, dup, err := t.publishCtx(ctx, key, payload, b.metrics)
	if err == nil {
		span.SetAttributes(tracing.AttrDuplicate.Bool(dup))
		if dup {
			b.metrics.IncPublishDup(topic)
		} else {
			b.metrics.IncPublish(topic, len(payload))
		}
	}
	return id, dup, err
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
	ctx, span := b.startSpan(ctx, "broker.subscribe",
		tracing.AttrTopic.String(topic),
		tracing.AttrConsumerID.String(consumerID),
	)
	defer span.End()

	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return nil, err
	}
	sub, err := t.subscribe(ctx, consumerID, sendCh, b.metrics)
	if err == nil {
		b.metrics.IncSubscribe(topic)
	}
	return sub, err
}

// startSpan is the broker's single entry point for tracer.Start —
// keeps the nil-tracer check in one place. With the noop provider
// (when --otlp-endpoint is empty) the returned span has IsRecording
// == false and every SetAttributes/End call is a no-op.
func (b *Broker) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if b.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return b.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
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
	if err := c.Ack(msgID); err != nil {
		return err
	}
	c.mu.Lock()
	n := len(c.inflight)
	c.mu.Unlock()
	b.metrics.SetInflight(topic, consumerID, n)
	return nil
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
	if err := c.Nack(msgID, sendCh); err != nil {
		return err
	}
	c.mu.Lock()
	n := len(c.inflight)
	c.mu.Unlock()
	b.metrics.SetInflight(topic, consumerID, n)
	return nil
}
