package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
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
	defaultRetentionInterval = 1 * time.Second
	// defaultRecvWindow mirrors config.DefaultRecvWindow; kept here so the
	// broker package (and its tests) need not import config. Used by the
	// constructors that don't take an explicit window (ADR 0022).
	defaultRecvWindow = 256
)

// Broker is the in-process facade over the lazy topic registry. It
// owns the persist and redelivery loops and the topic-recovery walk
// performed at New. See ADR 0005.
type Broker struct {
	dataDir   string
	dedupeCap int

	// defaultPartitions is the partition count applied to a topic
	// auto-created by a first PUB/SUB (--default-partitions). An
	// existing on-disk topic keeps its recovered count; an explicit
	// CreateTopic overrides. Minimum 1 (ADR 0021).
	defaultPartitions int

	// recvWindow is the per-consumer receive window applied to every
	// partition's consumers (ADR 0022, --recv-window). Min 1.
	recvWindow int

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

	// retention bounds per-partition WAL disk use (v2 M6, ADR 0023). The
	// zero value disables both segmentation and reclaim (pre-M6
	// behaviour). The sweep loop runs only when reclaim is enabled.
	retention       RetentionConfig
	retentionCtx    context.Context
	retentionCancel context.CancelFunc
	retentionDone   chan struct{}

	// dlqAfterNacks moves a message to <topic>.dlq once it has failed this
	// many delivery attempts (v2 M6, ADR 0024). 0 disables the DLQ.
	dlqAfterNacks int

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

// RetentionConfig bounds per-partition WAL disk usage (v2 M6, ADR 0023).
// The zero value disables segmentation and reclaim, keeping the pre-M6
// single ever-growing segment. Reclaim needs SegmentBytes > 0 to have
// sealed segments to drop; the sweep loop runs only when RetainBytes or
// RetainDuration is set.
type RetentionConfig struct {
	SegmentBytes   uint64        // WAL segment size cap (wal.WithSegmentBytes); 0 = no rotation
	RetainBytes    uint64        // keep at most this many bytes per partition; 0 = unbounded
	RetainDuration time.Duration // drop segments whose newest record is older than this; 0 = unbounded
	Interval       time.Duration // sweep tick; <=0 falls back to defaultRetentionInterval
}

// reclaimEnabled reports whether any reclaim policy is active. Rotation
// alone (SegmentBytes only) does not need the sweep loop.
func (rc RetentionConfig) reclaimEnabled() bool {
	return rc.RetainBytes > 0 || rc.RetainDuration > 0
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
func NewWithObservability(dataDir string, dedupeCap, defaultPartitions, recvWindow int, visibility, redeliverInterval time.Duration, sc SyncConfig, rc RetentionConfig, dlqAfterNacks int, m *metrics.Metrics, tr trace.Tracer) (*Broker, error) {
	b, err := newBroker(dataDir, dedupeCap, defaultPartitions, recvWindow, visibility, redeliverInterval, sc, rc, dlqAfterNacks)
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
	return newBroker(dataDir, dedupeCap, 1, defaultRecvWindow, visibility, redeliverInterval, SyncConfig{}, RetentionConfig{}, 0)
}

// newBroker is the shared constructor: it applies sc to every topic log
// it recovers, so the configured fsync mode is in effect from the first
// recovered topic (not just topics created after startup). defaultPartitions
// (min 1) is the count applied to topics auto-created after startup.
func newBroker(dataDir string, dedupeCap, defaultPartitions, recvWindow int, visibility, redeliverInterval time.Duration, sc SyncConfig, rc RetentionConfig, dlqAfterNacks int) (*Broker, error) {
	if defaultPartitions < 1 {
		defaultPartitions = 1
	}
	if recvWindow < 1 {
		recvWindow = defaultRecvWindow
	}
	if rc.Interval <= 0 {
		rc.Interval = defaultRetentionInterval
	}
	persistCtx, persistCancel := context.WithCancel(context.Background())
	redeliverCtx, redeliverCancel := context.WithCancel(context.Background())
	retentionCtx, retentionCancel := context.WithCancel(context.Background())

	b := &Broker{
		dataDir:           dataDir,
		dedupeCap:         dedupeCap,
		defaultPartitions: defaultPartitions,
		recvWindow:        recvWindow,
		visibilityTimeout: visibility,
		redeliverInterval: redeliverInterval,
		retention:         rc,
		dlqAfterNacks:     dlqAfterNacks,
		sync:              sc,
		topics:            make(map[string]*Topic),
		persistCtx:        persistCtx,
		persistCancel:     persistCancel,
		persistDone:       make(chan struct{}),
		redeliverCtx:      redeliverCtx,
		redeliverCancel:   redeliverCancel,
		redeliverDone:     make(chan struct{}),
		retentionCtx:      retentionCtx,
		retentionCancel:   retentionCancel,
		retentionDone:     make(chan struct{}),
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
		// getOrCreateTopic infers the partition count from disk (meta.json
		// or flat layout) and each partition loads its own offsets on open.
		if _, err := b.getOrCreateTopic(e.Name()); err != nil {
			persistCancel()
			redeliverCancel()
			return nil, err
		}
	}

	go b.runPersistLoop(100 * time.Millisecond)
	go b.runRedeliverLoop(b.redeliverInterval)
	go b.runRetentionLoop()

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
		for _, p := range t.partitions {
			if !partitionHasDirty(p) {
				continue
			}
			err := p.flushOffsets()
			if err != nil {
				slog.Error("flush offsets", "topic", t.name, "partition", p.id, "err", err)
			}
			b.metrics.IncOffsetsFlush(t.name, err == nil)
		}
	}
}

func partitionHasDirty(p *Partition) bool {
	p.consumersMu.RLock()
	defer p.consumersMu.RUnlock()
	for _, c := range p.consumers {
		if c.persistDirty.Load() {
			return true
		}
	}
	return false
}

// getOrCreateTopic returns the topic, opening (recovering) it if absent.
// A brand-new topic gets the server default partition count; an existing
// on-disk topic keeps its recovered count.
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
	return b.openTopicLocked(name, 0)
}

// CreateTopic opens (or validates) a topic with an exact partition count.
// It is idempotent: re-creating with the same count returns nil; a
// different count is an error. Partitions must be >= 1 (ADR 0021).
func (b *Broker) CreateTopic(name string, partitions int) error {
	if partitions < 1 {
		return fmt.Errorf("create topic %q: partitions must be >= 1, got %d", name, partitions)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		if t.count() != partitions {
			return fmt.Errorf("topic %q already exists with %d partitions, not %d", name, t.count(), partitions)
		}
		return nil
	}
	_, err := b.openTopicLocked(name, partitions)
	return err
}

// openTopicLocked opens (or recovers) a topic and registers it. wantCount
// > 0 forces an exact partition count (CreateTopic); wantCount == 0 infers
// the count from disk for an existing topic, or the server default for a
// new one. The caller must hold b.mu.
func (b *Broker) openTopicLocked(name string, wantCount int) (*Topic, error) {
	topicDir := filepath.Join(b.dataDir, "topics", name)
	diskCount, exists, err := topicPartitionCount(topicDir)
	if err != nil {
		return nil, err
	}

	var count int
	switch {
	case exists && wantCount > 0 && diskCount != wantCount:
		return nil, fmt.Errorf("topic %q already exists with %d partitions, not %d", name, diskCount, wantCount)
	case exists:
		count = diskCount
	case wantCount > 0:
		count = wantCount
	default:
		count = b.defaultPartitions
	}
	if count < 1 {
		count = 1
	}

	// meta.json is written only for N>1, so a flat 1-partition topic stays
	// byte-for-byte identical to the pre-M4 layout.
	if !exists && count > 1 {
		if err := writeTopicMeta(topicDir, count); err != nil {
			return nil, err
		}
	}

	parts := make([]*Partition, count)
	for i := 0; i < count; i++ {
		dir := topicDir
		if count > 1 {
			dir = filepath.Join(topicDir, strconv.Itoa(i))
		}
		p, err := b.openPartition(name, i, dir)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = parts[j].log.Close()
			}
			return nil, err
		}
		parts[i] = p
	}

	t := newTopic(name, parts)
	b.topics[name] = t
	b.metrics.SetTopicCount(len(b.topics))
	slog.Info("topic created", "topic", name, "partitions", count)
	return t, nil
}

// openPartition opens one partition's WAL (rebuilding its dedupe LRU via
// the recovery visitor, ADR 0018) and loads its persisted offsets.
func (b *Broker) openPartition(topic string, id int, dir string) (*Partition, error) {
	dedupe := NewDedupeIndex(b.dedupeCap)
	log, err := wal.Open(dir,
		wal.WithSyncMode(b.sync.Mode, b.sync.Interval),
		wal.WithSegmentBytes(b.retention.SegmentBytes),
		wal.WithRecoveryVisitor(func(rec wal.Record) {
			rebuildIndexes(dedupe, rec)
		}))
	if err != nil {
		return nil, fmt.Errorf("open wal for topic %q partition %d: %w", topic, id, err)
	}
	p := newPartition(topic, id, dir, log, dedupe, b.recvWindow)
	if err := p.loadOffsets(); err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("load offsets for topic %q partition %d: %w", topic, id, err)
	}
	return p, nil
}

// EnsureTopic creates (or recovers) the topic if it is absent, returning
// any error from opening its WAL. It lets a caller validate a topic
// before committing to a response — e.g. the session queues SUB's OK
// only after EnsureTopic succeeds, so the OK is enqueued before
// Subscribe starts the delivery goroutine (ordering the OK ahead of the
// first MSG).
func (b *Broker) EnsureTopic(topic string) error {
	_, err := b.getOrCreateTopic(topic)
	return err
}

// TopicPartitions ensures the topic exists (creating it with the default
// count if absent) and returns its partition count. Used by the server to
// range-check a SUB's partition selector before acknowledging.
func (b *Broker) TopicPartitions(topic string) (int, error) {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return 0, err
	}
	return t.count(), nil
}

// Publish appends payload to topic and returns (msgID, partition,
// duplicate?, err). Routing (ADR 0021): an explicit partition
// (partitionSet, from PUB <topic>#<n>) wins; else a non-empty routingKey
// hashes to a partition; else the keyless publish round-robins. A
// non-empty dedupeKey activates per-partition dedupe. Equivalent to
// PublishCtx(context.Background(), ...).
func (b *Broker) Publish(topic, dedupeKey, routingKey string, partition int, partitionSet bool, payload []byte) (uint64, int, bool, error) {
	return b.PublishCtx(context.Background(), topic, dedupeKey, routingKey, partition, partitionSet, payload, 0)
}

// PublishCtx is Publish with a context that carries the OTel span and an
// optional delivery delay (ADR 0025): delayMs > 0 stamps the record's
// VisibleAtNs to now+delay so delivery holds it until then. The broker
// creates a "broker.publish" span when a tracer is wired; otherwise the
// span is a no-op and ctx is only used as the cancel boundary.
func (b *Broker) PublishCtx(ctx context.Context, topic, dedupeKey, routingKey string, partition int, partitionSet bool, payload []byte, delayMs uint64) (uint64, int, bool, error) {
	ctx, span := b.startSpan(ctx, "broker.publish",
		tracing.AttrTopic.String(topic),
		tracing.AttrPayloadBytes.Int(len(payload)),
	)
	defer span.End()

	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return 0, 0, false, err
	}
	p, err := t.route(partition, partitionSet, routingKey)
	if err != nil {
		return 0, 0, false, err
	}
	id, dup, err := p.publishCtx(ctx, dedupeKey, payload, delayMs, b.metrics)
	if err == nil {
		span.SetAttributes(tracing.AttrDuplicate.Bool(dup))
		if dup {
			b.metrics.IncPublishDup(topic)
		} else {
			b.metrics.IncPublish(topic, len(payload))
		}
	}
	return id, p.id, dup, err
}

// Close cancels the redelivery and persist loops (in that order so
// any Attempts bumps land in the final flush), then closes every
// open WAL. Returns the first error encountered.
func (b *Broker) Close() error {
	b.retentionCancel()
	<-b.retentionDone

	b.redeliverCancel()
	<-b.redeliverDone

	b.persistCancel()
	<-b.persistDone

	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, t := range b.topics {
		for _, p := range t.partitions {
			if err := p.log.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	slog.Info("broker closed")
	return firstErr
}

// Subscribe attaches consumerID to a topic selection and returns one
// Subscription per matched partition. all (SUB <topic> or <topic>#*)
// subscribes to every partition, fanning their deliveries into the single
// sendCh; otherwise the one partition is used. Each partition applies the
// single-subscription-per-consumer swap independently. Inflight snapshots
// stream into sendCh until ctx cancels.
func (b *Broker) Subscribe(ctx context.Context, topic string, partition int, all bool, consumerID string, sendCh chan<- *Inflight) ([]*Subscription, error) {
	ctx, span := b.startSpan(ctx, "broker.subscribe",
		tracing.AttrTopic.String(topic),
		tracing.AttrConsumerID.String(consumerID),
	)
	defer span.End()

	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return nil, err
	}
	parts, err := t.selectPartitions(partition, all)
	if err != nil {
		return nil, err
	}
	subs := make([]*Subscription, 0, len(parts))
	for _, p := range parts {
		sub, err := p.subscribe(ctx, consumerID, sendCh, b.metrics)
		if err != nil {
			for _, s := range subs {
				s.cancel()
				<-s.done
			}
			return nil, err
		}
		subs = append(subs, sub)
	}
	b.metrics.IncSubscribe(topic)
	return subs, nil
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

// partitionAt returns the numbered partition of topic, range-checking it.
func (b *Broker) partitionAt(topic string, partition int) (*Partition, error) {
	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		return nil, err
	}
	if partition < 0 || partition >= t.count() {
		return nil, fmt.Errorf("partition %d out of range [0,%d) for topic %q", partition, t.count(), topic)
	}
	return t.partitions[partition], nil
}

// Ack records that consumerID successfully processed (partition, msgID)
// on topic. Advances lastAcked when msgID is contiguous; otherwise records
// in aboveLast. Marks the consumer dirty for the next persist tick.
func (b *Broker) Ack(topic string, partition int, consumerID string, msgID uint64) error {
	p, err := b.partitionAt(topic, partition)
	if err != nil {
		return err
	}
	c := p.getOrCreateConsumer(consumerID)
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
func (b *Broker) Nack(topic string, partition int, consumerID string, msgID uint64, sendCh chan<- *Inflight) error {
	p, err := b.partitionAt(topic, partition)
	if err != nil {
		return err
	}
	c := p.getOrCreateConsumer(consumerID)
	redeliver, killed, err := c.nackOrKill(msgID, b.dlqThreshold(topic))
	if err != nil {
		return err
	}
	if killed != nil {
		slog.Info("dead-lettering message",
			"topic", topic, "partition", partition, "consumer-id", consumerID,
			"msg-id", msgID, "attempts", killed.Attempts, "trigger", "nack")
		// Best-effort move (ADR 0024): the message was synthetically acked
		// out of the source inflight; a failed append to <topic>.dlq is
		// logged inside dlqMove, not surfaced to the client's NACK.
		_ = b.dlqMove(topic, killed.Payload)
	} else {
		select {
		case sendCh <- redeliver:
		default:
			// channel full — the redelivery ticker covers this path.
		}
	}
	c.mu.Lock()
	n := len(c.inflight)
	c.mu.Unlock()
	b.metrics.SetInflight(topic, consumerID, n)
	return nil
}
