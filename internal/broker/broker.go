package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/replication"
	"github.com/prajwalmahajan101/toymq/internal/tracing"
	"github.com/prajwalmahajan101/toymq/internal/wal"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
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

// ErrWaitTimeout is returned by PublishCtx when a PUB … WAIT <n> <ms> barrier
// is not met within the timeout (v3 M4, ADR 0033). The write is already
// committed and quorum-durable — only the caller-requested replication factor
// was not reached in time.
var ErrWaitTimeout = errors.New("broker: replication wait timeout")

// ReplicationStatus is a point-in-time snapshot of the node's raft replication
// state, formatted by the server into an INFO replication response (v3 M4, ADR
// 0033). In standalone mode Role is "standalone" and every other field is zero.
type ReplicationStatus struct {
	Role         string // "leader" | "follower" | "candidate" | "standalone"
	LeaderID     string // best-known leader id, "" if unknown
	Term         uint64
	CommitIndex  uint64
	ApplyIndex   uint64
	LastLogIndex uint64
	Peers        []PeerStatus // populated on a leader only (MatchIndex is leader-only)
}

// PeerStatus is one follower's replication position as seen by the leader.
// LagEntries = leader LastLogIndex − this peer's MatchIndex.
type PeerStatus struct {
	ID         string
	MatchIndex uint64
	LagEntries uint64
}

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

	// raft is non-nil only in --replicate mode (v3 M1, ADR 0028/0029). When
	// set, each mutating method (PUB/ACK/NACK/CREATE) routes its command
	// through Propose→StateMachine.Apply instead of mutating inline, so every
	// cluster member applies it in the same log-index order; a PUB reads its
	// WAL-assigned MsgID back from Propose's result. nil = standalone,
	// byte-identical to v2.
	raft raft.Node

	// selfID is this node's raft NodeID, captured at AttachRaft. Needed to
	// exclude self from Status().MatchIndex, which — contrary to a first
	// reading of the toyraft docs — includes the leader's own entry (v3 M4
	// finding, TOYRAFT-MIGRATION-REPORT). Empty in standalone mode.
	selfID string

	// appliedHighWater is the highest toyraft log index whose PUB is already
	// durably recorded in a WAL (v3 M1, ADR 0028). It is computed during
	// recovery from the RaftIndex stamped in each replicated record and lets
	// ApplyPublish skip re-applying a PUB that raft replays on restart —
	// toyraft has no persistent applied index and replays the whole committed
	// log, so without this guard every replicated PUB would be written twice.
	// 0 in standalone mode (records carry no RaftIndex) and on a fresh start.
	appliedHighWater atomic.Uint64
}

// AttachRaft switches the broker into replicated mode. After this call every
// mutating command is proposed through node and applied via the state machine
// on every cluster member rather than mutating broker state inline. Called once
// at startup by cmd/toymq after raft.New — never mid-flight. selfID is this
// node's raft NodeID, used to exclude self from replication counts.
func (b *Broker) AttachRaft(node raft.Node, selfID string) {
	b.raft = node
	b.selfID = selfID
}

// LeaderHint returns the raft node's best-known current leader id, or "" when
// standalone or the leader is unknown (v3 M2). The server uses it to fill the
// NOTLEADER reason when a write lands on a follower.
func (b *Broker) LeaderHint() string {
	if b.raft == nil {
		return ""
	}
	return string(b.raft.LeaderHint())
}

// IsLeader reports whether this node may serve leader-gated operations. A
// standalone broker (raft == nil) is always its own leader, so standalone
// behaviour is unchanged. In a replicated cluster only the raft leader
// returns true; the server uses it to redirect a default SUB off a follower
// (v3 M3, ADR 0032).
func (b *Broker) IsLeader() bool {
	return b.raft == nil || b.raft.Status().Role == raft.Leader
}

// ReplicationStatus snapshots the node's raft replication state for INFO
// replication (v3 M4, ADR 0033). Standalone (raft == nil) reports
// Role "standalone" and no peers. Peers are populated only on a leader, since
// Status().MatchIndex is leader-only (nil on a follower).
func (b *Broker) ReplicationStatus() ReplicationStatus {
	if b.raft == nil {
		return ReplicationStatus{Role: "standalone"}
	}
	st := b.raft.Status()
	rs := ReplicationStatus{
		Role:         roleString(st.Role),
		LeaderID:     string(st.LeaderHint),
		Term:         uint64(st.Term),
		CommitIndex:  uint64(st.CommitIndex),
		ApplyIndex:   uint64(st.ApplyIndex),
		LastLogIndex: uint64(st.LastLogIndex),
	}
	for id, mi := range st.MatchIndex {
		if string(id) == b.selfID {
			continue // MatchIndex includes the leader's own entry; exclude it
		}
		lag := uint64(0)
		if uint64(st.LastLogIndex) > uint64(mi) {
			lag = uint64(st.LastLogIndex) - uint64(mi)
		}
		rs.Peers = append(rs.Peers, PeerStatus{
			ID:         string(id),
			MatchIndex: uint64(mi),
			LagEntries: lag,
		})
	}
	sort.Slice(rs.Peers, func(i, j int) bool { return rs.Peers[i].ID < rs.Peers[j].ID })
	return rs
}

// roleString maps a raft.Role to its INFO wire label.
func roleString(r raft.Role) string {
	switch r {
	case raft.Leader:
		return "leader"
	case raft.Follower:
		return "follower"
	case raft.Candidate:
		return "candidate"
	default:
		return "unknown"
	}
}

// NotLeaderHint reports whether err is a raft rejection the client should
// redirect around and, if so, the leader id to redirect to. It matches three
// cases (v3 M2 + M3, ADR 0032):
//   - *raft.ErrNotLeader — a write reached a follower; use its own LeaderHint.
//   - raft.ErrProposalDropped — leadership was lost mid-propose.
//   - raft.ErrStopped — the raft node is stopped/stopping (a dying leader).
//
// The latter two carry no hint, so the broker's current best guess is used;
// when that too is empty the client sweeps its member set. This lets the server
// surface NOTLEADER for any "cannot serve this write as leader" condition
// instead of a command-specific *_FAILED, so an any-node client routes around a
// failing leader.
func (b *Broker) NotLeaderHint(err error) (string, bool) {
	var nl *raft.ErrNotLeader
	if errors.As(err, &nl) {
		hint := string(nl.LeaderHint)
		if hint == "" {
			hint = b.LeaderHint()
		}
		return hint, true
	}
	if errors.Is(err, raft.ErrProposalDropped) || errors.Is(err, raft.ErrStopped) {
		return b.LeaderHint(), true
	}
	return "", false
}

// proposePublish is the replicated publish path. It resolves the clock-derived
// fields (leader-stamped, so Apply stays deterministic), Proposes the envelope,
// and reads the WAL-assigned MsgID back from Propose's result — toyraft rc.3
// returns StateMachine.Apply's value as Propose's third return, and Propose
// blocks until Apply has run on this node (ADR 0029).
func (b *Broker) proposePublish(ctx context.Context, topic string, partition int, dedupeKey string, payload []byte, delayMs uint64) (msgID uint64, dup bool, raftIndex uint64, err error) {
	now := time.Now().UnixNano()
	var visibleAtNs uint64
	if delayMs > 0 {
		visibleAtNs = uint64(now) + delayMs*uint64(time.Millisecond)
	}
	env := replication.Envelope{
		Kind:        replication.KindPublish,
		Topic:       topic,
		Partition:   int32(partition),
		DedupeKey:   dedupeKey,
		TsNs:        uint64(now),
		VisibleAtNs: visibleAtNs,
		Payload:     payload,
	}
	idx, _, res, err := b.raft.Propose(ctx, replication.Encode(env))
	if err != nil {
		return 0, false, 0, err
	}
	// KindPublish's Apply returns an ApplyResult; ACK/NACK/CREATE return nil
	// (see proposeMutation), so only the publish path asserts.
	ar := res.(replication.ApplyResult)
	return ar.MsgID, ar.Dup, uint64(idx), nil
}

// proposeMutation Proposes a result-less command (ACK/NACK/CREATE) and returns
// Propose's error. Their Apply returns nil, so the Propose result is discarded.
func (b *Broker) proposeMutation(ctx context.Context, env replication.Envelope) error {
	_, _, _, err := b.raft.Propose(ctx, replication.Encode(env))
	return err
}

// The Apply* methods below satisfy replication.ApplySurface: the state machine
// calls them from raft's single apply goroutine, in log-index order, on every
// node. They are the deterministic mutations only — no routing, no clock, no
// per-session delivery — so they produce byte-identical state everywhere.

// ApplyPublish appends one already-resolved message to topic/partition,
// stamping it with raftIndex so a restart can tell it apart from a replay.
//
// raftIndex <= appliedHighWater means this PUB was already durably written
// before the crash and toyraft is merely replaying it: skip the append (it
// would duplicate the record) and return the state unchanged. The WAL recovery
// has already restored this message and advanced nextMsgID, so nothing more is
// needed. New commits (raftIndex > appliedHighWater) append normally.
func (b *Broker) ApplyPublish(ctx context.Context, topic string, partition int, tsNs, visibleAtNs, raftIndex uint64, dedupeKey string, payload []byte) (uint64, bool, error) {
	if raftIndex != 0 && raftIndex <= b.appliedHighWater.Load() {
		return 0, false, nil
	}
	p, err := b.partitionAt(topic, partition)
	if err != nil {
		return 0, false, err
	}
	return p.applyPublish(ctx, tsNs, visibleAtNs, raftIndex, dedupeKey, payload, b.metrics)
}

// ApplyAck advances the consumer's durable offset.
func (b *Broker) ApplyAck(topic string, partition int, consumerID string, msgID uint64) error {
	return b.applyAck(topic, partition, consumerID, msgID)
}

// ApplyNack bumps attempts / dead-letters. The redelivery push is per-session
// delivery, so it is dropped here (the leader's redelivery ticker covers it).
func (b *Broker) ApplyNack(ctx context.Context, topic string, partition int, consumerID string, msgID uint64) error {
	_, err := b.applyNack(ctx, topic, partition, consumerID, msgID)
	return err
}

// ApplyCreateTopic opens a topic at an exact partition count (idempotent on
// replay). It calls the inline creator directly, never the branching
// CreateTopic, so Apply cannot recurse back into Propose.
func (b *Broker) ApplyCreateTopic(topic string, partitions int) error {
	return b.createTopicLocal(topic, partitions)
}

// advanceAppliedHighWater raises appliedHighWater to idx if idx is larger.
// Called from the recovery visitor (single-threaded at New) for every
// recovered record; a compare-and-swap loop keeps it correct even if recovery
// ever parallelises across partitions.
func (b *Broker) advanceAppliedHighWater(idx uint64) {
	for {
		cur := b.appliedHighWater.Load()
		if idx <= cur || b.appliedHighWater.CompareAndSwap(cur, idx) {
			return
		}
	}
}

// Compile-time assertion that *Broker satisfies the state machine's surface.
var _ replication.ApplySurface = (*Broker)(nil)

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
	if b.raft != nil {
		// CreateTopic has no request ctx of its own; the command carries no
		// clock, so context.Background is fine as the Propose deadline
		// boundary. Nonce 0 — the caller needs no result beyond the error.
		return b.proposeMutation(context.Background(), replication.Envelope{
			Kind: replication.KindCreateTopic, Topic: name, Partitions: int32(partitions),
		})
	}
	return b.createTopicLocal(name, partitions)
}

// createTopicLocal is the deterministic inline topic creation shared by the
// standalone CreateTopic branch and ApplyCreateTopic. It must never route
// through Propose (ApplyCreateTopic calls it from inside Apply — branching on
// b.raft here would re-propose forever). Idempotent: an existing topic at the
// same count returns nil, a different count errors.
func (b *Broker) createTopicLocal(name string, partitions int) error {
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
			// Track the highest applied raft index across all partitions so
			// ApplyPublish can skip PUBs that toyraft replays on restart (ADR
			// 0028). Records apply in strict index order under per-message (or
			// batch-ordered) fsync, so the global max is a safe prefix
			// high-water. Zero on standalone records.
			b.advanceAppliedHighWater(rec.RaftIndex)
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
	return b.PublishCtx(context.Background(), topic, dedupeKey, routingKey, partition, partitionSet, payload, 0, 0, 0)
}

// PublishCtx is Publish with a context that carries the OTel span and an
// optional delivery delay (ADR 0025): delayMs > 0 stamps the record's
// VisibleAtNs to now+delay so delivery holds it until then. The broker
// creates a "broker.publish" span when a tracer is wired; otherwise the
// span is a no-op and ctx is only used as the cancel boundary.
//
// waitReplicas > 0 (v3 M4, ADR 0033) turns the publish into a replicated
// durability barrier: after Propose commits, the leader holds the return
// until waitReplicas *followers* durably hold the write's log index or
// waitTimeout elapses. On timeout it returns the assigned MsgID with
// ErrWaitTimeout — the write is committed, only the requested replication
// factor was not reached. Ignored in standalone mode (raft == nil).
func (b *Broker) PublishCtx(ctx context.Context, topic, dedupeKey, routingKey string, partition int, partitionSet bool, payload []byte, delayMs uint64, waitReplicas int, waitTimeout time.Duration) (uint64, int, bool, error) {
	ctx, span := b.startSpan(ctx, "broker.publish",
		tracing.AttrTopic.String(topic),
		tracing.AttrPayloadBytes.Int(len(payload)),
	)
	defer span.End()

	t, err := b.getOrCreateTopic(topic)
	if err != nil {
		b.metrics.IncPublishFailure(topic)
		return 0, 0, false, err
	}
	// Routing (partition selection, incl. the keyless round-robin) is stateful
	// and non-deterministic, so the leader resolves it once here, before
	// Propose — the chosen partition then travels in the envelope and every
	// replica applies to the same one (ADR 0028).
	p, err := t.route(partition, partitionSet, routingKey)
	if err != nil {
		b.metrics.IncPublishFailure(topic)
		return 0, 0, false, err
	}
	var (
		id        uint64
		dup       bool
		raftIndex uint64
	)
	if b.raft == nil {
		id, dup, err = p.publishCtx(ctx, dedupeKey, payload, delayMs, b.metrics)
	} else {
		id, dup, raftIndex, err = b.proposePublish(ctx, topic, p.id, dedupeKey, payload, delayMs)
	}
	if err != nil {
		b.metrics.IncPublishFailure(topic)
		return id, p.id, dup, err
	}
	span.SetAttributes(tracing.AttrDuplicate.Bool(dup))
	if dup {
		b.metrics.IncPublishDup(topic)
	} else {
		b.metrics.IncPublish(topic, len(payload))
	}
	// Replication barrier (ADR 0033). Skipped for standalone, for the
	// no-barrier default (waitReplicas == 0), and for a duplicate (no new
	// entry to wait on — the original is already committed).
	if b.raft != nil && waitReplicas > 0 && !dup {
		if err := b.waitForReplication(ctx, raftIndex, waitReplicas, waitTimeout); err != nil {
			return id, p.id, dup, err
		}
	}
	return id, p.id, dup, nil
}

// waitForReplication blocks until at least n followers (the leader excludes
// itself) durably hold the entry at raftIndex, or timeout/ctx fires (v3 M4,
// ADR 0033). It polls Status().MatchIndex — leader-only, per-peer, self
// excluded — so counting peers with MatchIndex >= raftIndex is the true
// follower-ack count; it never over-reports. Returns ErrWaitTimeout when the
// bar is not met in time.
//
// ponytail: 5ms poll of MatchIndex. toyraft exposes no commit-notify hook;
// switch to it if one lands (recorded in TOYRAFT-MIGRATION-REPORT).
func (b *Broker) waitForReplication(ctx context.Context, raftIndex uint64, n int, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if b.followerAckCount(raftIndex) >= n {
			b.metrics.IncWait("satisfied")
			return nil
		}
		select {
		case <-ctx.Done():
			b.metrics.IncWait("timeout")
			return ErrWaitTimeout
		case <-deadline.C:
			b.metrics.IncWait("timeout")
			return ErrWaitTimeout
		case <-ticker.C:
		}
	}
}

// followerAckCount returns how many peers have durably acknowledged the entry
// at raftIndex, per the leader's MatchIndex snapshot (nil on a follower → 0).
func (b *Broker) followerAckCount(raftIndex uint64) int {
	st := b.raft.Status()
	acked := 0
	for id, mi := range st.MatchIndex {
		if string(id) == b.selfID {
			continue // exclude the leader's own entry (v3 M4 finding)
		}
		if uint64(mi) >= raftIndex {
			acked++
		}
	}
	return acked
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

// traceIDFromCtx returns the W3C trace-id string of the active span in
// ctx, or "" when there is none (the noop provider path). Used to attach
// exemplars to metric observations (ADR 0027).
func traceIDFromCtx(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
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
	return b.AckCtx(context.Background(), topic, partition, consumerID, msgID)
}

// AckCtx is Ack with a context carrying the caller's span. It records a
// "broker.ack" child span (no-op under the noop provider) and updates the
// ack counter and per-consumer lag gauge (ADR 0027).
func (b *Broker) AckCtx(ctx context.Context, topic string, partition int, consumerID string, msgID uint64) error {
	_, span := b.startSpan(ctx, "broker.ack",
		tracing.AttrTopic.String(topic),
		tracing.AttrConsumerID.String(consumerID),
		tracing.AttrMsgID.Int64(int64(msgID)),
	)
	defer span.End()

	if b.raft == nil {
		return b.applyAck(topic, partition, consumerID, msgID)
	}
	return b.proposeMutation(ctx, replication.Envelope{
		Kind: replication.KindAck, Topic: topic, Partition: int32(partition),
		ConsumerID: consumerID, MsgID: msgID,
	})
}

// applyAck is the deterministic state mutation of an ACK: it advances the
// consumer's durable offset (c.Ack) and refreshes the inflight/lag gauges.
// It reads no wall-clock and does no per-session delivery work, so it runs
// identically inline (standalone) and inside StateMachine.Apply on every node
// (ADR 0028). The consumer offset is failover-visible state, so ACK is a
// replicated command (spec §Command classification).
func (b *Broker) applyAck(topic string, partition int, consumerID string, msgID uint64) error {
	p, err := b.partitionAt(topic, partition)
	if err != nil {
		return err
	}
	c := p.getOrCreateConsumer(consumerID)
	if err := c.Ack(msgID); err != nil {
		return err
	}
	b.metrics.IncAck(topic, partition)
	c.mu.Lock()
	n := len(c.inflight)
	lastAcked := c.lastAcked
	c.mu.Unlock()
	b.metrics.SetInflight(topic, consumerID, n)
	b.metrics.SetConsumerLag(topic, partition, consumerID, int(p.head())-int(lastAcked))
	return nil
}

// Nack pushes a fresh Inflight snapshot back onto sendCh for
// immediate redelivery (non-blocking; the redelivery ticker covers
// the buffer-full case) and bumps Attempts.
func (b *Broker) Nack(topic string, partition int, consumerID string, msgID uint64, sendCh chan<- *Inflight) error {
	return b.NackCtx(context.Background(), topic, partition, consumerID, msgID, sendCh)
}

// NackCtx is Nack with a context carrying the caller's span. Records a
// "broker.nack" child span and bumps the nack (and, on a DLQ move, the
// dlq) counters (ADR 0027).
func (b *Broker) NackCtx(ctx context.Context, topic string, partition int, consumerID string, msgID uint64, sendCh chan<- *Inflight) error {
	ctx, span := b.startSpan(ctx, "broker.nack",
		tracing.AttrTopic.String(topic),
		tracing.AttrConsumerID.String(consumerID),
		tracing.AttrMsgID.Int64(int64(msgID)),
	)
	defer span.End()

	if b.raft != nil {
		// Replicated NACK carries only the state mutation (attempts / DLQ);
		// the immediate redelivery push is per-session and does not run here.
		// The redelivery ticker re-pushes the still-inflight message (spec
		// §Command classification; ponytail: no cross-node inflight handle in
		// M1, ticker is the backstop).
		return b.proposeMutation(ctx, replication.Envelope{
			Kind: replication.KindNack, Topic: topic, Partition: int32(partition),
			ConsumerID: consumerID, MsgID: msgID,
		})
	}

	redeliver, err := b.applyNack(ctx, topic, partition, consumerID, msgID)
	if err != nil {
		return err
	}
	// The redelivery push is per-session delivery, not replicated state: only
	// the leader that holds this consumer's connection pushes it. On a
	// follower (or replay) applyNack returns the same attempts/DLQ state
	// mutation but its redeliver is dropped here (spec §Command
	// classification). Non-blocking — the redelivery ticker covers a full
	// channel.
	if redeliver != nil {
		select {
		case sendCh <- redeliver:
		default:
		}
	}
	return nil
}

// applyNack is the deterministic state mutation of a NACK: it bumps the
// message's attempts and, once the DLQ threshold is crossed, synthetically
// acks it out of inflight and appends it to <topic>.dlq. Both are
// failover-visible state, so they run on every node inside
// StateMachine.Apply (ADR 0028). It returns the Inflight to redeliver
// (nil when the message was dead-lettered instead); the caller decides
// whether to push it — that push is local per-session delivery, never
// replicated.
func (b *Broker) applyNack(ctx context.Context, topic string, partition int, consumerID string, msgID uint64) (redeliver *Inflight, err error) {
	p, err := b.partitionAt(topic, partition)
	if err != nil {
		return nil, err
	}
	c := p.getOrCreateConsumer(consumerID)
	redeliver, killed, err := c.nackOrKill(msgID, b.dlqThreshold(topic))
	if err != nil {
		return nil, err
	}
	b.metrics.IncNack(topic, partition)
	if killed != nil {
		slog.InfoContext(ctx, "dead-lettering message",
			"topic", topic, "partition", partition, "consumer-id", consumerID,
			"msg-id", msgID, "attempts", killed.Attempts, "trigger", "nack")
		b.metrics.IncDLQ(topic, "nack")
		// Best-effort move (ADR 0024): the message was synthetically acked
		// out of the source inflight; a failed append to <topic>.dlq is
		// logged inside dlqMove, not surfaced to the client's NACK.
		_ = b.dlqMoveCtx(ctx, topic, killed.Payload)
		redeliver = nil
	}
	c.mu.Lock()
	n := len(c.inflight)
	c.mu.Unlock()
	b.metrics.SetInflight(topic, consumerID, n)
	return redeliver, nil
}
