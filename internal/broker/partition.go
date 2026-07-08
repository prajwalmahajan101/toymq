package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/wal"
)

// Subscription is the per-Subscribe handle. cancel stops the
// delivery goroutine; done closes when that goroutine exits. consumer is
// the bound Consumer, exposed to the session (via SetPaused) so PAUSE/RESUME
// can reach exactly the partitions this SUB covers without the session
// touching the consumer registry (ADR 0022).
type Subscription struct {
	consumerID string
	consumer   *Consumer
	sendCh     chan<- *Inflight
	cancel     context.CancelFunc
	done       chan struct{}
}

// SetPaused suspends (true) or resumes (false) delivery for this
// subscription's consumer. The session fans a single PAUSE/RESUME frame
// across every Subscription of the connection (ADR 0022).
func (s *Subscription) SetPaused(paused bool) {
	s.consumer.setPaused(paused)
}

// Partition owns one WAL, dedupe LRU, and consumer registry — the unit
// that was the whole of Topic before v2 M4 (ADR 0021). MsgID is assigned
// by its wal.Log, so it is monotonic per partition: total order within a
// partition, none across partitions. Created by Broker.openPartition;
// never constructed directly outside the package.
type Partition struct {
	topic string // parent topic name (labels, Inflight.Topic)
	id    int
	dir   string // directory holding 000000.log + offsets.json

	log    *wal.Log
	dedupe *DedupeIndex

	// window is the per-consumer receive window applied to every Consumer
	// this partition creates (ADR 0022, --recv-window).
	window int

	pubMu sync.Mutex

	// pendingDelayed counts un-fired delayed records parked in this
	// partition, exported as toymq_delayed_pending (ADR 0027). Best-effort
	// and in-process only (like the DLQ attempt count, ADR 0024): it resets
	// to 0 on restart, so a delayed record published before a restart fires
	// without a matching increment — decDelayed clamps at 0 for that case.
	pendingDelayed atomic.Int64

	consumersMu sync.RWMutex
	consumers   map[string]*Consumer
}

func newPartition(topic string, id int, dir string, log *wal.Log, dedupe *DedupeIndex, window int) *Partition {
	return &Partition{
		topic:     topic,
		id:        id,
		dir:       dir,
		log:       log,
		dedupe:    dedupe,
		window:    window,
		consumers: make(map[string]*Consumer),
	}
}

// rebuildIndexes replays one recovered WAL record into a partition's
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

// publishCtx appends payload to this partition's log. A non-empty
// dedupeKey activates dedupe — a second call with the same key returns
// the original MsgID and duplicate=true without a new WAL write. The
// context carries any active OTel span; m may be nil.
func (p *Partition) publishCtx(ctx context.Context, dedupeKey string, payload []byte, delayMs uint64, m *metrics.Metrics) (msgID uint64, duplicate bool, err error) {
	p.pubMu.Lock()
	defer p.pubMu.Unlock()

	if dedupeKey != "" {
		if existing, ok := p.dedupe.Lookup(dedupeKey); ok {
			return existing, true, nil
		}
	}

	now := time.Now().UnixNano()
	// The proposer resolves the delay to an absolute visible-at time here,
	// so it travels in the record and is Apply-deterministic in v3 — the
	// same treatment as TsNs (ADR 0025 / ADR 0018). 0 = visible now.
	var visibleAtNs uint64
	if delayMs > 0 {
		visibleAtNs = uint64(now) + delayMs*uint64(time.Millisecond)
	}

	rec := wal.Record{
		TsNs:        uint64(now),
		DedupeKey:   dedupeKey,
		Payload:     payload,
		VisibleAtNs: visibleAtNs,
	}

	start := time.Now()
	id, _, err := p.log.Append(rec)
	if err != nil {
		return 0, false, err
	}
	// Exemplar links a WAL-latency spike to the publishing trace (ADR 0027);
	// traceIDFromCtx returns "" under the noop provider, degrading to a plain
	// Observe.
	m.ObserveWALAppendExemplar(p.topic, time.Since(start).Seconds(), traceIDFromCtx(ctx))
	m.SetPartitionLatestMsgID(p.topic, p.id, id)
	if visibleAtNs > 0 {
		m.SetDelayedPending(p.topic, p.id, int(p.pendingDelayed.Add(1)))
	}

	if dedupeKey != "" {
		p.dedupe.Insert(dedupeKey, id)
	}

	return id, false, nil
}

// decDelayed decrements the parked-delayed counter when a delayed record
// fires, clamping at 0 (a record published before a restart has no
// matching increment in this process — see the pendingDelayed comment).
func (p *Partition) decDelayed(m *metrics.Metrics) {
	n := p.pendingDelayed.Add(-1)
	if n < 0 {
		p.pendingDelayed.CompareAndSwap(n, 0)
		n = 0
	}
	m.SetDelayedPending(p.topic, p.id, int(n))
}

// head returns the partition's highest assigned MsgID, or 0 when empty.
// Used for the consumer-lag gauge (ADR 0027).
func (p *Partition) head() uint64 {
	id, ok := p.log.Head()
	if !ok {
		return 0
	}
	return id
}

func (p *Partition) getOrCreateConsumer(id string) *Consumer {
	p.consumersMu.RLock()
	c, ok := p.consumers[id]
	p.consumersMu.RUnlock()

	if ok {
		return c
	}

	p.consumersMu.Lock()
	defer p.consumersMu.Unlock()
	if c, ok := p.consumers[id]; ok {
		return c
	}
	c = newConsumer(id, p, p.window)
	p.consumers[id] = c
	return c
}

// consumerStartID resolves where delivery for c should begin, applying
// the retention floor (ADR 0023). A fresh consumer (never acked) starts
// at the floor — the earliest still-retained record — so a
// retention-trimmed partition stays subscribable. A resuming consumer
// whose next offset (lastAcked+1) has fallen below the floor lost
// un-consumed data: outOfRange is true and the caller must refuse
// delivery (the SUB path reports OUT_OF_RANGE). Any start at or above
// the floor is delivered normally.
func (p *Partition) consumerStartID(c *Consumer) (startID uint64, outOfRange bool) {
	c.mu.Lock()
	hasAcked := c.hasAcked
	lastAcked := c.lastAcked
	c.mu.Unlock()

	floor := p.log.RetainedFloor()
	if !hasAcked {
		return floor, false
	}
	start := lastAcked + 1
	if start < floor {
		return 0, true
	}
	return start, false
}

// awaitVisible blocks until the wall-clock reaches visibleAtNs (a
// delayed message's release time) or ctx is cancelled. Returns true when
// the message is now visible, false if ctx cancelled first. The wait is
// on the local delivery side only (ADR 0025): the release time is
// already fixed in the record, so this timer never affects Apply
// determinism in v3.
func awaitVisible(ctx context.Context, visibleAtNs uint64) bool {
	now := uint64(time.Now().UnixNano())
	if visibleAtNs <= now {
		return true
	}
	timer := time.NewTimer(time.Duration(visibleAtNs - now))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *Partition) runDelivery(ctx context.Context, c *Consumer, sub *Subscription, sendCh chan<- *Inflight, m *metrics.Metrics) {
	defer close(sub.done)
	defer m.DecSubs()

	startID, outOfRange := p.consumerStartID(c)
	if outOfRange {
		// Resuming consumer whose durable offset fell below the retained
		// floor. The SUB pre-check (SubStartCheck) already told the client
		// OUT_OF_RANGE; just don't start delivering.
		return
	}

	reader, err := p.log.NewReader(startID)
	if err != nil {
		return
	}
	defer reader.Close()

	for {
		// Flow control: block until the receive window has room and the
		// consumer is not PAUSEd (ADR 0022). Gating before reading the next
		// record keeps len(inflight) <= window, which is the memory bound.
		// Redelivery/Nack re-push already-counted inflight and bypass this
		// gate, so they never stall behind a full window.
		if !c.awaitCredit(ctx) {
			return
		}

		rec, err := reader.Next(ctx)
		if err != nil {
			return
		}

		// Delayed messages (ADR 0025): a record with a future VisibleAtNs
		// parks the delivery goroutine until that instant. Because delivery
		// is a single in-order goroutine per (partition, consumer), the
		// delayed record holds this partition's delivery until it fires —
		// head-of-line by design (use a dedicated partition for delayed
		// traffic if that is unwanted). The record is not yet inflight, so
		// no receive-window slot is held while parked.
		if rec.VisibleAtNs > 0 {
			if !awaitVisible(ctx, rec.VisibleAtNs) {
				return
			}
			p.decDelayed(m)
		}

		inf := &Inflight{
			MsgID:       rec.MsgID,
			Topic:       p.topic,
			Partition:   p.id,
			Payload:     rec.Payload,
			DeliveredAt: time.Now(),
			Attempts:    1,
		}

		c.mu.Lock()
		c.inflight[rec.MsgID] = inf
		snapshot := *inf
		inflightLen := len(c.inflight)
		c.mu.Unlock()
		m.SetInflight(p.topic, c.ID, inflightLen)

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

// subscribe binds consumerID to a fresh delivery goroutine that tails
// this partition's WAL from lastAcked+1 and forwards Inflight snapshots
// to sendCh. A second subscribe for the same consumerID detaches the
// previous Subscription before the new goroutine starts.
func (p *Partition) subscribe(ctx context.Context, consumerID string, sendCh chan<- *Inflight, m *metrics.Metrics) (*Subscription, error) {
	c := p.getOrCreateConsumer(consumerID)

	subCtx, cancel := context.WithCancel(ctx)

	sub := &Subscription{
		consumerID: consumerID,
		consumer:   c,
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

	startID, _ := p.consumerStartID(c)
	slog.InfoContext(ctx, "consumer subscribed",
		"topic", p.topic,
		"partition", p.id,
		"consumer-id", consumerID,
		"from-msg-id", startID,
	)

	m.IncSubs()
	go p.runDelivery(subCtx, c, sub, sendCh, m)
	return sub, nil
}
