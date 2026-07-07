package broker

import (
	"log/slog"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

// runRetentionLoop is the background WAL-reclaim sweeper — a peer of
// runRedeliverLoop. It ticks every retention.Interval and, per
// partition, drops whole sealed segments that fall outside the size/age
// budget. It operates only on the local materialised WAL view, never on
// the replicated state transition, so it stays v3-forward-compatible
// (ADR 0023): the sweep is a non-deterministic local trigger, not part
// of Apply.
//
// When no reclaim policy is configured the loop returns immediately
// (closing retentionDone), so Close can always wait on it without a
// conditional.
func (b *Broker) runRetentionLoop() {
	defer close(b.retentionDone)

	if !b.retention.reclaimEnabled() {
		return
	}

	ticker := time.NewTicker(b.retention.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.sweepRetention(time.Now())
		case <-b.retentionCtx.Done():
			return
		}
	}
}

// sweepRetention reclaims disk for every partition once. now is passed
// in (not read from the clock) so tests can drive age-based retention
// with a fixed instant.
func (b *Broker) sweepRetention(now time.Time) {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, t := range topics {
		for _, p := range t.partitions {
			b.sweepPartitionRetention(p, now)
		}
	}
}

// sweepPartitionRetention computes the drop floor for one partition from
// the size/age policy and asks its WAL to drop every sealed segment
// below it. The active segment is never dropped (retentionKeepIndex and
// wal.DropSegmentsBefore both guard it), so an idle or single-segment
// partition is left untouched.
func (b *Broker) sweepPartitionRetention(p *Partition, now time.Time) {
	segs := p.log.SegmentInfos()
	keepIndex, drop := retentionKeepIndex(segs, b.retention.RetainBytes, b.retention.RetainDuration, now)
	if !drop {
		return
	}

	dropped, floor, err := p.log.DropSegmentsBefore(keepIndex)
	if err != nil {
		slog.Error("retention drop", "topic", p.topic, "partition", p.id, "err", err)
	}
	if dropped > 0 {
		slog.Info("retention reclaimed segments",
			"topic", p.topic,
			"partition", p.id,
			"segments-dropped", dropped,
			"retained-floor-msg-id", floor,
		)
	}
}

// retentionKeepIndex returns the lowest segment index to keep and
// whether any segment should be dropped. A segment is dropped if EITHER
// policy would evict it (keep only what BOTH policies retain), so the
// result is the more aggressive of the two floors, clamped to the active
// segment (which is never dropped).
func retentionKeepIndex(segs []wal.SegmentInfo, retainBytes uint64, retainDur time.Duration, now time.Time) (keepIndex uint64, drop bool) {
	if len(segs) == 0 {
		return 0, false
	}
	activeIdx := segs[len(segs)-1].Index

	keep := keepIndexByBytes(segs, retainBytes)
	if ageFloor := keepIndexByAge(segs, retainDur, now); ageFloor > keep {
		keep = ageFloor
	}
	if keep > activeIdx {
		keep = activeIdx
	}
	// Drop only if the floor moved past the oldest segment currently held.
	return keep, keep > segs[0].Index
}

// keepIndexByBytes returns the oldest segment index that keeps the total
// retained byte size within retainBytes, accumulating newest-first. The
// active segment is always retained even if it alone exceeds the budget.
// retainBytes == 0 disables the size policy (keep everything).
func keepIndexByBytes(segs []wal.SegmentInfo, retainBytes uint64) uint64 {
	if retainBytes == 0 {
		return segs[0].Index
	}
	keepFrom := segs[len(segs)-1].Index // always keep the active segment
	var total uint64
	for i := len(segs) - 1; i >= 0; i-- {
		total += segs[i].Bytes
		if total > retainBytes {
			break // segs[i] and everything older falls outside the budget
		}
		keepFrom = segs[i].Index
	}
	return keepFrom
}

// keepIndexByAge returns the oldest segment index whose newest record is
// still within retainDur of now; older sealed segments (all records past
// the cutoff) are dropped. The active segment is always retained.
// retainDur <= 0 disables the age policy (keep everything).
func keepIndexByAge(segs []wal.SegmentInfo, retainDur time.Duration, now time.Time) uint64 {
	if retainDur <= 0 {
		return segs[0].Index
	}
	cutoff := uint64(now.Add(-retainDur).UnixNano())
	keepFrom := segs[len(segs)-1].Index // always keep the active segment
	for _, s := range segs {
		if s.Active {
			break
		}
		// A fresh (or empty, MaxTsNs==0) sealed segment marks the floor:
		// keep it and everything newer.
		if s.MaxTsNs == 0 || s.MaxTsNs >= cutoff {
			keepFrom = s.Index
			break
		}
	}
	return keepFrom
}
