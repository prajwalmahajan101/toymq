package broker

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

func seg(index, baseMsgID, bytes, maxTsNs uint64, active bool) wal.SegmentInfo {
	return wal.SegmentInfo{Index: index, BaseMsgID: baseMsgID, Bytes: bytes, MaxTsNs: maxTsNs, Active: active}
}

// TestRetentionKeepIndexGuardsUnfiredDelay verifies the delayed-message
// guard (ADR 0025): a size/age policy that would drop a segment holding
// an un-fired delayed record is overridden to keep it.
func TestRetentionKeepIndexGuardsUnfiredDelay(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	old := uint64(now.Add(-time.Hour).UnixNano())
	future := uint64(now.Add(time.Hour).UnixNano())

	// Four sealed old segments + active. Size budget alone would keep only
	// the active (drop 0..3). Segment 2 holds an un-fired delayed record.
	segs := []wal.SegmentInfo{
		seg(0, 0, 1000, old, false),
		seg(1, 10, 1000, old, false),
		{Index: 2, BaseMsgID: 20, Bytes: 1000, MaxTsNs: old, MaxVisibleAtNs: future},
		seg(3, 30, 1000, old, false),
		seg(4, 40, 1000, old, true),
	}
	// Tiny byte budget → size policy wants keepIndex 4 (drop 0..3), but the
	// un-fired delay at segment 2 clamps it to 2.
	keep, drop := retentionKeepIndex(segs, 100, 0, now)
	if !drop || keep != 2 {
		t.Fatalf("keep=%d drop=%v, want keep=2 drop=true (un-fired delay guard)", keep, drop)
	}

	// Once the delayed record has fired (now past its VisibleAtNs), the
	// guard lifts and size retention drops up to the active segment.
	later := time.Unix(1_000_000, 0).Add(2 * time.Hour)
	keep, drop = retentionKeepIndex(segs, 100, 0, later)
	if !drop || keep != 4 {
		t.Fatalf("after firing: keep=%d drop=%v, want keep=4 drop=true", keep, drop)
	}
}

func TestKeepIndexByBytes(t *testing.T) {
	// Three 100-byte segments; the last is active.
	segs := []wal.SegmentInfo{
		seg(0, 0, 100, 0, false),
		seg(1, 10, 100, 0, false),
		seg(2, 20, 100, 0, true),
	}
	tests := []struct {
		name        string
		retainBytes uint64
		wantKeep    uint64
	}{
		{"disabled keeps all", 0, 0},
		{"budget fits two", 200, 1},
		{"budget fits one", 100, 2},
		{"budget below one still keeps active", 10, 2},
		{"budget fits all", 1000, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := keepIndexByBytes(segs, tc.retainBytes); got != tc.wantKeep {
				t.Errorf("keepIndexByBytes(%d) = %d, want %d", tc.retainBytes, got, tc.wantKeep)
			}
		})
	}
}

func TestKeepIndexByAge(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	nowNs := uint64(now.UnixNano())
	minAgo := func(m int) uint64 { return uint64(now.Add(-time.Duration(m) * time.Minute).UnixNano()) }

	// segments 0,1 are old (10m/6m ago), segment 2 is fresh, 3 active.
	segs := []wal.SegmentInfo{
		seg(0, 0, 100, minAgo(10), false),
		seg(1, 10, 100, minAgo(6), false),
		seg(2, 20, 100, minAgo(1), false),
		seg(3, 30, 100, nowNs, true),
	}
	tests := []struct {
		name     string
		retain   time.Duration
		wantKeep uint64
	}{
		{"disabled keeps all", 0, 0},
		{"5m drops the two oldest", 5 * time.Minute, 2},
		{"8m drops only the oldest", 8 * time.Minute, 1},
		{"1h keeps all", time.Hour, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := keepIndexByAge(segs, tc.retain, now); got != tc.wantKeep {
				t.Errorf("keepIndexByAge(%v) = %d, want %d", tc.retain, got, tc.wantKeep)
			}
		})
	}
}

func TestRetentionKeepIndexCombines(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	old := uint64(now.Add(-time.Hour).UnixNano())
	fresh := uint64(now.UnixNano())
	segs := []wal.SegmentInfo{
		seg(0, 0, 100, old, false),
		seg(1, 10, 100, fresh, false),
		seg(2, 20, 100, fresh, true),
	}
	// Bytes budget alone keeps from index 1; age alone (30m) drops index 0
	// too. The union drops index 0 => keepIndex 1, drop=true.
	keep, drop := retentionKeepIndex(segs, 200, 30*time.Minute, now)
	if !drop || keep != 1 {
		t.Fatalf("keep=%d drop=%v, want keep=1 drop=true", keep, drop)
	}

	// Nothing to drop when both policies retain everything.
	keep, drop = retentionKeepIndex(segs, 1000, time.Hour, now)
	if drop || keep != 0 {
		t.Fatalf("keep=%d drop=%v, want keep=0 drop=false", keep, drop)
	}

	// Single (active) segment is never dropped.
	one := []wal.SegmentInfo{seg(0, 0, 10_000, old, true)}
	if _, drop := retentionKeepIndex(one, 1, time.Nanosecond, now); drop {
		t.Fatal("active-only partition must never drop")
	}
}

// TestSweepPartitionRetentionDropsOldSegments publishes enough to create
// several segments, runs one sweep with a tight byte budget, and asserts
// old segments are reclaimed on disk and a read below the new floor
// returns OUT_OF_RANGE.
func TestSweepPartitionRetentionDropsOldSegments(t *testing.T) {
	rc := RetentionConfig{
		SegmentBytes: 300,
		RetainBytes:  600,
		Interval:     time.Hour, // ticker parked; we sweep by hand
	}
	b, err := newBroker(t.TempDir(), testDedupeCap, 1, defaultRecvWindow,
		defaultVisibilityTimeout, defaultRedeliverInterval, SyncConfig{}, rc, 0)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	const topic = "orders"
	payload := bytes.Repeat([]byte("x"), 80)
	for range 30 {
		if _, _, _, err := b.Publish(topic, "", "", 0, false, payload); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	p := b.topics[topic].partitions[0]
	before := p.log.SegmentInfos()
	if len(before) < 3 {
		t.Fatalf("expected several segments, got %d", len(before))
	}

	b.sweepRetention(time.Now())

	after := p.log.SegmentInfos()
	if len(after) >= len(before) {
		t.Fatalf("sweep did not drop segments: before=%d after=%d", len(before), len(after))
	}
	floor := after[0].BaseMsgID
	if floor == 0 {
		t.Fatal("retained floor should have advanced past MsgID 0")
	}
	// Total retained bytes are within budget plus one active segment's worth.
	var total uint64
	for _, s := range after {
		total += s.Bytes
	}
	if total > rc.RetainBytes+rc.SegmentBytes {
		t.Errorf("retained %d bytes, want <= %d", total, rc.RetainBytes+rc.SegmentBytes)
	}
	// A reader below the floor is refused rather than silently skipping.
	if _, err := p.log.NewReader(0); err != wal.ErrOutOfRange {
		t.Errorf("NewReader(0) after reclaim: err=%v, want ErrOutOfRange", err)
	}
	// A reader at the floor still works.
	r, err := p.log.NewReader(floor)
	if err != nil {
		t.Fatalf("NewReader(floor): %v", err)
	}
	r.Close()
}

// TestConsumerStartFloorSemantics verifies the fresh-vs-resuming rule
// (ADR 0023): a fresh consumer starts at the floor; a resuming consumer
// whose next offset fell below the floor is OUT_OF_RANGE; one at/above
// the floor proceeds.
func TestConsumerStartFloorSemantics(t *testing.T) {
	rc := RetentionConfig{SegmentBytes: 300, RetainBytes: 600, Interval: time.Hour}
	b, err := newBroker(t.TempDir(), testDedupeCap, 1, defaultRecvWindow,
		defaultVisibilityTimeout, defaultRedeliverInterval, SyncConfig{}, rc, 0)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	const topic = "orders"
	payload := bytes.Repeat([]byte("x"), 80)
	for range 30 {
		if _, _, _, err := b.Publish(topic, "", "", 0, false, payload); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	b.sweepRetention(time.Now())

	p := b.topics[topic].partitions[0]
	floor := p.log.RetainedFloor()
	if floor == 0 {
		t.Fatal("floor should have advanced")
	}

	// Fresh consumer → starts at the floor, never out of range.
	fresh := p.getOrCreateConsumer("fresh")
	if start, oor := p.consumerStartID(fresh); oor || start != floor {
		t.Fatalf("fresh consumer: start=%d oor=%v, want start=%d oor=false", start, oor, floor)
	}
	if err := b.SubStartCheck(topic, 0, false, "fresh"); err != nil {
		t.Fatalf("SubStartCheck(fresh): %v, want nil", err)
	}

	// Resuming consumer with next offset below the floor → OUT_OF_RANGE.
	below := p.getOrCreateConsumer("below")
	below.mu.Lock()
	below.hasAcked = true
	below.lastAcked = floor - 2 // resumes just below the retained floor
	below.mu.Unlock()
	if _, oor := p.consumerStartID(below); !oor {
		t.Fatal("resuming below floor: want outOfRange=true")
	}
	if err := b.SubStartCheck(topic, 0, false, "below"); !errors.Is(err, ErrSubOutOfRange) {
		t.Fatalf("SubStartCheck(below): %v, want ErrSubOutOfRange", err)
	}

	// Resuming consumer at/above the floor → proceeds from lastAcked+1.
	above := p.getOrCreateConsumer("above")
	above.mu.Lock()
	above.hasAcked = true
	above.lastAcked = floor + 3
	above.mu.Unlock()
	if start, oor := p.consumerStartID(above); oor || start != floor+4 {
		t.Fatalf("resuming above floor: start=%d oor=%v, want start=%d oor=false", start, oor, floor+4)
	}
	if err := b.SubStartCheck(topic, 0, false, "above"); err != nil {
		t.Fatalf("SubStartCheck(above): %v, want nil", err)
	}
}
