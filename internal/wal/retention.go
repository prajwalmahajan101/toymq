package wal

import "os"

// SegmentInfo is a read-only snapshot of one segment, exposed to the
// broker's retention sweeper so it can decide which sealed segments to
// drop without reaching into wal internals. Slices returned by
// SegmentInfos are ordered oldest-first.
type SegmentInfo struct {
	Index          uint64 // segment number (file name)
	BaseMsgID      uint64 // MsgID of the first record in the segment
	Bytes          uint64 // readable byte length (committed for active, full for sealed)
	MaxTsNs        uint64 // newest record timestamp in the segment (0 if empty)
	MaxVisibleAtNs uint64 // highest VisibleAtNs in the segment (0 if none delayed)
	Active         bool   // true for the single writable segment (never dropped)
}

// RetainedFloor returns the lowest MsgID still readable — the oldest
// retained segment's baseMsgID. A NewReader below this returns
// ErrOutOfRange; the broker uses it to distinguish a resuming consumer
// that lost data (start < floor) from a fresh consumer that should begin
// at the floor.
func (l *Log) RetainedFloor() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments[0].baseMsgID
}

// SegmentInfos returns a snapshot of every segment, oldest first. The
// retention sweeper uses it to compute a drop set from size/age policy;
// it is a point-in-time copy, safe to inspect without holding any lock.
func (l *Log) SegmentInfos() []SegmentInfo {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]SegmentInfo, len(l.segments))
	for i, seg := range l.segments {
		bytes, active := l.readableLenLocked(seg)
		out[i] = SegmentInfo{
			Index:          seg.index,
			BaseMsgID:      seg.baseMsgID,
			Bytes:          bytes,
			MaxTsNs:        seg.maxTsNs,
			MaxVisibleAtNs: seg.maxVisibleAtNs,
			Active:         active,
		}
	}
	return out
}

// DropSegmentsBefore removes every sealed segment with index < keepIndex:
// it deletes their files and closes their (append/sealed) fds, then
// returns how many were dropped and the new retained-floor MsgID (the
// oldest surviving segment's baseMsgID). The active segment is never
// dropped — keepIndex is clamped to the active index — so the Log always
// keeps at least one segment.
//
// A concurrent Reader that still holds one of the dropped segments keeps
// its own fd (opened via os.Open) valid after the unlink and rolls
// forward normally; a NewReader started afterwards for a MsgID below the
// new floor gets ErrOutOfRange.
func (l *Log) DropSegmentsBefore(keepIndex uint64) (dropped int, floorMsgID uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if activeIdx := l.active().index; keepIndex > activeIdx {
		keepIndex = activeIdx
	}

	cut := 0
	for cut < len(l.segments) && l.segments[cut].index < keepIndex {
		cut++
	}
	if cut == 0 {
		return 0, l.segments[0].baseMsgID, nil
	}

	toDrop := l.segments[:cut]
	l.segments = l.segments[cut:]

	var firstErr error
	for _, seg := range toDrop {
		seg.close()
		if rmErr := os.Remove(seg.path); rmErr != nil && firstErr == nil {
			firstErr = rmErr
		}
	}
	return len(toDrop), l.segments[0].baseMsgID, firstErr
}
