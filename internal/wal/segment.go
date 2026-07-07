package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// segmentExt is the on-disk suffix for every WAL segment file. Segments
// are named by a zero-padded, monotonically increasing index:
// 000000.log, 000001.log, … — lexical order equals creation order.
const segmentExt = ".log"

// segmentName formats the file name for the segment with the given
// zero-based index (000000.log, 000001.log, …). Six digits comfortably
// covers any realistic segment count for a single-node WAL.
func segmentName(index uint64) string {
	return fmt.Sprintf("%06d%s", index, segmentExt)
}

// segment is one numbered WAL file on disk. Segments are created in
// ascending order and, once a newer segment exists, are sealed and
// immutable — only the last (active) segment is written to. A record's
// position in the logical byte stream is baseByteOffset plus its offset
// within this segment's file; likewise its MsgID is >= baseMsgID.
//
// Byte-stream length and durability are tracked globally on the owning
// Log (Log.written / Log.committed), which the group committer and
// readers already coordinate through. A segment therefore holds only
// the per-file facts: where on disk it lives, where in the logical
// stream it starts, and its fd. Only the active segment is written to;
// a sealed segment keeps its fd open (read-only) until the Log closes
// or retention drops it — see rotate for why closing it early is unsafe
// against a racing group-commit flush.
type segment struct {
	index          uint64   // zero-based segment number; matches the file name
	path           string   // absolute path to the .log file
	baseMsgID      uint64   // MsgID of the first record this segment holds
	baseByteOffset uint64   // logical byte offset where this segment begins
	f              *os.File // fd: writable while active, retained read-only once sealed
}

// openSegment opens the segment file at the given index and returns a
// segment. The active segment is opened O_RDWR|O_CREATE|O_APPEND (it is
// the append target and the only one recovery may truncate); a sealed
// segment is opened read-only. baseMsgID and baseByteOffset place it in
// the logical stream — both zero for the very first segment, and filled
// in by recovery for segments discovered on Open.
func openSegment(dir string, index, baseMsgID, baseByteOffset uint64, active bool) (*segment, error) {
	path := filepath.Join(dir, segmentName(index))
	flags := os.O_RDONLY
	if active {
		flags = os.O_RDWR | os.O_CREATE | os.O_APPEND
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	return &segment{
		index:          index,
		path:           path,
		baseMsgID:      baseMsgID,
		baseByteOffset: baseByteOffset,
		f:              f,
	}, nil
}

// discoverSegments lists dir for segment files (NNNNNN.log), opens them
// in ascending index order, and returns them with the last marked
// active. Indices need not start at 0 or be contiguous — retention may
// have dropped a prefix. Returns nil (no error) when dir holds no
// segment files yet, so Open can create the first one. baseMsgID and
// baseByteOffset are left zero here; recovery fills them by scanning.
func discoverSegments(dir string) ([]*segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var indices []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segmentExt) {
			continue
		}
		var idx uint64
		if _, err := fmt.Sscanf(name, "%06d"+segmentExt, &idx); err != nil {
			continue
		}
		// Sscanf is lenient (e.g. accepts "1.log"); require the exact
		// zero-padded canonical name so only real segment files match.
		if segmentName(idx) != name {
			continue
		}
		indices = append(indices, idx)
	}
	if len(indices) == 0 {
		return nil, nil
	}
	slices.Sort(indices)

	segs := make([]*segment, len(indices))
	for i, idx := range indices {
		active := i == len(indices)-1
		seg, err := openSegment(dir, idx, 0, 0, active)
		if err != nil {
			for _, s := range segs[:i] {
				s.close()
			}
			return nil, err
		}
		segs[i] = seg
	}
	return segs, nil
}

// segmentAfterLocked returns the segment immediately following the one
// with the given index — the smallest index strictly greater than idx —
// or nil if none exists yet. Segments are kept sorted ascending, so the
// first match is the immediate successor. Caller must hold l.mu.
func (l *Log) segmentAfterLocked(idx uint64) *segment {
	for _, s := range l.segments {
		if s.index > idx {
			return s
		}
	}
	return nil
}

// readableLenLocked returns how many bytes of seg a reader may consume
// and whether seg is the active tail. For the active segment the limit
// is the committed offset (readers never see un-fsynced bytes); for a
// sealed segment it is the full segment length (next segment's base
// minus this one's), which is entirely durable. Caller must hold l.mu.
func (l *Log) readableLenLocked(seg *segment) (limit uint64, isActive bool) {
	last := l.segments[len(l.segments)-1]
	if seg.index == last.index {
		return l.committed.Load() - seg.baseByteOffset, true
	}
	if next := l.segmentAfterLocked(seg.index); next != nil {
		return next.baseByteOffset - seg.baseByteOffset, false
	}
	// seg is no longer in the slice (dropped by retention while a reader
	// held it); fall back to the committed-based length.
	return l.committed.Load() - seg.baseByteOffset, false
}

// readableLen is readableLenLocked with its own lock, for callers (e.g.
// NewReader's forward scan) that do not already hold l.mu.
func (l *Log) readableLen(seg *segment) (limit uint64, isActive bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readableLenLocked(seg)
}

// close releases the segment's fd. Both active and sealed segments hold
// an open fd; a segment whose fd was already released (retention) is a
// no-op. Safe to call once during Log.Close.
func (s *segment) close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}
