package wal

import (
	"fmt"
	"os"
	"path/filepath"
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

// newActiveSegment opens (creating if absent) the segment file for
// append and returns a segment ready to be written to. baseMsgID and
// baseByteOffset place it in the logical stream — both zero for the
// very first segment (000000.log).
func newActiveSegment(dir string, index, baseMsgID, baseByteOffset uint64) (*segment, error) {
	path := filepath.Join(dir, segmentName(index))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
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

// close releases the segment's fd. Both active and sealed segments hold
// an open fd; a segment whose fd was already released (retention) is a
// no-op. Safe to call once during Log.Close.
func (s *segment) close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}
