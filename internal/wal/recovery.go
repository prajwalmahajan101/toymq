package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// recover scans every segment in ascending order, rebuilding the
// logical stream from the retained segments. For each segment it fills
// in baseMsgID (the first record's MsgID) and baseByteOffset (the
// cumulative durable length of all prior segments), and — when visit is
// non-nil — calls it for each valid record in ascending MsgID order
// (never for a truncated tail), letting callers rebuild in-memory
// indexes in the same pass. See ADR 0018.
//
// A torn tail (partial trailing write) can legitimately occur only in
// the active (last) segment, which is truncated at the last good frame
// boundary. Corruption in a sealed segment is a hard error: sealed
// segments are fsynced in full before sealing, so a bad frame there
// signals real damage rather than an interrupted write.
func (l *Log) recover(visit func(Record)) error {
	var (
		globalPos uint64 // cumulative durable bytes across scanned segments
		lastID    uint64
		anyRecord bool
	)

	for i, seg := range l.segments {
		seg.baseByteOffset = globalPos
		isActive := i == len(l.segments)-1

		localGood, firstID, sawRecord, err := l.recoverSegment(seg, isActive, &lastID, &anyRecord, visit)
		if err != nil {
			return err
		}

		if sawRecord {
			seg.baseMsgID = firstID
		} else if anyRecord {
			// Empty segment (only the active one can legitimately be empty,
			// e.g. a crash right after rotation but before the first write):
			// its first record will be the next MsgID assigned.
			seg.baseMsgID = lastID + 1
		}

		globalPos += localGood
	}

	if anyRecord {
		l.nextMsgID = lastID + 1
	}
	l.committed.Store(globalPos)
	return nil
}

// recoverSegment scans one segment file, returning the byte length of
// its good (untruncated) records, the MsgID of its first record, and
// whether it held any record. lastID/anyRecord are threaded across
// segments so MsgID order and emptiness are tracked globally.
func (l *Log) recoverSegment(seg *segment, isActive bool, lastID *uint64, anyRecord *bool, visit func(Record)) (localGood, firstID uint64, sawRecord bool, err error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return 0, 0, false, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var localPos uint64

	for {
		rec, n, decErr := Decode(br)
		if errors.Is(decErr, io.EOF) {
			break
		}
		if errors.Is(decErr, ErrShortRead) || errors.Is(decErr, ErrBadCRC) || errors.Is(decErr, ErrTooLarge) {
			if !isActive {
				return 0, 0, false, fmt.Errorf("wal: corruption in sealed segment %s at offset %d: %w", seg.path, localGood, decErr)
			}
			// Torn tail in the active segment: truncate to the last good
			// boundary. seg.f is the active O_RDWR fd.
			if err := seg.f.Truncate(int64(localGood)); err != nil {
				return 0, 0, false, err
			}
			break
		}
		if decErr != nil {
			return 0, 0, false, decErr
		}

		localPos += uint64(n)
		localGood = localPos
		if !sawRecord {
			firstID = rec.MsgID
			sawRecord = true
		}
		*lastID = rec.MsgID
		*anyRecord = true
		if visit != nil {
			visit(rec)
		}
	}

	return localGood, firstID, sawRecord, nil
}
