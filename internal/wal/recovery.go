package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
)

// recover scans the segment for a torn tail, truncating at the last
// good frame boundary. When visit is non-nil it is called for each
// valid record in ascending MsgID order (never for the truncated
// tail), letting callers rebuild in-memory indexes in the same pass.
func (l *Log) recover(visit func(Record)) error {
	f, err := os.Open(l.path)
	if err != nil {
		return err
	}

	defer f.Close()

	br := bufio.NewReader(f)

	var (
		pos      uint64
		lastGood uint64
		lastID   uint64
	)

	for {
		rec, n, err := Decode(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, ErrShortRead) || errors.Is(err, ErrBadCRC) || errors.Is(err, ErrTooLarge) {
			if err := l.f.Truncate(int64(lastGood)); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		pos += uint64(n)
		lastGood = pos
		lastID = rec.MsgID
		if visit != nil {
			visit(rec)
		}
	}

	if lastGood > 0 {
		l.nextMsgID = lastID + 1
	}

	l.committed.Store(lastGood)
	return nil
}
