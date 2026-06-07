package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
)

func (l *Log) recover() error {
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
	}

	if lastGood > 0 {
		l.nextMsgID = lastID + 1
	}

	l.committed.Store(lastGood)
	return nil
}
