package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Log is the append-only write-ahead log backing one topic. Writers
// serialize through mu; readers cap reads at committed (advanced
// after fsync). See ADRs 0001 and 0002.
type Log struct {
	path      string
	f         *os.File
	mu        sync.Mutex
	nextMsgID uint64
	committed atomic.Uint64
	cond      *sync.Cond
}

// Open opens (or creates) the segment file at dir/000000.log, scans
// for a torn tail, truncates if needed, and returns a Log ready for
// Append and NewReader. See ADR 0003.
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "000000.log")

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	l := &Log{
		path: path,
		f:    f,
	}
	l.cond = sync.NewCond(&l.mu)

	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}

	return l, nil
}

// Append assigns the next MsgID to rec, writes it to disk, calls
// fsync, and advances committed. Returns the assigned MsgID and the
// post-fsync byte offset. Serializes through l.mu.
func (l *Log) Append(rec Record) (msgID uint64, byteOffset uint64, err error) {
	l.mu.Lock()

	defer l.mu.Unlock()

	rec.MsgID = l.nextMsgID

	var buf bytes.Buffer
	if err := Encode(rec, &buf); err != nil {
		return 0, 0, err
	}

	if _, err := l.f.Write(buf.Bytes()); err != nil {
		return 0, 0, err
	}
	if err := l.f.Sync(); err != nil {
		return 0, 0, err
	}
	l.nextMsgID++
	newCommitted := l.committed.Load() + uint64(buf.Len())
	l.committed.Store(newCommitted)

	l.cond.Broadcast()
	return rec.MsgID, newCommitted, nil
}

// Close releases the segment file. Safe to call once; further
// Appends after Close will fail at the os.File layer.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
