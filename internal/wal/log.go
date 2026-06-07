package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type Log struct {
	path      string
	f         *os.File
	mu        sync.Mutex
	nextMsgID uint64
	committed atomic.Uint64
}

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

	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}

	return l, nil
}

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

	return rec.MsgID, newCommitted, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
