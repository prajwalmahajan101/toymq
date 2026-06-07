package wal

import (
	"bufio"
	"context"
	"io"
	"os"
)

type Reader struct {
	log *Log
	f   *os.File
	br  *bufio.Reader
	pos uint64
}

func (l *Log) NewReader(fromMsgID uint64) (*Reader, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		log: l,
		f:   f,
		br:  bufio.NewReader(f),
	}

	for {
		commited := l.committed.Load()

		if r.pos >= commited {
			break
		}

		rec, n, err := Decode(r.br)
		if err != nil {
			f.Close()
			return nil, err
		}
		if rec.MsgID >= fromMsgID {
			if _, err := f.Seek(int64(r.pos), io.SeekStart); err != nil {
				f.Close()
				return nil, err
			}
			r.br.Reset(f)
			return r, nil
		}
		r.pos += uint64(n)
	}

	return r, nil
}

func (r *Reader) Next(ctx context.Context) (Record, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			r.log.mu.Lock()
			r.log.cond.Broadcast()
			r.log.mu.Unlock()
		case <-done:
		}
	}()

	r.log.mu.Lock()
	defer r.log.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		if r.pos < r.log.committed.Load() {
			r.log.mu.Unlock()
			rec, n, err := Decode(r.br)
			r.log.mu.Lock()
			if err != nil {
				return Record{}, err
			}
			r.pos += uint64(n)
			return rec, nil
		}
		r.log.cond.Wait()
	}
}

func (r *Reader) Close() error {
	return r.f.Close()
}
