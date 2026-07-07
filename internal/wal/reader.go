package wal

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
)

// ErrOutOfRange is returned by NewReader when the requested start MsgID
// is below the retained floor — its segment has been dropped by
// retention and can no longer be served. The server maps this to the
// wire error OUT_OF_RANGE so a consumer learns its start point is gone
// rather than silently skipping to the oldest surviving record.
var ErrOutOfRange = errors.New("wal: start MsgID below retained floor")

// Reader is a tailing cursor into a Log. One Reader per active
// subscription; readers do not block writers (writers serialize
// through Log.mu, readers cap at the atomic committed offset). A Reader
// spans segment boundaries: it opens its own fd per segment, rolls
// forward to the next segment at the end of a sealed one, and tails the
// active segment via the log's cond-var.
type Reader struct {
	log      *Log
	seg      *segment // segment currently open
	f        *os.File // reader-owned fd on seg (independent of the append fd)
	br       *bufio.Reader
	localPos uint64 // byte offset within seg's file
}

// NewReader opens a cursor positioned at the first record with
// MsgID >= fromMsgID. It locates the segment whose MsgID range contains
// fromMsgID, opens a fresh fd there, and scans forward to the target.
// Returns ErrOutOfRange if fromMsgID is below the oldest retained
// segment's base (that data has been dropped). Callers must Close the
// Reader.
func (l *Log) NewReader(fromMsgID uint64) (*Reader, error) {
	l.mu.Lock()
	if fromMsgID < l.segments[0].baseMsgID {
		l.mu.Unlock()
		return nil, ErrOutOfRange
	}
	// Default to the active (last) segment; if fromMsgID falls within an
	// earlier segment's range, start there instead.
	start := l.segments[len(l.segments)-1]
	for i := 0; i+1 < len(l.segments); i++ {
		if fromMsgID < l.segments[i+1].baseMsgID {
			start = l.segments[i]
			break
		}
	}
	l.mu.Unlock()

	r := &Reader{log: l}
	if err := r.openSegment(start); err != nil {
		return nil, err
	}

	// Scan forward within the start segment to the first record whose
	// MsgID >= fromMsgID, then rewind to its frame start so the first
	// Next returns it. Bounded by the segment's readable length; if
	// fromMsgID is beyond what is written yet, Next tails for it.
	for {
		limit, _ := l.readableLen(start)
		if r.localPos >= limit {
			break
		}
		rec, n, err := Decode(r.br)
		if err != nil {
			r.f.Close()
			return nil, err
		}
		if rec.MsgID >= fromMsgID {
			if _, err := r.f.Seek(int64(r.localPos), io.SeekStart); err != nil {
				r.f.Close()
				return nil, err
			}
			r.br.Reset(r.f)
			break
		}
		r.localPos += uint64(n)
	}

	return r, nil
}

// openSegment points the reader at seg: it closes any current fd, opens
// a fresh read-only fd on seg's file, and resets the position to the
// segment start.
func (r *Reader) openSegment(seg *segment) error {
	if r.f != nil {
		r.f.Close()
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return err
	}
	r.f = f
	r.br = bufio.NewReader(f)
	r.seg = seg
	r.localPos = 0
	return nil
}

// Next returns the next Record after the current position. Within a
// segment it decodes the next frame; at the end of a sealed segment it
// rolls to the successor; at the committed end of the active segment it
// blocks on the log's cond-var until a record is appended. Returns
// ctx.Err() if ctx cancels while waiting.
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

		limit, isActive := r.log.readableLenLocked(r.seg)

		if r.localPos < limit {
			r.log.mu.Unlock()
			rec, n, err := Decode(r.br)
			r.log.mu.Lock()
			if err != nil {
				return Record{}, err
			}
			r.localPos += uint64(n)
			return rec, nil
		}

		if !isActive {
			// Sealed segment fully read: roll to its successor and retry.
			next := r.log.segmentAfterLocked(r.seg.index)
			if next == nil {
				// A sealed segment always has a successor (otherwise it
				// would be the active tail); reaching here means it was
				// dropped from under us. Wait as if active.
				r.log.cond.Wait()
				continue
			}
			r.log.mu.Unlock()
			err := r.openSegment(next)
			r.log.mu.Lock()
			if err != nil {
				return Record{}, err
			}
			continue
		}

		// Active segment, caught up to committed: wait for more data (or
		// for a rotation that seals this segment and appends a successor).
		r.log.cond.Wait()
	}
}

// Close releases the Reader's fd. Independent of Log.Close.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}
