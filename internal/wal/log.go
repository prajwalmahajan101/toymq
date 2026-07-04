package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// SyncMode selects when Append forces data to disk. See ADR 0019
// (supersedes ADR 0002).
type SyncMode int

const (
	// SyncPerMessage fsyncs on every Append before returning; OK means
	// durable. The default and today's behaviour (ADR 0002).
	SyncPerMessage SyncMode = iota
	// SyncBatched defers fsync to a group committer that flushes every
	// sync-interval; Append blocks until its record's batch is fsynced,
	// so OK still means durable — it just amortises the fsync cost.
	SyncBatched
	// SyncNone never fsyncs; committed advances immediately so records
	// are delivered, but OK means only "written to the OS", not durable.
	// Survives a process SIGKILL (page cache), NOT power loss / panic.
	SyncNone
)

// DefaultSyncInterval is the group-commit window for SyncBatched.
const DefaultSyncInterval = 5 * time.Millisecond

// String returns the flag-facing name of the mode.
func (m SyncMode) String() string {
	switch m {
	case SyncPerMessage:
		return "per-message"
	case SyncBatched:
		return "batched"
	case SyncNone:
		return "none"
	default:
		return "unknown"
	}
}

// ParseSyncMode maps a flag string to a SyncMode. Unknown values return
// an error naming the accepted set.
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "per-message":
		return SyncPerMessage, nil
	case "batched":
		return SyncBatched, nil
	case "none":
		return SyncNone, nil
	default:
		return 0, errors.New("fsync mode must be per-message|batched|none")
	}
}

// ErrSyncFailed is returned to blocked appenders when the group
// committer's fsync fails; the Log is no longer durable.
var ErrSyncFailed = errors.New("wal: group-commit fsync failed")

// Log is the append-only write-ahead log backing one topic. Writers
// serialize through mu; readers cap reads at committed (advanced only
// after fsync, so consumers never see un-fsynced data). See ADRs 0001,
// 0002, 0019.
type Log struct {
	path      string
	f         *os.File
	mu        sync.Mutex
	nextMsgID uint64
	written   uint64 // bytes written to f; may exceed committed pre-fsync (batched)
	committed atomic.Uint64
	cond      *sync.Cond

	syncMode     SyncMode
	syncInterval time.Duration
	syncFn       func() error // seam: defaults to l.f.Sync; overridable in tests
	syncCount    atomic.Uint64
	commitErr    error // set by the committer on fsync failure; read by waiters

	stopCommit chan struct{}
	commitDone chan struct{}
	stopOnce   sync.Once
}

// Option configures Open.
type Option func(*options)

type options struct {
	recoveryVisitor func(Record)
	syncMode        SyncMode
	syncInterval    time.Duration
}

// WithRecoveryVisitor registers fn to be invoked once for every valid
// record found during the recovery scan, in ascending MsgID order.
// Records beyond a torn tail (which get truncated) are never visited.
// The broker uses this to rebuild the per-topic dedupe index from the
// log without a second scan or a sidecar file — see ADR 0018.
func WithRecoveryVisitor(fn func(Record)) Option {
	return func(o *options) { o.recoveryVisitor = fn }
}

// WithSyncMode selects the fsync strategy. interval is the group-commit
// window and applies only to SyncBatched (<=0 falls back to
// DefaultSyncInterval). The zero value (SyncPerMessage) preserves ADR
// 0002 behaviour, so callers that omit this option are unaffected.
func WithSyncMode(mode SyncMode, interval time.Duration) Option {
	return func(o *options) {
		o.syncMode = mode
		o.syncInterval = interval
	}
}

// Open opens (or creates) the segment file at dir/000000.log, scans
// for a torn tail, truncates if needed, and returns a Log ready for
// Append and NewReader. See ADR 0003.
func Open(dir string, opts ...Option) (*Log, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "000000.log")

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	interval := o.syncInterval
	if interval <= 0 {
		interval = DefaultSyncInterval
	}

	l := &Log{
		path:         path,
		f:            f,
		syncMode:     o.syncMode,
		syncInterval: interval,
	}
	l.syncFn = l.f.Sync
	l.cond = sync.NewCond(&l.mu)

	if err := l.recover(o.recoveryVisitor); err != nil {
		f.Close()
		return nil, err
	}
	// After recovery, committed == the durable byte length; nothing is
	// written-but-unflushed yet.
	l.written = l.committed.Load()

	if l.syncMode == SyncBatched {
		l.stopCommit = make(chan struct{})
		l.commitDone = make(chan struct{})
		go l.runCommitter()
	}

	return l, nil
}

// Append assigns the next MsgID to rec, writes it to disk, and returns
// the assigned MsgID and its end byte-offset once that offset is
// durable-and-visible (committed). The write itself is always
// serialized through l.mu; the durability fence depends on SyncMode:
//
//   - SyncPerMessage: fsync inline, then return (ADR 0002).
//   - SyncBatched:    return once the group committer has fsynced this
//     record's batch — Append blocks until committed >= its end offset.
//   - SyncNone:       advance committed without fsync and return.
func (l *Log) Append(rec Record) (msgID uint64, byteOffset uint64, err error) {
	l.mu.Lock()

	rec.MsgID = l.nextMsgID

	var buf bytes.Buffer
	if err := Encode(rec, &buf); err != nil {
		l.mu.Unlock()
		return 0, 0, err
	}
	if _, err := l.f.Write(buf.Bytes()); err != nil {
		l.mu.Unlock()
		return 0, 0, err
	}
	l.nextMsgID++
	l.written += uint64(buf.Len())
	end := l.written

	switch l.syncMode {
	case SyncBatched:
		// Wait until the group committer makes this record durable. The
		// committer snapshots l.written under mu, fsyncs, then advances
		// committed and broadcasts; we recheck the predicate under mu so
		// a broadcast between our unlock/relock is never missed.
		for l.committed.Load() < end {
			if l.commitErr != nil {
				err := l.commitErr
				l.mu.Unlock()
				return 0, 0, err
			}
			l.cond.Wait()
		}
		l.mu.Unlock()
		return rec.MsgID, end, nil

	case SyncNone:
		l.committed.Store(end) // visible immediately; NOT fsynced
		l.cond.Broadcast()
		l.mu.Unlock()
		return rec.MsgID, end, nil

	default: // SyncPerMessage
		if err := l.syncFn(); err != nil {
			l.mu.Unlock()
			return 0, 0, err
		}
		l.syncCount.Add(1)
		l.committed.Store(end)
		l.cond.Broadcast()
		l.mu.Unlock()
		return rec.MsgID, end, nil
	}
}

// runCommitter is the SyncBatched group-commit loop: every syncInterval
// (and once more on stop) it fsyncs any bytes written since the last
// commit and advances committed, releasing all appenders waiting on
// that range. Owned by the Log; started in Open, stopped in Close.
func (l *Log) runCommitter() {
	defer close(l.commitDone)

	ticker := time.NewTicker(l.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.flush()
		case <-l.stopCommit:
			l.flush() // final flush so graceful shutdown is durable
			return
		}
	}
}

// flush fsyncs the bytes written so far and advances committed. It
// snapshots the write offset under mu, fsyncs OUTSIDE the lock so
// concurrent Appends can proceed during the fsync, then re-takes mu to
// publish the new committed offset. The snapshot is always on a record
// boundary (Append updates written only after a full record write), so
// committed never lands mid-record.
func (l *Log) flush() {
	l.mu.Lock()
	snap := l.written
	if snap <= l.committed.Load() || l.commitErr != nil {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	syncErr := l.syncFn()

	l.mu.Lock()
	if syncErr != nil {
		l.commitErr = syncErr
		l.cond.Broadcast() // release waiters with ErrSyncFailed
		l.mu.Unlock()
		return
	}
	l.syncCount.Add(1)
	if snap > l.committed.Load() {
		l.committed.Store(snap)
	}
	l.cond.Broadcast()
	l.mu.Unlock()
}

// Close releases the segment file. For SyncBatched it first stops the
// group committer, whose final flush makes any pending appends durable
// and releases their waiters, before the fd is closed. Safe to call
// once; further Appends after Close fail at the os.File layer.
func (l *Log) Close() error {
	if l.syncMode == SyncBatched {
		l.stopOnce.Do(func() { close(l.stopCommit) })
		<-l.commitDone
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
