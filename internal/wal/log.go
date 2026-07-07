package wal

import (
	"bytes"
	"errors"
	"os"
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
	dir       string
	segments  []*segment // ordered by baseMsgID; last element is the active (writable) one
	mu        sync.Mutex
	nextMsgID uint64
	written   uint64 // logical bytes written; may exceed committed pre-fsync (batched)
	committed atomic.Uint64
	cond      *sync.Cond

	syncMode     SyncMode
	syncInterval time.Duration
	syncFn       func() error // fsync of the active segment; repointed on rotation, snapshotted under mu
	syncCount    atomic.Uint64
	commitErr    error // set by the committer on fsync failure; read by waiters

	segmentBytes uint64 // soft size cap per segment; 0 disables rotation (single ever-growing segment)

	stopCommit chan struct{}
	commitDone chan struct{}
	stopOnce   sync.Once
}

// active returns the current writable segment — always the last in the
// ordered slice. A Log always has at least one segment after Open.
func (l *Log) active() *segment {
	return l.segments[len(l.segments)-1]
}

// Option configures Open.
type Option func(*options)

type options struct {
	recoveryVisitor func(Record)
	syncMode        SyncMode
	syncInterval    time.Duration
	segmentBytes    uint64
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

// WithSegmentBytes sets the soft size cap for a single WAL segment. When
// the active segment already holds data and appending the next record
// would carry it past n bytes, Append seals the active segment on the
// record boundary and rolls to a fresh one (a single record larger than
// n still lands whole in its own segment — rotation never splits a
// record). n == 0 disables rotation: the Log keeps one ever-growing
// segment, exactly as before M6, so callers that omit this are
// unaffected.
func WithSegmentBytes(n uint64) Option {
	return func(o *options) { o.segmentBytes = n }
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

	// Discover existing segments (NNNNNN.log) in ascending order; create
	// the first (000000.log) if the directory is empty. baseMsgID and
	// baseByteOffset are filled in by recover as it scans.
	segments, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		seg, err := openSegment(dir, 0, 0, 0, true)
		if err != nil {
			return nil, err
		}
		segments = []*segment{seg}
	}

	interval := o.syncInterval
	if interval <= 0 {
		interval = DefaultSyncInterval
	}

	l := &Log{
		dir:          dir,
		segments:     segments,
		syncMode:     o.syncMode,
		syncInterval: interval,
		segmentBytes: o.segmentBytes,
	}
	l.syncFn = l.active().f.Sync
	l.cond = sync.NewCond(&l.mu)

	if err := l.recover(o.recoveryVisitor); err != nil {
		for _, seg := range segments {
			seg.close()
		}
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

	// Roll to a fresh segment on the record boundary if writing this
	// record would carry the active segment past its size cap. Only when
	// the active segment already holds at least one record, so a lone
	// oversized record still lands whole in its own segment.
	if l.segmentBytes > 0 {
		localSize := l.written - l.active().baseByteOffset
		if localSize > 0 && localSize+uint64(buf.Len()) > l.segmentBytes {
			if err := l.rotate(); err != nil {
				l.mu.Unlock()
				return 0, 0, err
			}
		}
	}

	active := l.active()
	if _, err := active.f.Write(buf.Bytes()); err != nil {
		l.mu.Unlock()
		return 0, 0, err
	}
	l.nextMsgID++
	l.written += uint64(buf.Len())
	if rec.TsNs > active.maxTsNs {
		active.maxTsNs = rec.TsNs // newest timestamp in the segment, for age-based retention
	}
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

// rotate seals the active segment and appends a fresh one that starts
// at the current logical byte offset and next MsgID. The caller must
// hold l.mu, and rec.MsgID for the record about to be written must
// already be l.nextMsgID (so the new segment's baseMsgID equals its
// first record's MsgID).
//
// Before sealing, any bytes written to the active segment but not yet
// fsynced are made durable and committed is advanced, so a sealed
// segment is always fully durable regardless of SyncMode. The sealed
// segment intentionally keeps its append fd open (read-only from here
// on) until Log.Close or retention drops it: a concurrent group-commit
// flush may have snapshotted the old segment's Sync, and closing the fd
// out from under it would turn a harmless stray fsync into a spurious
// durability failure. Open fds are bounded by the number of retained
// segments, which retention (T5) keeps small.
func (l *Log) rotate() error {
	old := l.active()

	if l.written > l.committed.Load() {
		if err := old.f.Sync(); err != nil {
			return err
		}
		l.syncCount.Add(1)
		l.committed.Store(l.written)
		l.cond.Broadcast()
	}

	seg, err := openSegment(l.dir, old.index+1, l.nextMsgID, l.written, true)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.syncFn = seg.f.Sync
	return nil
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
// snapshots the write offset AND the active segment's Sync under mu,
// fsyncs OUTSIDE the lock so concurrent Appends can proceed during the
// fsync, then re-takes mu to publish the new committed offset. The
// snapshot is always on a record boundary (Append updates written only
// after a full record write), so committed never lands mid-record.
//
// syncFn is snapshotted under the lock because rotation repoints it;
// the snapshotted Sync is always safe to call because a sealed
// segment's fd stays open (see rotate). If a rotation raced this cycle,
// committed will already have advanced past snap and the publish below
// becomes a no-op — the next tick fsyncs the new active segment.
func (l *Log) flush() {
	l.mu.Lock()
	snap := l.written
	syncFn := l.syncFn
	if snap <= l.committed.Load() || l.commitErr != nil {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	syncErr := syncFn()

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

// Close releases the segment files. For SyncBatched it first stops the
// group committer, whose final flush makes any pending appends durable
// and releases their waiters, before the fds are closed. Safe to call
// once; further Appends after Close fail at the os.File layer. Only the
// active segment holds an open fd; sealed segments close as no-ops.
func (l *Log) Close() error {
	if l.syncMode == SyncBatched {
		l.stopOnce.Do(func() { close(l.stopCommit) })
		<-l.commitDone
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, seg := range l.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
