package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestAppendAndReopen(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := range 3 {
		id, _, err := l.Append(Record{Payload: []byte("msg")})
		if err != nil {
			t.Fatalf("Append %d:%v", i, err)
		}

		if id != uint64(i) {
			t.Errorf("Append %d: got id=%d, want %d", i, id, i)
		}
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()

	if l2.nextMsgID != 3 {
		t.Errorf("After reopen: nextMsgID = %d, want 3", l2.nextMsgID)
	}

	id, _, err := l2.Append(Record{Payload: []byte("after-reopen")})
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if id != 3 {
		t.Errorf("Append after reopem: got id=%d, want 3", id)
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 5 {
		if _, _, err := l.Append(Record{Payload: []byte("payload")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	sizeBefore := int64(l.committed.Load())
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, "000000.log")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("garbage")); err != nil {
		t.Fatalf("Write garbage: %v", err)
	}
	f.Close()

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != sizeBefore {
		t.Errorf("file size after recovery: got %d, want %d", fi.Size(), sizeBefore)
	}
	if l2.nextMsgID != 5 {
		t.Errorf("nextMsgID after recovery: got %d, want 5", l2.nextMsgID)
	}
}

func TestOpenMkdirFails(t *testing.T) {
	// Create a regular file, then try to Open a path that treats it
	// as a parent directory. os.MkdirAll returns "not a directory".
	parent := t.TempDir()
	asFile := filepath.Join(parent, "blocker")
	if err := os.WriteFile(asFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := Open(filepath.Join(asFile, "topic")); err == nil {
		t.Fatal("Open: expected error, got nil")
	}
}

func TestAppendOnClosedLog(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := l.Append(Record{Payload: []byte("after-close")}); err == nil {
		t.Fatal("Append on closed log: expected error, got nil")
	}
}

func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const writers = 8
	const perWriter = 100

	var wg sync.WaitGroup
	seen := make(map[uint64]struct{})
	var seenMu sync.Mutex

	for range writers {
		wg.Go(func() {
			for range perWriter {
				id, _, err := l.Append(Record{Payload: []byte("p")})
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				seenMu.Lock()
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate id: %d", id)
				}
				seen[id] = struct{}{}
				seenMu.Unlock()
			}
		})
	}

	wg.Wait()

	if len(seen) != writers*perWriter {
		t.Errorf("total ids: got %d, want %d", len(seen), writers*perWriter)
	}
	for i := range uint64(writers * perWriter) {
		if _, ok := seen[i]; !ok {
			t.Errorf("missing id %d", i)
		}
	}
}

func TestRecoveryVisitorSeesGoodRecordsInOrder(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Mix keyed and unkeyed records; the visitor must see every good
	// record in ascending MsgID order regardless of key.
	want := []Record{
		{DedupeKey: "a", Payload: []byte("0")},
		{Payload: []byte("1")},
		{DedupeKey: "b", Payload: []byte("2")},
		{DedupeKey: "c", Payload: []byte("3")},
	}
	for i, rec := range want {
		if _, _, err := l.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []Record
	l2, err := Open(dir, WithRecoveryVisitor(func(rec Record) {
		got = append(got, rec)
	}))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()

	if len(got) != len(want) {
		t.Fatalf("visited %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].MsgID != uint64(i) {
			t.Errorf("record %d: MsgID = %d, want %d", i, got[i].MsgID, i)
		}
		if got[i].DedupeKey != want[i].DedupeKey {
			t.Errorf("record %d: DedupeKey = %q, want %q", i, got[i].DedupeKey, want[i].DedupeKey)
		}
	}
}

func TestRecoveryVisitorSkipsTornTail(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 3 {
		if _, _, err := l.Append(Record{DedupeKey: "k", Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a torn frame past the last good record.
	path := filepath.Join(dir, "000000.log")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("garbage")); err != nil {
		t.Fatalf("Write garbage: %v", err)
	}
	f.Close()

	var count int
	l2, err := Open(dir, WithRecoveryVisitor(func(Record) { count++ }))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()

	// Exactly the 3 good records, never the truncated tail.
	if count != 3 {
		t.Errorf("visited %d records, want 3 (torn tail must not be visited)", count)
	}
}

// --- v2 M2: batched-fsync (ADR 0019) ---

// waitWritten spins until the Log has buffered `n` total bytes written
// (i.e. `wait` appenders have written and are blocked on the committer),
// so batched tests can drive flush() deterministically without racing.
func waitWritten(t *testing.T, l *Log, atLeast uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		l.mu.Lock()
		w := l.written
		l.mu.Unlock()
		if w >= atLeast {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("written=%d never reached %d", w, atLeast)
		}
		time.Sleep(time.Millisecond)
	}
}

// In batched mode a record is invisible to readers (committed does not
// advance) until the group committer fsyncs; only then does Append
// return. A long interval keeps the ticker out of the way so the test
// drives flush() by hand.
func TestBatchedRecordInvisibleUntilFlush(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncMode(SyncBatched, time.Hour))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	done := make(chan uint64, 1)
	go func() {
		id, _, err := l.Append(Record{DedupeKey: "k", Payload: []byte("v")})
		if err != nil {
			t.Errorf("Append: %v", err)
		}
		done <- id
	}()

	// Bytes are written but not yet committed: no fsync has happened.
	waitWritten(t, l, 1)
	if c := l.committed.Load(); c != 0 {
		t.Fatalf("committed = %d before flush, want 0 (record must be invisible)", c)
	}
	select {
	case <-done:
		t.Fatal("Append returned before flush — un-fsynced record was acked")
	case <-time.After(50 * time.Millisecond):
	}

	before := l.syncCount.Load()
	l.flush()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Append did not return after flush")
	}
	if l.committed.Load() == 0 {
		t.Fatal("committed did not advance after flush")
	}
	if got := l.syncCount.Load() - before; got != 1 {
		t.Fatalf("flush did %d fsyncs, want 1", got)
	}
}

// N concurrent appends within one commit window coalesce into a single
// fsync — the whole point of batching.
func TestBatchedAppendsCoalesceIntoOneFsync(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncMode(SyncBatched, time.Hour))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const n = 8
	// Every record here is identical in size (1-byte payload, no key),
	// so the barrier must wait for all n frames' bytes — not n bytes —
	// before flushing, otherwise flush() would fire while later
	// appenders haven't written yet and they'd block until the (1h)
	// ticker.
	var oneFrame bytes.Buffer
	if err := Encode(Record{Payload: []byte{0}}, &oneFrame); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	frameSize := uint64(oneFrame.Len())

	var wg sync.WaitGroup
	perRecord := make(chan uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, end, err := l.Append(Record{Payload: []byte{byte(i)}})
			if err != nil {
				t.Errorf("Append %d: %v", i, err)
			}
			perRecord <- end
		}(i)
	}

	// Wait until all n have written, then one flush must release all of
	// them with exactly one fsync.
	waitWritten(t, l, n*frameSize)
	before := l.syncCount.Load()
	l.flush()
	wg.Wait()

	if got := l.syncCount.Load() - before; got != 1 {
		t.Fatalf("%d appends took %d fsyncs, want 1 (no coalescing)", n, got)
	}
	if len(perRecord) != n {
		t.Fatalf("only %d/%d appends returned", len(perRecord), n)
	}
}

// SyncNone advances committed immediately and never fsyncs.
func TestNoneModeVisibleWithoutFsync(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncMode(SyncNone, 0))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	id, end, err := l.Append(Record{Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if id != 0 || end == 0 {
		t.Fatalf("Append = (%d,%d), want (0, >0)", id, end)
	}
	if l.committed.Load() != end {
		t.Fatalf("committed = %d, want %d (none must advance immediately)", l.committed.Load(), end)
	}
	if l.syncCount.Load() != 0 {
		t.Fatalf("none mode did %d fsyncs, want 0", l.syncCount.Load())
	}
}

// Close's final flush makes a pending batched append durable and
// releases its waiter — graceful shutdown loses no acked data.
func TestBatchedCloseFlushesPending(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncMode(SyncBatched, time.Hour))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := l.Append(Record{Payload: []byte("pending")})
		done <- err
	}()
	waitWritten(t, l, 1)

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pending Append errored on close-flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending Append never returned after Close")
	}

	// Reopen: the record must be recovered (it was fsynced by the final flush).
	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()
	if l2.nextMsgID != 1 {
		t.Fatalf("nextMsgID after reopen = %d, want 1 (record not durable)", l2.nextMsgID)
	}
}

func TestParseSyncMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want SyncMode
		ok   bool
	}{
		{"per-message", SyncPerMessage, true},
		{"batched", SyncBatched, true},
		{"none", SyncNone, true},
		{"nonsense", 0, false},
		{"", 0, false},
	} {
		got, err := ParseSyncMode(tc.in)
		if tc.ok && (err != nil || got != tc.want) {
			t.Errorf("ParseSyncMode(%q) = (%v,%v), want (%v,nil)", tc.in, got, err, tc.want)
		}
		if !tc.ok && err == nil {
			t.Errorf("ParseSyncMode(%q): expected error", tc.in)
		}
	}
}
