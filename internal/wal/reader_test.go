package wal

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReaderReadsAppendedRecords(t *testing.T) {
	dir := t.TempDir()

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, p := range payloads {
		if _, _, err := l.Append(Record{Payload: p}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	r, err := l.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i, want := range payloads {
		rec, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if rec.MsgID != uint64(i) {
			t.Errorf("Next %d: MsgID = %d, want %d", i, rec.MsgID, i)
		}
		if !bytes.Equal(rec.Payload, want) {
			t.Errorf("Next %d: Payload = %q, want = %q", i, rec.Payload, want)
		}
	}
}

func TestReaderBlocksUntilAppend(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	r, err := l.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		rec Record
		err error
	}
	done := make(chan result, 1)

	go func() {
		rec, err := r.Next(ctx)
		done <- result{rec, err}
	}()

	time.Sleep(50 * time.Millisecond)

	if _, _, err := l.Append(Record{Payload: []byte("late")}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("Next: %v", res.err)
		}
		if !bytes.Equal(res.rec.Payload, []byte("late")) {
			t.Errorf("Payload = %q, want = %q", res.rec.Payload, "late")
		}
	case <-time.After(time.Second):
		t.Fatalf("Next did not return after Append")
	}
}

func TestNewReaderMissingFile(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Append something so committed > 0; otherwise NewReader returns
	// early without touching the file.
	if _, _, err := l.Append(Record{Payload: []byte("x")}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Remove the file out from under the log. The next NewReader
	// will fail at the os.Open(l.path) call.
	if err := os.Remove(filepath.Join(dir, "000000.log")); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := l.NewReader(0); err == nil {
		t.Fatal("NewReader: expected error after file removed, got nil")
	}
}

// TestReaderSpansSegments reads a stream that rotates across several
// segments with a single reader, asserting every record is returned in
// MsgID order across the boundaries.
func TestReaderSpansSegments(t *testing.T) {
	dir := t.TempDir()
	fs := frameSize(t, Record{Payload: []byte("0123456789")})

	l, err := Open(dir, WithSegmentBytes(2*fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const n = 7
	for i := range n {
		if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if len(l.segments) < 3 {
		t.Fatalf("expected multiple segments, got %d", len(l.segments))
	}

	r, err := l.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := range uint64(n) {
		rec, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if rec.MsgID != i {
			t.Fatalf("Next %d: MsgID=%d, want %d (must span segment boundaries)", i, rec.MsgID, i)
		}
	}
}

// TestReaderStartsInLaterSegment positions a reader at a MsgID that
// lives in a non-first segment and asserts it begins exactly there.
func TestReaderStartsInLaterSegment(t *testing.T) {
	dir := t.TempDir()
	fs := frameSize(t, Record{Payload: []byte("0123456789")})

	l, err := Open(dir, WithSegmentBytes(2*fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const n = 7
	for i := range n {
		if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	const from = 5 // lives in segment index 2 (records {4,5})
	r, err := l.NewReader(from)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for want := uint64(from); want < n; want++ {
		rec, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if rec.MsgID != want {
			t.Fatalf("Next: MsgID=%d, want %d", rec.MsgID, want)
		}
	}
}

// TestReaderOutOfRangeBelowFloor simulates retention dropping the oldest
// segment, then asserts a reader starting below the new floor gets
// ErrOutOfRange rather than silently skipping ahead.
func TestReaderOutOfRangeBelowFloor(t *testing.T) {
	dir := t.TempDir()
	fs := frameSize(t, Record{Payload: []byte("0123456789")})

	l, err := Open(dir, WithSegmentBytes(2*fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	for i := range 7 {
		if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Simulate retention dropping segment 0 (records {0,1}); the new
	// floor is the next segment's baseMsgID.
	l.mu.Lock()
	dropped := l.segments[0]
	l.segments = l.segments[1:]
	floor := l.segments[0].baseMsgID
	l.mu.Unlock()
	dropped.close()

	if floor == 0 {
		t.Fatal("test setup: floor should be > 0 after dropping segment 0")
	}
	if _, err := l.NewReader(0); err != ErrOutOfRange {
		t.Fatalf("NewReader(0) below floor: err=%v, want ErrOutOfRange", err)
	}
	// At/above the floor still works.
	r, err := l.NewReader(floor)
	if err != nil {
		t.Fatalf("NewReader(floor): %v", err)
	}
	r.Close()
}

// TestReaderTailsAcrossRotation has a caught-up reader on the active
// segment; subsequent appends rotate to a new segment and the reader
// must roll forward and deliver them in order.
func TestReaderTailsAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	fs := frameSize(t, Record{Payload: []byte("0123456789")})

	l, err := Open(dir, WithSegmentBytes(2*fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	// Prime one record and read it so the reader is caught up on segment 0.
	if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	r, err := l.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if rec, err := r.Next(ctx); err != nil || rec.MsgID != 0 {
		t.Fatalf("first Next: rec=%v err=%v", rec, err)
	}

	// Append enough to force rotation while the reader tails.
	go func() {
		for range 5 {
			l.Append(Record{Payload: []byte("0123456789")})
		}
	}()

	for want := uint64(1); want <= 5; want++ {
		rec, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next %d: %v", want, err)
		}
		if rec.MsgID != want {
			t.Fatalf("Next: MsgID=%d, want %d (reader must roll across rotation)", rec.MsgID, want)
		}
	}
	if len(l.segments) < 2 {
		t.Fatalf("expected rotation to have occurred, segments=%d", len(l.segments))
	}
}

func TestReaderCancelUnblocksNext(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	r, err := l.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := r.Next(ctx)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from cancelled Next, got nil")
		}
		if err != context.Canceled {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return after cancel")
	}
}
