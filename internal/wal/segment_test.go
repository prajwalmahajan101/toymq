package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// frameSize returns the exact on-disk byte size of one encoded record
// with the given payload/key, so rotation tests can pick thresholds
// without hard-coding the frame layout.
func frameSize(t *testing.T, rec Record) uint64 {
	t.Helper()
	var buf bytes.Buffer
	if err := Encode(rec, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return uint64(buf.Len())
}

func segFileSize(t *testing.T, dir string, index uint64) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, segmentName(index)))
	if err != nil {
		t.Fatalf("stat segment %d: %v", index, err)
	}
	return fi.Size()
}

// TestSegmentRotation drives the active segment past --segment-bytes on
// a record boundary and asserts it seals and rolls to a fresh segment
// with MsgIDs continuous across the boundary and each sealed segment
// within the cap.
func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	rec := Record{Payload: []byte("0123456789")} // fixed-width frame
	fs := frameSize(t, rec)                      // bytes per record on disk
	// Cap fits two records but not three: rotate on every 3rd append.
	capBytes := 2*fs + fs/2

	l, err := Open(dir, WithSegmentBytes(capBytes))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const n = 5
	for i := range n {
		id, _, err := l.Append(Record{Payload: []byte("0123456789")})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if id != uint64(i) {
			t.Fatalf("Append %d: id=%d, want %d (MsgID must stay monotonic across segments)", i, id, i)
		}
	}

	// 5 records, 2 per segment before rotation → segments hold {0,1},
	// {2,3}, {4}: three segments in all.
	if got := len(l.segments); got != 3 {
		t.Fatalf("segment count = %d, want 3", got)
	}

	wantBaseMsgID := []uint64{0, 2, 4}
	wantBaseByte := []uint64{0, 2 * fs, 4 * fs}
	for i, seg := range l.segments {
		if seg.index != uint64(i) {
			t.Errorf("segment[%d].index = %d, want %d", i, seg.index, i)
		}
		if seg.baseMsgID != wantBaseMsgID[i] {
			t.Errorf("segment[%d].baseMsgID = %d, want %d", i, seg.baseMsgID, wantBaseMsgID[i])
		}
		if seg.baseByteOffset != wantBaseByte[i] {
			t.Errorf("segment[%d].baseByteOffset = %d, want %d", i, seg.baseByteOffset, wantBaseByte[i])
		}
	}

	// Each sealed segment holds exactly two records and stays within cap;
	// the active segment holds the final record.
	for i, wantRecs := range []int64{2, 2, 1} {
		if got, want := segFileSize(t, dir, uint64(i)), wantRecs*int64(fs); got != want {
			t.Errorf("segment %d on-disk size = %d, want %d", i, got, want)
		}
		if i < 2 && int64(capBytes) < segFileSize(t, dir, uint64(i)) {
			t.Errorf("sealed segment %d exceeds cap %d", i, capBytes)
		}
	}
}

// TestSegmentRotationOversizedRecord proves a single record larger than
// the cap still lands whole in its own segment — rotation never splits
// a record.
func TestSegmentRotationOversizedRecord(t *testing.T) {
	dir := t.TempDir()

	rec := Record{Payload: []byte("this record is far larger than the cap")}
	fs := frameSize(t, rec)

	// Cap smaller than one record: every append after the first rotates.
	l, err := Open(dir, WithSegmentBytes(fs/4))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const n = 3
	for i := range n {
		if _, _, err := l.Append(Record{Payload: []byte("this record is far larger than the cap")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if got := len(l.segments); got != n {
		t.Fatalf("segment count = %d, want %d (each oversized record its own segment)", got, n)
	}
	for i := range n {
		if got, want := segFileSize(t, dir, uint64(i)), int64(fs); got != want {
			t.Errorf("segment %d size = %d, want %d (record must land whole)", i, got, want)
		}
	}
}

// TestSegmentRotationBatchedConcurrent hammers rotation while the group
// committer runs, so -race exercises the rotate/flush interplay: rotate
// repoints syncFn under mu, flush snapshots it under mu and fsyncs a
// sealed-but-still-open fd. Every append must return durable, MsgIDs
// must be dense 0..N-1, and several segments must be created.
func TestSegmentRotationBatchedConcurrent(t *testing.T) {
	dir := t.TempDir()

	fs := frameSize(t, Record{Payload: []byte("payload")})
	l, err := Open(dir, WithSyncMode(SyncBatched, time.Millisecond), WithSegmentBytes(3*fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	const n = 200
	var wg sync.WaitGroup
	ids := make([]uint64, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, _, err := l.Append(Record{Payload: []byte("payload")})
			if err != nil {
				t.Errorf("Append: %v", err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool, n)
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate MsgID %d assigned", id)
		}
		seen[id] = true
	}
	for i := range uint64(n) {
		if !seen[i] {
			t.Fatalf("MsgID %d missing — assignment not dense across rotation", i)
		}
	}
	if len(l.segments) < 2 {
		t.Fatalf("segment count = %d, want >=2 (rotation should have fired)", len(l.segments))
	}
	// committed must cover every byte written (all appends returned).
	if l.committed.Load() != l.written {
		t.Fatalf("committed=%d written=%d: not all appends durable", l.committed.Load(), l.written)
	}
}

// TestNoRotationWhenDisabled confirms the zero-value cap keeps a single
// ever-growing segment — the pre-M6 behaviour for callers that omit
// WithSegmentBytes.
func TestNoRotationWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir) // no WithSegmentBytes → rotation disabled
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	for i := range 20 {
		if _, _, err := l.Append(Record{Payload: []byte("payload")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if got := len(l.segments); got != 1 {
		t.Fatalf("segment count = %d, want 1 (rotation disabled)", got)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); !os.IsNotExist(err) {
		t.Fatalf("segment 000001.log must not exist when rotation disabled (err=%v)", err)
	}
}
