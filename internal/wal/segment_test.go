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

// TestMultiSegmentRecovery writes across several segments, reopens, and
// asserts every record is recovered in MsgID order across the segment
// boundaries, with nextMsgID and each segment's base fields restored.
func TestMultiSegmentRecovery(t *testing.T) {
	dir := t.TempDir()

	fs := frameSize(t, Record{Payload: []byte("0123456789")})
	capBytes := 2 * fs // two records per segment

	l, err := Open(dir, WithSegmentBytes(capBytes))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 7
	for i := range n {
		if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	wantSegments := len(l.segments)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and collect recovered records via the visitor.
	var got []uint64
	l2, err := Open(dir, WithSegmentBytes(capBytes),
		WithRecoveryVisitor(func(rec Record) { got = append(got, rec.MsgID) }))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()

	if len(got) != n {
		t.Fatalf("recovered %d records, want %d", len(got), n)
	}
	for i := range uint64(n) {
		if got[i] != i {
			t.Fatalf("recovered MsgID[%d] = %d, want %d (order must hold across segments)", i, got[i], i)
		}
	}
	if l2.nextMsgID != n {
		t.Errorf("nextMsgID after recovery = %d, want %d", l2.nextMsgID, n)
	}
	if len(l2.segments) != wantSegments {
		t.Fatalf("segment count after recovery = %d, want %d", len(l2.segments), wantSegments)
	}
	// Base fields must be reconstructed from the scan: 2 records/segment.
	for i, seg := range l2.segments {
		if want := uint64(i) * 2; seg.baseMsgID != want {
			t.Errorf("segment[%d].baseMsgID = %d, want %d", i, seg.baseMsgID, want)
		}
		if want := uint64(i) * capBytes; seg.baseByteOffset != want {
			t.Errorf("segment[%d].baseByteOffset = %d, want %d", i, seg.baseByteOffset, want)
		}
	}

	// A further append continues the MsgID sequence and the byte stream.
	id, _, err := l2.Append(Record{Payload: []byte("0123456789")})
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if id != n {
		t.Errorf("Append after recovery: id=%d, want %d", id, n)
	}
}

// TestTornTailTruncatesActiveSegmentOnly corrupts the tail of the active
// segment after a multi-segment run and asserts recovery truncates only
// that segment, leaving sealed segments and their records intact.
func TestTornTailTruncatesActiveSegmentOnly(t *testing.T) {
	dir := t.TempDir()

	fs := frameSize(t, Record{Payload: []byte("0123456789")})
	capBytes := 2 * fs

	l, err := Open(dir, WithSegmentBytes(capBytes))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 5 // → segments {0,1}, {2,3}, {4}: active is 000002.log
	for i := range n {
		if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	activeIdx := uint64(len(l.segments) - 1)
	sealedSizeBefore := segFileSize(t, dir, 0)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append garbage to the active segment only.
	af, err := os.OpenFile(filepath.Join(dir, segmentName(activeIdx)), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open active: %v", err)
	}
	if _, err := af.Write([]byte("garbage")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	af.Close()

	var got []uint64
	l2, err := Open(dir, WithSegmentBytes(capBytes),
		WithRecoveryVisitor(func(rec Record) { got = append(got, rec.MsgID) }))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer l2.Close()

	// All 5 real records recovered; garbage dropped.
	if len(got) != n || l2.nextMsgID != n {
		t.Fatalf("recovered=%v nextMsgID=%d, want %d records", got, l2.nextMsgID, n)
	}
	// Sealed segment 0 untouched; active segment truncated back to one record.
	if after := segFileSize(t, dir, 0); after != sealedSizeBefore {
		t.Errorf("sealed segment 0 size changed: got %d, want %d", after, sealedSizeBefore)
	}
	if got := segFileSize(t, dir, activeIdx); got != int64(fs) {
		t.Errorf("active segment size after recovery = %d, want %d (garbage truncated)", got, fs)
	}
}

// TestCorruptionInSealedSegmentIsFatal proves recovery refuses to
// silently truncate a sealed segment: a bad frame there is real damage,
// not an interrupted write, and must surface as an error.
func TestCorruptionInSealedSegmentIsFatal(t *testing.T) {
	dir := t.TempDir()

	fs := frameSize(t, Record{Payload: []byte("0123456789")})
	l, err := Open(dir, WithSegmentBytes(2*fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 5 { // ensure segment 0 is sealed
		if _, _, err := l.Append(Record{Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if len(l.segments) < 2 {
		t.Fatalf("need a sealed segment; got %d", len(l.segments))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte inside sealed segment 0 to break its CRC.
	p := filepath.Join(dir, segmentName(0))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := Open(dir, WithSegmentBytes(2*fs)); err == nil {
		t.Fatal("Open: expected error on sealed-segment corruption, got nil")
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
