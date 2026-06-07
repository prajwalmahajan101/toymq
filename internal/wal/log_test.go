package wal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
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
