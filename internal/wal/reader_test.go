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
