package broker

import (
	"testing"
	"time"
)

func TestRedeliverAfterVisibilityTimeout(t *testing.T) {
	dir := t.TempDir()

	b, err := NewWithTimings(dir, 16, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWithTimings: %v", err)
	}
	defer b.Close()

	sendCh := make(chan *Inflight, 4)
	ctx := t.Context()

	if _, err := bsub(b, ctx, "orders", "c1", sendCh); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	id, dup, err := bpub(b, "orders", "", []byte("hello"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if dup {
		t.Fatal("unexpected dup")
	}

	var first *Inflight
	select {
	case first = <-sendCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first delivery")
	}
	if first.MsgID != id {
		t.Fatalf("first delivery MsgID = %d, want %d", first.MsgID, id)
	}
	if first.Attempts != 1 {
		t.Fatalf("first delivery Attempts = %d, want 1", first.Attempts)
	}

	// Deliberately do NOT ack. Wait > visibility (100ms) so the ticker
	// (20ms) fires at least once after expiry.
	var second *Inflight
	select {
	case second = <-sendCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for redelivery")
	}
	if second.MsgID != id {
		t.Fatalf("redelivered MsgID = %d, want %d", second.MsgID, id)
	}
	if second.Attempts != 2 {
		t.Fatalf("redelivered Attempts = %d, want 2", second.Attempts)
	}
}
