package broker

import (
	"context"
	"testing"
	"time"
)

// bpubDelay publishes to partition 0 with a delivery delay (ms).
func bpubDelay(b *Broker, topic string, payload []byte, delayMs uint64) (uint64, error) {
	id, _, _, err := b.PublishCtx(context.Background(), topic, "", "", 0, false, payload, delayMs)
	return id, err
}

// TestDelayedDeliveryHoldsUntilVisible asserts a DELAYed message is not
// delivered before its visible-at time and is delivered shortly after.
func TestDelayedDeliveryHoldsUntilVisible(t *testing.T) {
	b := newTestBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const delayMs = 250
	if _, err := bpubDelay(b, "orders", []byte("later"), delayMs); err != nil {
		t.Fatalf("pub: %v", err)
	}

	ch := make(chan *Inflight, 4)
	start := time.Now()
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("sub: %v", err)
	}

	// Must not arrive well before the delay.
	select {
	case inf := <-ch:
		t.Fatalf("delivered after %v, before the %dms delay: %+v", time.Since(start), delayMs, inf)
	case <-time.After(delayMs/2*time.Millisecond - 10*time.Millisecond):
	}

	// Must arrive after the delay elapses.
	inf := recvInflight(t, ch, time.Second)
	if elapsed := time.Since(start); elapsed < (delayMs-60)*time.Millisecond {
		t.Errorf("delivered too early: %v < ~%dms", elapsed, delayMs)
	}
	if string(inf.Payload) != "later" {
		t.Errorf("payload = %q, want %q", inf.Payload, "later")
	}
}

// TestDelayedPreservesOrder confirms a delayed record holds the
// partition's delivery: a later immediate publish does not jump ahead of
// an earlier delayed one (head-of-line by design).
func TestDelayedPreservesOrder(t *testing.T) {
	b := newTestBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// MsgID 0 delayed, MsgID 1 immediate — same partition.
	if _, err := bpubDelay(b, "orders", []byte("first-delayed"), 200); err != nil {
		t.Fatalf("pub delayed: %v", err)
	}
	if _, _, err := bpub(b, "orders", "", []byte("second-immediate")); err != nil {
		t.Fatalf("pub immediate: %v", err)
	}

	ch := make(chan *Inflight, 4)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("sub: %v", err)
	}

	first := recvInflight(t, ch, time.Second)
	if first.MsgID != 0 || string(first.Payload) != "first-delayed" {
		t.Fatalf("first delivered = MsgID %d %q, want MsgID 0 first-delayed (order must hold)", first.MsgID, first.Payload)
	}
	second := recvInflight(t, ch, time.Second)
	if second.MsgID != 1 {
		t.Fatalf("second delivered = MsgID %d, want 1", second.MsgID)
	}
}

// TestDelayedDeliverySurvivesRestart proves the delay is persisted: the
// VisibleAtNs is in the WAL, so a broker restart re-parks rather than
// delivering early. Here we just confirm a delayed record recovers and
// is readable with its VisibleAtNs intact.
func TestDelayedRecordPersistsVisibleAt(t *testing.T) {
	b := newTestBroker(t)
	if _, err := bpubDelay(b, "orders", []byte("p"), 10_000); err != nil {
		t.Fatalf("pub: %v", err)
	}
	p := b.topics["orders"].partitions[0]
	r, err := p.log.NewReader(0)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rec, err := r.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if rec.VisibleAtNs == 0 {
		t.Error("VisibleAtNs must be persisted in the WAL for a delayed record")
	}
}
