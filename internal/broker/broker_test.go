package broker

import (
	"testing"
	"time"
)

const testDedupeCap = 100

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	b, err := New(t.TempDir(), testDedupeCap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func mustPublish(t *testing.T, b *Broker, topic, key string, payload []byte) uint64 {
	t.Helper()
	id, dup, err := b.Publish(topic, key, payload)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if dup {
		t.Fatalf("Publish restured dedp=true unexpectedly")
	}
	return id
}

func recvInflight(t *testing.T, ch <-chan *Inflight, timeout time.Duration) *Inflight {
	t.Helper()
	select {
	case inf := <-ch:
		return inf
	case <-time.After(timeout):
		t.Fatal("timed out waiting for inflight")
		return nil
	}
}

func TestSubscribeReceivesNewPublish(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := b.Subscribe(ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	id := mustPublish(t, b, "orders", "", []byte("hello"))

	inf := recvInflight(t, ch, time.Second)

	if inf.MsgID != id {
		t.Errorf("MsgID = %d, want %d", inf.MsgID, id)
	}

	if string(inf.Payload) != "hello" {
		t.Errorf("Payload =%q, want %q", inf.Payload, "hello")
	}
}

func TestSubscribeReadsBacklog(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	id0 := mustPublish(t, b, "orders", "", []byte("first"))
	id1 := mustPublish(t, b, "orders", "", []byte("second"))

	ch := make(chan *Inflight, 8)
	if _, err := b.Subscribe(ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	got0 := recvInflight(t, ch, time.Second)
	got1 := recvInflight(t, ch, time.Second)
	if got0.MsgID != id0 || got1.MsgID != id1 {
		t.Errorf("got msg ids (%d, %d), want (%d, %d)", got0.MsgID, got1.MsgID, id0, id1)
	}
}

func TestAckAdvancesLastAcked(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := b.Subscribe(ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	mustPublish(t, b, "orders", "", []byte("a"))
	mustPublish(t, b, "orders", "", []byte("b"))
	mustPublish(t, b, "orders", "", []byte("c"))

	for i := range uint64(3) {
		inf := recvInflight(t, ch, time.Second)
		if inf.MsgID != i {
			t.Fatalf("got msg %d, want %d", inf.MsgID, i)
		}
		if err := b.Ack("orders", "c1", inf.MsgID); err != nil {
			t.Fatalf("Ack %d: %v", inf.MsgID, err)
		}
	}

	// Reach into the consumer to verify lastAcked.
	topic, _ := b.getOrCreateTopic("orders")
	c := topic.getOrCreateConsumer("c1")
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastAcked != 2 {
		t.Errorf("lastAcked = %d, want 2", c.lastAcked)
	}
	if len(c.inflight) != 0 {
		t.Errorf("inflight not empty: %d entries", len(c.inflight))
	}
	if len(c.aboveLast) != 0 {
		t.Errorf("aboveLast not empty: %d entries", len(c.aboveLast))
	}
}

func TestAckOutOfOrderDrains(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := b.Subscribe(ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for range 5 {
		mustPublish(t, b, "orders", "", []byte("x"))
	}
	for range 5 {
		recvInflight(t, ch, time.Second)
	}

	// Ack out of order: 2, 0, 1 — then 4, 3.
	for _, id := range []uint64{2, 0, 1} {
		if err := b.Ack("orders", "c1", id); err != nil {
			t.Fatalf("Ack %d: %v", id, err)
		}
	}
	topic, _ := b.getOrCreateTopic("orders")
	c := topic.getOrCreateConsumer("c1")
	c.mu.Lock()
	got := c.lastAcked
	c.mu.Unlock()
	if got != 2 {
		t.Fatalf("after acking 2,0,1: lastAcked = %d, want 2", got)
	}

	for _, id := range []uint64{4, 3} {
		if err := b.Ack("orders", "c1", id); err != nil {
			t.Fatalf("Ack %d: %v", id, err)
		}
	}
	c.mu.Lock()
	got = c.lastAcked
	drained := len(c.aboveLast)
	c.mu.Unlock()
	if got != 4 {
		t.Errorf("after all acks: lastAcked = %d, want 4", got)
	}
	if drained != 0 {
		t.Errorf("aboveLast = %d entries, want 0", drained)
	}
}

func TestPublishDedupeReturnsOriginalID(t *testing.T) {
	b := newTestBroker(t)

	id1, dup1, err := b.Publish("orders", "key-A", []byte("first"))
	if err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	if dup1 {
		t.Errorf("first publish: dup = true, want false")
	}

	id2, dup2, err := b.Publish("orders", "key-A", []byte("second"))
	if err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if !dup2 {
		t.Errorf("second publish: dup = false, want true")
	}
	if id2 != id1 {
		t.Errorf("second publish: id = %d, want %d (original)", id2, id1)
	}
}
