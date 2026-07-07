package broker

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// newBrokerDLQ builds a broker with the DLQ armed at threshold and
// timers parked (30s visibility, 1h redeliver) so tests drive nacks
// deterministically without the redelivery sweep firing.
func newBrokerDLQ(t *testing.T, threshold int) *Broker {
	t.Helper()
	b, err := newBroker(t.TempDir(), testDedupeCap, 1, defaultRecvWindow,
		30*time.Second, time.Hour, SyncConfig{}, RetentionConfig{}, threshold)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestDLQThresholdAndLoopGuard(t *testing.T) {
	b := newBrokerDLQ(t, 3)
	if got := b.dlqThreshold("orders"); got != 3 {
		t.Errorf("dlqThreshold(orders) = %d, want 3", got)
	}
	if got := b.dlqThreshold("orders.dlq"); got != 0 {
		t.Errorf("dlqThreshold(orders.dlq) = %d, want 0 (loop guard)", got)
	}
	if !isDLQTopic("orders.dlq") || isDLQTopic("orders") {
		t.Error("isDLQTopic misclassified")
	}

	// Disabled globally → 0 even for a normal topic.
	off := newBrokerDLQ(t, 0)
	if got := off.dlqThreshold("orders"); got != 0 {
		t.Errorf("dlqThreshold with DLQ off = %d, want 0", got)
	}
}

func TestNackOrKillDecision(t *testing.T) {
	b := newBrokerDLQ(t, 2)
	if _, _, err := bpub(b, "orders", "", []byte("p")); err != nil {
		t.Fatalf("pub: %v", err)
	}
	p := b.topics["orders"].partitions[0]
	c := p.getOrCreateConsumer("c1")

	// Seed an inflight at Attempts=1: not yet dead (threshold 2).
	c.mu.Lock()
	c.inflight[0] = &Inflight{MsgID: 0, Topic: "orders", Payload: []byte("p"), Attempts: 1}
	c.mu.Unlock()

	redeliver, killed, err := c.nackOrKill(0, 2)
	if err != nil || killed != nil || redeliver == nil {
		t.Fatalf("first nack: redeliver=%v killed=%v err=%v; want redeliver", redeliver, killed, err)
	}
	if redeliver.Attempts != 2 {
		t.Errorf("redeliver Attempts = %d, want 2", redeliver.Attempts)
	}

	// Now at Attempts=2 == threshold: the next nack kills it.
	redeliver, killed, err = c.nackOrKill(0, 2)
	if err != nil || redeliver != nil || killed == nil {
		t.Fatalf("second nack: redeliver=%v killed=%v err=%v; want killed", redeliver, killed, err)
	}
	c.mu.Lock()
	_, stillInflight := c.inflight[0]
	c.mu.Unlock()
	if stillInflight {
		t.Error("killed message must be removed from inflight (synthetic ack)")
	}
}

// TestNackDeadLettersEndToEnd drives a message past the nack threshold
// and asserts it lands on <topic>.dlq, stops redelivering on the source,
// and that the .dlq topic is not itself dead-lettered.
func TestNackDeadLettersEndToEnd(t *testing.T) {
	b := newBrokerDLQ(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := []byte("poison")
	if _, _, err := bpub(b, "orders", "", payload); err != nil {
		t.Fatalf("pub: %v", err)
	}

	ch := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("sub: %v", err)
	}

	// Delivery 1 (Attempts 1) → nack → redelivery (Attempts 2) → nack → dead.
	first := recvInflight(t, ch, time.Second)
	if first.Attempts != 1 {
		t.Fatalf("first delivery Attempts = %d, want 1", first.Attempts)
	}
	if err := bnack(b, "orders", "c1", 0, ch); err != nil {
		t.Fatalf("nack 1: %v", err)
	}
	second := recvInflight(t, ch, time.Second)
	if second.Attempts != 2 {
		t.Fatalf("redelivery Attempts = %d, want 2", second.Attempts)
	}
	if err := bnack(b, "orders", "c1", 0, ch); err != nil {
		t.Fatalf("nack 2: %v", err)
	}

	// Source consumer must not redeliver again.
	select {
	case inf := <-ch:
		t.Fatalf("unexpected redelivery after dead-letter: %+v", inf)
	case <-time.After(200 * time.Millisecond):
	}
	p := b.topics["orders"].partitions[0]
	c := p.getOrCreateConsumer("c1")
	c.mu.Lock()
	n := len(c.inflight)
	c.mu.Unlock()
	if n != 0 {
		t.Errorf("source inflight = %d, want 0 after dead-letter", n)
	}

	// The dead message is on orders.dlq (1 partition), payload intact.
	dlqCh := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders.dlq", "d1", dlqCh); err != nil {
		t.Fatalf("sub dlq: %v", err)
	}
	dead := recvInflight(t, dlqCh, time.Second)
	if !bytes.Equal(dead.Payload, payload) {
		t.Errorf("dlq payload = %q, want %q", dead.Payload, payload)
	}
	if got := b.topics["orders.dlq"].count(); got != 1 {
		t.Errorf("orders.dlq partitions = %d, want 1", got)
	}
}
