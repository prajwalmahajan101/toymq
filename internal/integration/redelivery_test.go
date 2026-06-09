package integration

import (
	"bytes"
	"testing"
	"time"
)

// Scenario 2: PUB -> SUB -> MSG; no ACK; wait > visibility; receive
// again. Default harness uses 100ms visibility / 20ms ticker.
func TestRedeliverAfterVisibilityTimeout(t *testing.T) {
	h := startBroker(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("v"))
	_ = pub.expectOK(t)

	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	first := sub.expectMsg(t)
	if !bytes.Equal(first.payload, []byte("v")) {
		t.Fatalf("first payload = %q, want %q", first.payload, "v")
	}

	// Wait > visibility (100ms) so the ticker (20ms) fires at least
	// once after expiry. expectMsg uses a 2s read deadline so the
	// blocked read returns as soon as the redelivery is pushed.
	second := sub.expectMsg(t)
	if second.msgID != first.msgID {
		t.Fatalf("redelivered MsgID = %d, want %d", second.msgID, first.msgID)
	}
}

// Scenario 3: PUB -> SUB -> MSG -> NACK; receive again immediately
// (without waiting for the visibility timeout).
func TestNackRedeliversImmediately(t *testing.T) {
	h := startBroker(t,
		withVisibility(5*time.Second), // long enough that the ticker can't be the cause
		withRedeliverInterval(1*time.Second),
	)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("retry-me"))
	_ = pub.expectOK(t)

	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	first := sub.expectMsg(t)

	start := time.Now()
	sub.nack(t, "consumer-1", first.msgID)
	_ = sub.expectOK(t)

	second := sub.expectMsg(t)
	elapsed := time.Since(start)

	if second.msgID != first.msgID {
		t.Fatalf("redelivered MsgID = %d, want %d", second.msgID, first.msgID)
	}
	if !bytes.Equal(second.payload, first.payload) {
		t.Fatalf("redelivered payload = %q, want %q", second.payload, first.payload)
	}
	// NACK must redeliver before the next ticker pass (1s here).
	if elapsed > 500*time.Millisecond {
		t.Fatalf("NACK redelivery took %s; expected sub-second", elapsed)
	}
}
