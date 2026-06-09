package integration

import (
	"bytes"
	"fmt"
	"testing"
)

// Scenario 1: PUB → SUB → MSG → ACK; restart; re-SUB; no redelivery.
func TestRoundTripAckSurvivesRestart(t *testing.T) {
	h := startBroker(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("only-one"))
	pubID := pub.expectOK(t)

	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	msg := sub.expectMsg(t)
	if msg.msgID != pubID {
		t.Fatalf("delivered MsgID = %d, want %d", msg.msgID, pubID)
	}
	if !bytes.Equal(msg.payload, []byte("only-one")) {
		t.Fatalf("payload = %q, want %q", msg.payload, "only-one")
	}
	sub.ack(t, "consumer-1", msg.msgID)
	_ = sub.expectOK(t)

	pub.close()
	sub.close()
	h.restart(t)

	resub := dial(t, h.addr)
	resub.sub(t, "orders", "consumer-1")
	_ = resub.expectOK(t)
	// 250ms covers >2 redelivery ticks at 20ms cadence; if any
	// resend were going to happen, it would land in this window.
	resub.expectNoMsg(t, 250_000_000) // 250ms in ns
}

// Scenario 5: PUB 1000; SUB; ACK 500; restart; re-SUB; receive 500..999.
func TestThousandMsgPartialAckResumesAfterRestart(t *testing.T) {
	const total = 1000
	const ackedThrough = 500 // ack MsgIDs 0..499 (500 messages)

	h := startBroker(t)

	pub := dial(t, h.addr)
	ids := make([]uint64, total)
	for i := 0; i < total; i++ {
		payload := []byte(fmt.Sprintf("p-%04d", i))
		pub.pub(t, "orders", "", payload)
		ids[i] = pub.expectOK(t)
	}
	pub.close()

	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	received := make(map[uint64][]byte, total)
	for i := 0; i < total; i++ {
		msg := sub.expectMsg(t)
		received[msg.msgID] = msg.payload
	}
	if len(received) != total {
		t.Fatalf("received %d distinct MsgIDs, want %d", len(received), total)
	}

	for i := 0; i < ackedThrough; i++ {
		sub.ack(t, "consumer-1", ids[i])
		_ = sub.expectOK(t)
	}
	sub.close()

	h.restart(t)

	resub := dial(t, h.addr)
	resub.sub(t, "orders", "consumer-1")
	_ = resub.expectOK(t)

	remaining := total - ackedThrough
	seen := make(map[uint64][]byte, remaining)
	for i := 0; i < remaining; i++ {
		msg := resub.expectMsg(t)
		seen[msg.msgID] = msg.payload
	}
	if len(seen) != remaining {
		t.Fatalf("post-restart got %d MsgIDs, want %d", len(seen), remaining)
	}
	for i := ackedThrough; i < total; i++ {
		payload, ok := seen[ids[i]]
		if !ok {
			t.Fatalf("missing redelivery for MsgID %d (index %d)", ids[i], i)
		}
		want := []byte(fmt.Sprintf("p-%04d", i))
		if !bytes.Equal(payload, want) {
			t.Fatalf("MsgID %d payload = %q, want %q", ids[i], payload, want)
		}
	}
	for i := 0; i < ackedThrough; i++ {
		if _, ok := seen[ids[i]]; ok {
			t.Fatalf("acked MsgID %d was redelivered after restart", ids[i])
		}
	}
}
