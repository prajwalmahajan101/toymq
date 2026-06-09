package integration

import (
	"bytes"
	"testing"
)

// Scenario 8: client receives a MSG, drops the connection without
// ACKing, then reconnects and re-subscribes. The unacked message
// must be re-delivered on the new subscription.
//
// Per ADR 0010, the redelivery sweep skips consumers with sub==nil
// (which is the state after the old session tore down), so the
// resend is driven by the fresh SUB's WAL tail rather than by the
// visibility ticker. The client therefore sees Attempts == 1 on the
// re-delivered MSG (we don't have wire access to Attempts, so the
// observable assertion is "same MsgID, same payload, no ticker
// wait required").
func TestUnackedMessageResumesAfterDisconnect(t *testing.T) {
	h := startBroker(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("resume-me"))
	id := pub.expectOK(t)
	pub.close()

	first := dial(t, h.addr)
	first.sub(t, "orders", "consumer-1")
	_ = first.expectOK(t)

	got := first.expectMsg(t)
	if got.msgID != id {
		t.Fatalf("first delivery MsgID = %d, want %d", got.msgID, id)
	}
	if !bytes.Equal(got.payload, []byte("resume-me")) {
		t.Fatalf("first payload = %q, want %q", got.payload, "resume-me")
	}
	// Drop the conn without acking. Server's reader hits EOF, the
	// session tears down its subscription. The inflight stays in
	// Consumer.inflight; lastAcked / hasAcked are unchanged.
	first.close()

	resub := dial(t, h.addr)
	resub.sub(t, "orders", "consumer-1")
	_ = resub.expectOK(t)

	again := resub.expectMsg(t)
	if again.msgID != id {
		t.Fatalf("re-delivered MsgID = %d, want %d", again.msgID, id)
	}
	if !bytes.Equal(again.payload, []byte("resume-me")) {
		t.Fatalf("re-delivered payload = %q, want %q", again.payload, "resume-me")
	}

	resub.ack(t, "consumer-1", again.msgID)
	_ = resub.expectOK(t)
}
