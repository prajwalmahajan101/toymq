package integration

import (
	"bytes"
	"testing"
	"time"
)

// Scenario 4: PUB key=A twice. Second returns DUP <original_id>; only
// one MSG is ever delivered.
func TestPubDedupeReturnsOriginalAndDeliversOnce(t *testing.T) {
	h := startBroker(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "key-A", []byte("first"))
	originalID := pub.expectOK(t)

	pub.pub(t, "orders", "key-A", []byte("second"))
	// Session.handlePub writes DUP then OK on a duplicate. The OK
	// also echoes the original ID. Drain both.
	dupID := pub.expectDup(t)
	if dupID != originalID {
		t.Fatalf("DUP id = %d, want %d", dupID, originalID)
	}
	if okID := pub.expectOK(t); okID != originalID {
		t.Fatalf("OK after DUP id = %d, want %d", okID, originalID)
	}

	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	msg := sub.expectMsg(t)
	if msg.msgID != originalID {
		t.Fatalf("delivered MsgID = %d, want %d", msg.msgID, originalID)
	}
	if !bytes.Equal(msg.payload, []byte("first")) {
		t.Fatalf("delivered payload = %q, want %q", msg.payload, "first")
	}
	sub.ack(t, "consumer-1", msg.msgID)
	_ = sub.expectOK(t)

	sub.expectNoMsg(t, 200*time.Millisecond)
}

// Scenario 6: SUB consumer X on conn A; second SUB consumer X on
// conn B. The first session is detached; the second receives
// subsequent messages.
func TestSecondSubscribeDetachesFirst(t *testing.T) {
	h := startBroker(t,
		withVisibility(5*time.Second),
		withRedeliverInterval(1*time.Second),
	)

	first := dial(t, h.addr)
	first.sub(t, "orders", "consumer-1")
	_ = first.expectOK(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("for-first"))
	id1 := pub.expectOK(t)

	got1 := first.expectMsg(t)
	if got1.msgID != id1 {
		t.Fatalf("first delivery MsgID = %d, want %d", got1.msgID, id1)
	}
	first.ack(t, "consumer-1", got1.msgID)
	_ = first.expectOK(t)

	second := dial(t, h.addr)
	second.sub(t, "orders", "consumer-1")
	_ = second.expectOK(t)

	pub.pub(t, "orders", "", []byte("for-second"))
	id2 := pub.expectOK(t)

	got2 := second.expectMsg(t)
	if got2.msgID != id2 {
		t.Fatalf("second-sub delivery MsgID = %d, want %d", got2.msgID, id2)
	}

	// The first conn must NOT receive the post-takeover message.
	// Long-enough window to make sure no MSG sneaks in.
	first.expectNoMsg(t, 200*time.Millisecond)
}

// Scenario 7: two consumers with distinct IDs on the same topic. Each
// receives every published message (fan-out, not load-balancing).
func TestFanOutAcrossConsumerIDs(t *testing.T) {
	h := startBroker(t)

	a := dial(t, h.addr)
	a.sub(t, "orders", "consumer-A")
	_ = a.expectOK(t)

	b := dial(t, h.addr)
	b.sub(t, "orders", "consumer-B")
	_ = b.expectOK(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("m1"))
	id1 := pub.expectOK(t)
	pub.pub(t, "orders", "", []byte("m2"))
	id2 := pub.expectOK(t)

	wantIDs := map[uint64]bool{id1: true, id2: true}

	gotA := map[uint64]bool{}
	for i := 0; i < 2; i++ {
		m := a.expectMsg(t)
		gotA[m.msgID] = true
	}
	if len(gotA) != 2 || !gotA[id1] || !gotA[id2] {
		t.Fatalf("consumer-A got %v, want %v", gotA, wantIDs)
	}

	gotB := map[uint64]bool{}
	for i := 0; i < 2; i++ {
		m := b.expectMsg(t)
		gotB[m.msgID] = true
	}
	if len(gotB) != 2 || !gotB[id1] || !gotB[id2] {
		t.Fatalf("consumer-B got %v, want %v", gotB, wantIDs)
	}
}

// M1 owned risk test: a dedupe key published before a restart must
// still deduplicate after it. The broker rebuilds the dedupe index
// from the WAL on Open (ADR 0018), so a re-publish of the same key
// returns the original MsgID with no second WAL append, and the
// consumer sees exactly one message across the restart boundary.
func TestDedupeSurvivesRestart(t *testing.T) {
	h := startBroker(t)

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "key-A", []byte("first"))
	originalID := pub.expectOK(t)
	pub.close()

	// Restart over the same data dir — the in-memory dedupe LRU is
	// gone; only the WAL survives.
	h.restart(t)

	// Same key, different payload: must be reported as a duplicate of
	// the original, not appended anew.
	repub := dial(t, h.addr)
	repub.pub(t, "orders", "key-A", []byte("second-attempt"))
	if dupID := repub.expectDup(t); dupID != originalID {
		t.Fatalf("post-restart DUP id = %d, want %d", dupID, originalID)
	}
	if okID := repub.expectOK(t); okID != originalID {
		t.Fatalf("post-restart OK after DUP id = %d, want %d", okID, originalID)
	}
	repub.close()

	// Consumer must receive exactly one message (the original), and no
	// duplicate from the re-publish.
	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	msg := sub.expectMsg(t)
	if msg.msgID != originalID {
		t.Fatalf("delivered MsgID = %d, want %d", msg.msgID, originalID)
	}
	if !bytes.Equal(msg.payload, []byte("first")) {
		t.Fatalf("delivered payload = %q, want %q (original, not the re-publish)", msg.payload, "first")
	}
	sub.ack(t, "consumer-1", msg.msgID)
	_ = sub.expectOK(t)
	// No second delivery: the re-publish never created a new record.
	sub.expectNoMsg(t, 250_000_000) // 250ms in ns
}
