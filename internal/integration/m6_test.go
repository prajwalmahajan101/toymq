package integration

import (
	"bytes"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
)

// aggressiveRetention rolls tiny segments and keeps only a few records,
// so a modest publish burst reclaims older segments within a couple of
// sweep ticks.
func aggressiveRetention() broker.RetentionConfig {
	return broker.RetentionConfig{
		SegmentBytes: 200,
		RetainBytes:  400,
		Interval:     20 * time.Millisecond,
	}
}

// TestM6DelayedDeliveryOverWire (owned risk test, part c): a DELAYed
// message must not be delivered before its visible-at and must arrive
// shortly after.
func TestM6DelayedDeliveryOverWire(t *testing.T) {
	h := startBroker(t)
	prod := dial(t, h.addr)
	prod.pubDelay(t, "orders", []byte("later"), 250)
	prod.expectOK(t)

	con := dial(t, h.addr)
	con.sub(t, "orders", "c1")
	con.expectOK(t) // SUB ack

	// Not delivered in the first ~half of the delay.
	con.expectNoMsg(t, 120*time.Millisecond)

	// Delivered after the delay elapses.
	m := con.expectMsg(t)
	if string(m.payload) != "later" {
		t.Fatalf("delivered payload = %q, want %q", m.payload, "later")
	}
}

// TestM6DeadLetterOverWire (owned risk test, part b): a message past the
// nack threshold lands on <topic>.dlq and stops redelivering on the
// source. Long visibility isolates the nack path from the timeout sweep.
func TestM6DeadLetterOverWire(t *testing.T) {
	h := startBroker(t, withDLQ(1), withVisibility(10*time.Second))

	prod := dial(t, h.addr)
	prod.pubDelay(t, "orders", []byte("poison"), 0)
	prod.expectOK(t)

	con := dial(t, h.addr)
	con.sub(t, "orders", "c1")
	con.expectOK(t)

	m := con.expectMsg(t)
	if string(m.payload) != "poison" {
		t.Fatalf("first delivery = %q, want poison", m.payload)
	}
	con.nack(t, "c1", m.msgID) // threshold 1 → dead-lettered
	con.expectOK(t)            // NACK ack

	// Source must not redeliver.
	con.expectNoMsg(t, 300*time.Millisecond)

	// The dead message is on orders.dlq.
	dlq := dial(t, h.addr)
	dlq.sub(t, "orders.dlq", "d1")
	dlq.expectOK(t)
	dm := dlq.expectMsg(t)
	if string(dm.payload) != "poison" {
		t.Fatalf("dlq payload = %q, want poison", dm.payload)
	}
}

// TestM6RetentionOutOfRangeOverWire (owned risk test, part a): retention
// drops old segments; a resuming consumer whose acked offset fell below
// the retained floor gets ERR OUT_OF_RANGE on re-subscribe.
func TestM6RetentionOutOfRangeOverWire(t *testing.T) {
	h := startBroker(t, withRetention(aggressiveRetention()))

	prod := dial(t, h.addr)
	payload := bytes.Repeat([]byte("x"), 100)

	// Publish a few, then a consumer acks MsgID 0 (durable lastAcked=0).
	for range 3 {
		prod.pubDelay(t, "orders", payload, 0)
		prod.expectOK(t)
	}
	con := dial(t, h.addr)
	con.sub(t, "orders", "c1")
	con.expectOK(t)
	m := con.expectMsg(t)
	if m.msgID != 0 {
		t.Fatalf("first msg id = %d, want 0", m.msgID)
	}
	con.ack(t, "c1", 0)
	con.expectOK(t) // ACK ack
	con.close()

	// Flood more so retention advances the floor well past MsgID 1.
	for range 40 {
		prod.pubDelay(t, "orders", payload, 0)
		prod.expectOK(t)
	}
	time.Sleep(150 * time.Millisecond) // let the sweeper run

	// c1 resumes from MsgID 1, now below the floor → OUT_OF_RANGE.
	con2 := dial(t, h.addr)
	con2.sub(t, "orders", "c1")
	if code, _ := con2.expectErr(t); code != "OUT_OF_RANGE" {
		t.Fatalf("resume below floor: code = %q, want OUT_OF_RANGE", code)
	}
}

// TestM6RetentionKeepsUnfiredDelayed (owned risk test, part d): a burst
// that would otherwise reclaim MsgID 0's segment must not drop it while
// its delayed record is un-fired — the consumer still receives it.
func TestM6RetentionKeepsUnfiredDelayed(t *testing.T) {
	h := startBroker(t, withRetention(aggressiveRetention()))

	prod := dial(t, h.addr)
	// MsgID 0 is a delayed record (fires ~400ms out).
	prod.pubDelay(t, "orders", []byte("keepme"), 400)
	prod.expectOK(t)

	// Flood immediates that would advance the floor past MsgID 0 if the
	// delayed-record guard were absent.
	payload := bytes.Repeat([]byte("y"), 100)
	for range 40 {
		prod.pubDelay(t, "orders", payload, 0)
		prod.expectOK(t)
	}
	time.Sleep(150 * time.Millisecond) // sweeper runs; guard must keep seg 0

	// A fresh consumer starts at the floor. If the guard held, the floor
	// is still MsgID 0, so the first delivery (after the delay) is the
	// delayed record. If retention had dropped it, "keepme" is gone.
	con := dial(t, h.addr)
	con.sub(t, "orders", "d1")
	con.expectOK(t)
	m := con.expectMsg(t)
	if string(m.payload) != "keepme" || m.msgID != 0 {
		t.Fatalf("first delivery = MsgID %d %q, want MsgID 0 keepme (delayed record must survive retention)", m.msgID, m.payload)
	}
}
