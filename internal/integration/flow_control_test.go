package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestReceiveWindowBoundsInflight is the M5 owned-risk test: a fast
// producer floods the log, but the broker delivers at most --recv-window
// un-acked messages to a slow consumer, and only releases the next one when
// an ACK frees a window slot. This bounds inflight memory regardless of the
// backlog size (ADR 0022).
func TestReceiveWindowBoundsInflight(t *testing.T) {
	const window = 4
	// A long visibility timeout keeps the redelivery ticker from resending
	// the in-window messages during the expectNoMsg grace windows — this
	// test isolates first-delivery flow control, not redelivery (which
	// legitimately resends the already-counted inflight).
	h := startBroker(t, withRecvWindow(window), withVisibility(30*time.Second))

	// Fast producer: fill the log well past the window.
	pub := dial(t, h.addr)
	const total = 20
	for i := range total {
		pub.pub(t, "orders", "", fmt.Appendf(nil, "m-%02d", i))
		_ = pub.expectOK(t)
	}

	// Slow consumer: acks nothing yet. Exactly `window` messages are
	// delivered, then delivery stalls — the (window+1)th must not arrive.
	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	got := make([]uint64, 0, window)
	for range window {
		got = append(got, sub.expectMsg(t).msgID)
	}
	// 150ms covers several redeliver ticks (20ms): if the window were not
	// enforced, a 5th message (or a redelivery beyond the window) would land.
	sub.expectNoMsg(t, 150*time.Millisecond)

	// Ack one: exactly one further message is released, then delivery stalls
	// again at the window.
	sub.ack(t, "consumer-1", got[0])
	_ = sub.expectOK(t)
	next := sub.expectMsg(t)
	if next.msgID == got[0] {
		t.Fatalf("released message reused acked MsgID %d", got[0])
	}
	sub.expectNoMsg(t, 150*time.Millisecond)
}

// TestPauseHaltsAndResumeReleases proves PAUSE suspends delivery even when
// the window has room, and RESUME lets it continue. Uses window=1 so the
// delivery goroutine is parked in the window gate (not mid-WAL-read) when
// PAUSE lands, making the halt deterministic with no in-flight slip.
func TestPauseHaltsAndResumeReleases(t *testing.T) {
	// Long visibility so a redelivery of the first (unacked) message can't
	// race the PAUSE assertions.
	h := startBroker(t, withRecvWindow(1), withVisibility(30*time.Second))

	pub := dial(t, h.addr)
	pub.pub(t, "orders", "", []byte("first"))
	id0 := pub.expectOK(t)
	pub.pub(t, "orders", "", []byte("second"))
	_ = pub.expectOK(t)

	sub := dial(t, h.addr)
	sub.sub(t, "orders", "consumer-1")
	_ = sub.expectOK(t)

	// Window 1: the first message is delivered, the second waits on the gate.
	first := sub.expectMsg(t)
	if first.msgID != id0 {
		t.Fatalf("first delivery MsgID = %d, want %d", first.msgID, id0)
	}

	// PAUSE, then free the window by acking. Without PAUSE the second message
	// would now be released; PAUSE must keep it suppressed.
	sub.pause(t)
	_ = sub.expectOK(t)
	sub.ack(t, "consumer-1", first.msgID)
	_ = sub.expectOK(t)
	sub.expectNoMsg(t, 150*time.Millisecond)

	// RESUME releases the held message.
	sub.resume(t)
	_ = sub.expectOK(t)
	second := sub.expectMsg(t)
	if second.msgID == first.msgID {
		t.Fatalf("resumed delivery re-sent acked MsgID %d", first.msgID)
	}
}

// TestPauseResumeWithoutSub reports NO_SUB when there is no subscription to
// pause.
func TestPauseResumeWithoutSub(t *testing.T) {
	h := startBroker(t)
	c := dial(t, h.addr)

	c.pause(t)
	if line := c.readResponseLine(t); !isErr(line, "NO_SUB") {
		t.Fatalf("PAUSE without SUB: got %q, want ERR NO_SUB", line)
	}
	c.resume(t)
	if line := c.readResponseLine(t); !isErr(line, "NO_SUB") {
		t.Fatalf("RESUME without SUB: got %q, want ERR NO_SUB", line)
	}
}

func isErr(line, code string) bool {
	return strings.HasPrefix(line, "ERR ") && strings.Contains(line, code)
}
