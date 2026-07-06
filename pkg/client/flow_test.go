package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestPauseResume_HaltsAndResumesDelivery drives the client PAUSE/RESUME
// round-trip against a real broker: after PAUSE, delivery of freshly
// published messages stops (at most one may slip — the record the delivery
// goroutine was already awaiting when PAUSE landed), and after RESUME every
// message is delivered. See ADR 0022.
func TestPauseResume_HaltsAndResumesDelivery(t *testing.T) {
	addr := startBroker(t)

	pub, _ := Dial(context.Background(), addr)
	defer pub.Close()

	sub, _ := Dial(context.Background(), addr)
	defer sub.Close()
	ch, err := sub.Sub(context.Background(), "orders", "cg")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	if err := sub.Pause(context.Background()); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	const n = 5
	for i := range n {
		if _, _, err := pub.Pub(context.Background(), "orders", "", "", []byte{byte('a' + i)}); err != nil {
			t.Fatalf("Pub %d: %v", i, err)
		}
	}

	// While paused, at most one message may have slipped through (the record
	// the delivery goroutine was already awaiting when PAUSE landed). Ack
	// what arrives so it can't be redelivered and inflate the count.
	seen := map[uint64]struct{}{}
	whilePaused := collectDistinctAck(t, ch, seen, 300*time.Millisecond)
	if whilePaused > 1 {
		t.Fatalf("received %d distinct messages while paused, want <= 1", whilePaused)
	}

	if err := sub.Resume(context.Background()); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// After resume every remaining message must arrive.
	afterResume := collectDistinctAck(t, ch, seen, 2*time.Second)
	if whilePaused+afterResume != n {
		t.Fatalf("delivered %d distinct total (%d paused + %d resumed), want %d", whilePaused+afterResume, whilePaused, afterResume, n)
	}
}

// TestPause_NoSub returns a server error when no subscription is active.
func TestPause_NoSub(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	defer c.Close()

	err := c.Pause(context.Background())
	if !errors.Is(err, ErrServer) {
		t.Fatalf("Pause without SUB: err = %v, want ErrServer", err)
	}
}

// collectDistinctAck reads deliveries until the channel is idle for d,
// acking each (so it isn't redelivered) and counting only first-seen
// MsgIDs. Returns the number of newly-distinct messages observed in this
// call; seen accumulates across calls.
func collectDistinctAck(t *testing.T, ch <-chan Delivery, seen map[uint64]struct{}, d time.Duration) int {
	t.Helper()
	fresh := 0
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return fresh
			}
			if _, dup := seen[msg.MsgID]; !dup {
				seen[msg.MsgID] = struct{}{}
				fresh++
			}
			if err := msg.Ack(context.Background()); err != nil {
				t.Fatalf("Ack %d: %v", msg.MsgID, err)
			}
		case <-time.After(d):
			return fresh
		}
	}
}
