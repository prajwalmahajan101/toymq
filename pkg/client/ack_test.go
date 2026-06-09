package client

import (
	"context"
	"testing"
	"time"
)

func TestAck_RemovesFromBacklog(t *testing.T) {
	addr := startBroker(t)

	pub, _ := Dial(context.Background(), addr)
	defer pub.Close()

	for i := range 3 {
		if _, _, err := pub.Pub(context.Background(), "orders", "", []byte{byte('a' + i)}); err != nil {
			t.Fatalf("Pub: %v", err)
		}
	}

	sub1, _ := Dial(context.Background(), addr)
	ch1, err := sub1.Sub(context.Background(), "orders", "cg")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	d := <-ch1
	firstID := d.MsgID
	if err := d.Ack(context.Background()); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	_ = sub1.Close()

	sub2, _ := Dial(context.Background(), addr)
	defer sub2.Close()
	ch2, err := sub2.Sub(context.Background(), "orders", "cg")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	select {
	case d := <-ch2:
		if d.MsgID == firstID {
			t.Fatalf("got already-acked msg %d again", firstID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no replay")
	}
}

func TestNack_Redelivers(t *testing.T) {
	addr := startBroker(t)

	pub, _ := Dial(context.Background(), addr)
	defer pub.Close()
	if _, _, err := pub.Pub(context.Background(), "orders", "", []byte("x")); err != nil {
		t.Fatalf("Pub: %v", err)
	}

	sub, _ := Dial(context.Background(), addr)
	defer sub.Close()
	ch, err := sub.Sub(context.Background(), "orders", "cg")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	d1 := <-ch
	if err := d1.Nack(context.Background()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	select {
	case d2 := <-ch:
		if d2.MsgID != d1.MsgID {
			t.Fatalf("redelivery id mismatch: %d vs %d", d1.MsgID, d2.MsgID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no redelivery after NACK")
	}
}

// TestClient_Ack_PublicMethod exercises Client.Ack as a top-level
// method (no Delivery closure). The broker requires the target msg
// to be inflight before it accepts an ACK, so the test drains the
// delivery channel until the target arrives and then calls
// Client.Ack rather than Delivery.Ack — proving the public method
// works as the wire-level affordance it advertises.
func TestClient_Ack_PublicMethod(t *testing.T) {
	addr := startBroker(t)

	pub, _ := Dial(context.Background(), addr)
	defer pub.Close()
	id, _, err := pub.Pub(context.Background(), "orders", "", []byte("x"))
	if err != nil {
		t.Fatalf("Pub: %v", err)
	}

	sub, _ := Dial(context.Background(), addr)
	ch, err := sub.Sub(context.Background(), "orders", "cg")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	select {
	case d := <-ch:
		if d.MsgID != id {
			t.Fatalf("got msg %d, want %d", d.MsgID, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery within 2s")
	}

	if err := sub.Ack(context.Background(), "cg", id); err != nil {
		t.Fatalf("Client.Ack: %v", err)
	}
	_ = sub.Close()

	resub, _ := Dial(context.Background(), addr)
	defer resub.Close()
	ch2, err := resub.Sub(context.Background(), "orders", "cg")
	if err != nil {
		t.Fatalf("resub: %v", err)
	}
	select {
	case d := <-ch2:
		t.Fatalf("got replay of acked msg %d", d.MsgID)
	case <-time.After(300 * time.Millisecond):
	}
}
