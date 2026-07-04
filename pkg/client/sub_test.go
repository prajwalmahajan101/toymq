package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSub_DeliversPubs(t *testing.T) {
	addr := startBroker(t)

	pub, _ := Dial(context.Background(), addr)
	defer pub.Close()
	sub, _ := Dial(context.Background(), addr)
	defer sub.Close()

	for _, p := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		if _, _, err := pub.Pub(context.Background(), "orders", "", "", p); err != nil {
			t.Fatalf("Pub: %v", err)
		}
	}

	ch, err := sub.Sub(context.Background(), "orders", "c1")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	got := make([]string, 0, 3)
	timeout := time.After(2 * time.Second)
	for range 3 {
		select {
		case d := <-ch:
			got = append(got, string(d.Payload))
			if err := d.Ack(context.Background()); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-timeout:
			t.Fatalf("got %d/3 deliveries: %v", len(got), got)
		}
	}
	if got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("out of order: %v", got)
	}
}

func TestSub_SecondSubReturnsErrSubInUse(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	defer c.Close()

	if _, err := c.Sub(context.Background(), "orders", "c1"); err != nil {
		t.Fatalf("first Sub: %v", err)
	}
	if _, err := c.Sub(context.Background(), "orders", "c2"); !errors.Is(err, ErrSubInUse) {
		t.Fatalf("want ErrSubInUse, got %v", err)
	}
}

func TestSub_ChannelClosesOnClose(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	ch, err := c.Sub(context.Background(), "orders", "c1")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	_ = c.Close()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("delivery channel did not close")
		}
	}
}
