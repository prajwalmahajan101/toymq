package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPub_OK(t *testing.T) {
	addr := startBroker(t)
	c, err := Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	_, dup, err := c.Pub(context.Background(), "orders", "k1", "", []byte("hello"))
	if err != nil {
		t.Fatalf("Pub: %v", err)
	}
	if dup {
		t.Fatalf("want fresh, got dup=true")
	}
}

func TestPub_Dup(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	defer c.Close()

	id1, _, err := c.Pub(context.Background(), "orders", "k1", "", []byte("a"))
	if err != nil {
		t.Fatalf("first Pub: %v", err)
	}
	id2, dup, err := c.Pub(context.Background(), "orders", "k1", "", []byte("a"))
	if err != nil {
		t.Fatalf("second Pub: %v", err)
	}
	if !dup || id2 != id1 {
		t.Fatalf("want dup with id=%d, got id=%d dup=%v", id1, id2, dup)
	}
}

func TestPub_Concurrent(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	defer c.Close()

	const n = 50
	var wg sync.WaitGroup
	ids := make([]uint64, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, _, err := c.Pub(context.Background(), "orders", "", "", []byte("x"))
			if err != nil {
				t.Errorf("Pub: %v", err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()

	seen := map[uint64]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id in stream: %v", ids)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("want %d unique ids, got %d", n, len(seen))
	}
}

func TestPub_ContextCancel(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := c.Pub(ctx, "orders", "", "", []byte("x"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want Canceled, got %v", err)
	}

	if _, _, err := c.Pub(context.Background(), "orders", "", "", []byte("ok")); err != nil {
		t.Fatalf("post-cancel Pub: %v", err)
	}
}

func TestPub_AfterClose(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)
	_ = c.Close()

	_, _, err := c.Pub(context.Background(), "orders", "", "", []byte("x"))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestPub_ClosesCleanly(t *testing.T) {
	addr := startBroker(t)
	c, _ := Dial(context.Background(), addr)

	for range 10 {
		if _, _, err := c.Pub(context.Background(), "orders", "", "", []byte("x")); err != nil {
			t.Fatalf("Pub: %v", err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-c.loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not exit")
	}
}
