package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/pkg/client"
)

func TestDistribute_Even(t *testing.T) {
	got := distribute(10, 5)
	if len(got) != 5 {
		t.Fatalf("len=%d", len(got))
	}
	for i, n := range got {
		if n != 2 {
			t.Fatalf("got[%d]=%d, want 2", i, n)
		}
	}
}

func TestDistribute_Remainder(t *testing.T) {
	got := distribute(11, 4) // 3, 3, 3, 2
	sum := 0
	for _, n := range got {
		sum += n
	}
	if sum != 11 {
		t.Fatalf("sum=%d", sum)
	}
	if got[0] != 3 || got[1] != 3 || got[2] != 3 || got[3] != 2 {
		t.Fatalf("got=%v", got)
	}
}

func TestMakePayload(t *testing.T) {
	b := makePayload(8)
	if len(b) != 8 {
		t.Fatalf("len=%d", len(b))
	}
	for i, c := range b {
		if c != 'x' {
			t.Fatalf("b[%d]=%v", i, c)
		}
	}
}

func TestRunProducer_Pubs100Msgs(t *testing.T) {
	addr := startBroker(t)
	payload := makePayload(64)

	const (
		producers     = 4
		msgsPerProd   = 25
		expectedTotal = producers * msgsPerProd
	)

	var wg sync.WaitGroup
	results := make([]result, producers)
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := client.Dial(context.Background(), addr)
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			defer c.Close()
			results[i] = runProducer(context.Background(), c, "bench", msgsPerProd, payload)
		}(i)
	}
	wg.Wait()

	total := 0
	errs := 0
	for _, r := range results {
		total += len(r.latencies)
		errs += r.pubErrs
	}
	if total != expectedTotal {
		t.Fatalf("total latencies=%d, want %d", total, expectedTotal)
	}
	if errs != 0 {
		t.Fatalf("errs=%d, want 0", errs)
	}
}

func TestRunProducer_CtxCancel(t *testing.T) {
	addr := startBroker(t)
	c, err := client.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := runProducer(ctx, c, "bench", 1000, makePayload(32))
	if len(r.latencies) > 5 {
		t.Fatalf("got %d latencies, want <=5 (ctx already cancelled)", len(r.latencies))
	}
	_ = time.Now()
}
