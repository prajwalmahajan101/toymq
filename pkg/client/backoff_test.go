package client

import (
	"testing"
	"time"
)

func TestBackoffDelayBounds(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 200*time.Millisecond, 8)
	// randFloat=1 gives the upper bound: min(max, base*2^(n-1)).
	b.randFloat = func() float64 { return 1 }
	cases := []struct {
		attempt int
		wantCap time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 160 * time.Millisecond},
		{6, 200 * time.Millisecond}, // clamped at max
		{60, 200 * time.Millisecond}, // huge shift clamped, no overflow to negative
	}
	for _, c := range cases {
		if got := b.delay(c.attempt); got != c.wantCap {
			t.Errorf("delay(%d) = %v, want %v", c.attempt, got, c.wantCap)
		}
	}
	if got := b.delay(0); got != 0 {
		t.Errorf("delay(0) = %v, want 0", got)
	}
}

func TestBackoffFullJitter(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 200*time.Millisecond, 8)
	b.randFloat = func() float64 { return 0 } // lower bound is 0 (full jitter)
	if got := b.delay(5); got != 0 {
		t.Errorf("delay with rand=0 = %v, want 0", got)
	}
	b.randFloat = func() float64 { return 0.5 }
	if got := b.delay(3); got != 20*time.Millisecond { // half of 40ms cap
		t.Errorf("delay with rand=0.5 = %v, want 20ms", got)
	}
}

func TestBackoffWaitAbort(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 200*time.Millisecond, 8)
	b.randFloat = func() float64 { return 1 }
	// sleep never fires; done is already closed → wait returns false.
	b.sleep = func(time.Duration) <-chan time.Time { return make(chan time.Time) }
	done := make(chan struct{})
	close(done)
	if b.wait(3, done) {
		t.Fatal("wait returned true despite closed done channel")
	}
}
