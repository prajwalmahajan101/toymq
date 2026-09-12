package client

import (
	"math/rand"
	"time"
)

// backoff computes bounded exponential delays with full jitter for the
// ClusterClient redirect/retry loop. It caps the number of attempts so a
// redirect storm during a raft election terminates with a surfaced error
// rather than spinning forever (v3 M3, ADR 0032). rand and sleep are
// injectable so tests are deterministic and instant.
type backoff struct {
	base     time.Duration // delay before attempt 1's retry
	max      time.Duration // ceiling on the exponential term
	attempts int           // total tries (attempt 0 is the first, no wait)

	randFloat func() float64             // [0,1); defaults to rand
	sleep     func(time.Duration) <-chan time.Time
}

func newBackoff(base, max time.Duration, attempts int) *backoff {
	if attempts < 1 {
		attempts = 1
	}
	rng := rand.New(rand.NewSource(1))
	return &backoff{
		base:      base,
		max:       max,
		attempts:  attempts,
		randFloat: rng.Float64,
		sleep:     time.After,
	}
}

// delay returns the full-jitter wait before retry number attempt (1-based:
// attempt 1 is the first retry). It is random in [0, min(max, base*2^(n-1))].
func (b *backoff) delay(attempt int) time.Duration {
	if attempt < 1 {
		return 0
	}
	d := b.base << (attempt - 1)
	if d <= 0 || d > b.max { // <=0 guards the shift overflowing to negative
		d = b.max
	}
	return time.Duration(b.randFloat() * float64(d))
}

// wait blocks for delay(attempt) unless ctx-like done fires first. Returns
// false if done fired (caller should abort). A zero/negative delay returns
// immediately with true.
func (b *backoff) wait(attempt int, done <-chan struct{}) bool {
	d := b.delay(attempt)
	if d <= 0 {
		return true
	}
	select {
	case <-b.sleep(d):
		return true
	case <-done:
		return false
	}
}
