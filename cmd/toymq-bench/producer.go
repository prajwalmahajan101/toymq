package main

import (
	"context"
	"time"

	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// result holds one producer goroutine's measurements. Aggregation
// (sorting, percentiles) happens in report.go after every producer
// returns.
type result struct {
	latencies []time.Duration
	pubErrs   int
}

// runProducer publishes count messages on c, recording per-message
// latency. Errors are counted but not fatal — a slow broker should
// produce a noisy report, not a crashed bench.
func runProducer(ctx context.Context, c *client.Client, topic string,
	count int, payload []byte) result {
	out := result{latencies: make([]time.Duration, 0, count)}
	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			return out
		}
		start := time.Now()
		_, _, err := c.Pub(ctx, topic, "", payload)
		if err != nil {
			out.pubErrs++
			continue
		}
		out.latencies = append(out.latencies, time.Since(start))
	}
	return out
}

// distribute splits total across producers as evenly as possible.
// Producer i gets floor(total/n) plus 1 extra if i < total%n.
func distribute(total, producers int) []int {
	per := make([]int, producers)
	base := total / producers
	rem := total % producers
	for i := range per {
		per[i] = base
		if i < rem {
			per[i]++
		}
	}
	return per
}

// makePayload returns a deterministic byte slice of length n.
func makePayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return b
}
