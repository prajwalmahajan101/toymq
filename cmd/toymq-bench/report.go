package main

import (
	"fmt"
	"io"
	"sort"
	"time"
)

// Stats is the aggregate report across every producer's measurements.
type Stats struct {
	Total      int
	Errors     int
	Elapsed    time.Duration
	Throughput float64 // msgs / sec
	MiBPerSec  float64 // payload throughput
	Min, P50, P95, P99, Max time.Duration
}

// aggregate sorts all producer latencies into one slice and picks
// percentile indices by position. Per BUILD_GUIDE Step 14c: no
// histogram library — slice + sort is sufficient at the bench's
// typical N (sub-millisecond sort for tens of thousands of samples).
func aggregate(results []result, payloadSize int, elapsed time.Duration) Stats {
	var all []time.Duration
	var errs int
	for _, r := range results {
		all = append(all, r.latencies...)
		errs += r.pubErrs
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	s := Stats{
		Total:   len(all),
		Errors:  errs,
		Elapsed: elapsed,
	}
	if elapsed > 0 {
		s.Throughput = float64(s.Total) / elapsed.Seconds()
		s.MiBPerSec = float64(s.Total*payloadSize) / 1024 / 1024 / elapsed.Seconds()
	}
	if len(all) > 0 {
		s.Min = all[0]
		s.Max = all[len(all)-1]
		s.P50 = all[idx(len(all), 50)]
		s.P95 = all[idx(len(all), 95)]
		s.P99 = all[idx(len(all), 99)]
	}
	return s
}

// idx returns the index for the given percentile in a length-n
// sorted slice. Uses nearest-rank (no interpolation) — fine for the
// bench's reporting precision.
func idx(n, p int) int {
	if n <= 0 {
		return 0
	}
	i := n * p / 100
	if i >= n {
		i = n - 1
	}
	return i
}

// writeReport emits the human-readable stats block. Fields are
// labelled and tab-aligned so the output stays grep-friendly.
func writeReport(w io.Writer, s Stats, cfg benchConfig) {
	fmt.Fprintf(w, "toymq-bench  addr=%s  topic=%s  producers=%d  msgs=%d  size=%d\n",
		cfg.Addr, cfg.Topic, cfg.Producers, cfg.Msgs, cfg.Size)
	fmt.Fprintf(w, "elapsed     %s\n", s.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "throughput  %.1f msg/s   %.2f MiB/s\n", s.Throughput, s.MiBPerSec)
	fmt.Fprintf(w, "latency     min=%s  p50=%s  p95=%s  p99=%s  max=%s\n",
		fmtDur(s.Min), fmtDur(s.P50), fmtDur(s.P95), fmtDur(s.P99), fmtDur(s.Max))
	fmt.Fprintf(w, "errors      %d\n", s.Errors)
}

// fmtDur trims to microsecond precision so the output is consistent
// across magnitudes.
func fmtDur(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Microsecond {
		return d.String()
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return d.Round(100 * time.Microsecond).String()
}
