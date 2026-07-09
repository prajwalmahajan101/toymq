package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func mkResult(durs ...time.Duration) result {
	return result{latencies: append([]time.Duration{}, durs...)}
}

func TestAggregate_Percentiles(t *testing.T) {
	// 100 samples, 1ms..100ms (one per ms).
	var lats []time.Duration
	for i := 1; i <= 100; i++ {
		lats = append(lats, time.Duration(i)*time.Millisecond)
	}
	results := []result{{latencies: lats}}

	s := aggregate(results, 0, time.Second)

	if s.Total != 100 {
		t.Fatalf("Total=%d", s.Total)
	}
	if s.Errors != 0 {
		t.Fatalf("Errors=%d", s.Errors)
	}
	if s.Min != time.Millisecond {
		t.Fatalf("Min=%v", s.Min)
	}
	if s.Max != 100*time.Millisecond {
		t.Fatalf("Max=%v", s.Max)
	}
	// nearest-rank: idx = n*p/100 = 50, sample[50] = 51ms
	if s.P50 != 51*time.Millisecond {
		t.Fatalf("P50=%v, want 51ms", s.P50)
	}
	// idx = 95, sample[95] = 96ms
	if s.P95 != 96*time.Millisecond {
		t.Fatalf("P95=%v, want 96ms", s.P95)
	}
	// idx = 99, sample[99] = 100ms
	if s.P99 != 100*time.Millisecond {
		t.Fatalf("P99=%v, want 100ms", s.P99)
	}
}

func TestAggregate_MergesMultipleResults(t *testing.T) {
	r1 := mkResult(1*time.Millisecond, 3*time.Millisecond)
	r2 := mkResult(2*time.Millisecond, 4*time.Millisecond)
	r2.pubErrs = 2

	s := aggregate([]result{r1, r2}, 0, time.Second)
	if s.Total != 4 {
		t.Fatalf("Total=%d", s.Total)
	}
	if s.Errors != 2 {
		t.Fatalf("Errors=%d", s.Errors)
	}
	if s.Min != time.Millisecond || s.Max != 4*time.Millisecond {
		t.Fatalf("Min=%v Max=%v", s.Min, s.Max)
	}
}

func TestAggregate_Throughput(t *testing.T) {
	lats := []time.Duration{time.Millisecond, time.Millisecond}
	s := aggregate([]result{{latencies: lats}}, 100, time.Second)
	if s.Throughput != 2 {
		t.Fatalf("Throughput=%v, want 2", s.Throughput)
	}
	wantMiB := float64(2*100) / 1024 / 1024
	if s.MiBPerSec != wantMiB {
		t.Fatalf("MiBPerSec=%v, want %v", s.MiBPerSec, wantMiB)
	}
}

func TestAggregate_EmptySafe(t *testing.T) {
	s := aggregate(nil, 0, time.Second)
	if s.Total != 0 || s.Min != 0 || s.Max != 0 || s.P50 != 0 {
		t.Fatalf("got %+v", s)
	}
}

func TestWriteReport_HasExpectedLabels(t *testing.T) {
	s := Stats{
		Total:      100,
		Elapsed:    time.Second,
		Throughput: 100,
		MiBPerSec:  0.024,
		Min:        100 * time.Microsecond,
		P50:        500 * time.Microsecond,
		P95:        2 * time.Millisecond,
		P99:        5 * time.Millisecond,
		Max:        10 * time.Millisecond,
	}
	cfg := benchConfig{Addr: "x", Topic: "t", Producers: 1, Msgs: 100, Size: 32, Partitions: 4, Fsync: "batched", TLS: true}
	var buf bytes.Buffer
	writeReport(&buf, s, cfg)
	for _, want := range []string{"toymq-bench", "elapsed", "throughput", "latency", "p50=", "p95=", "p99=", "errors",
		"partitions=4", "fsync=batched", "tls=true", "per-part"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("report missing %q: %s", want, buf.String())
		}
	}
}

// TestWriteReport_PerPartitionThroughput: the per-partition line is the
// aggregate throughput divided by the partition count (keyless round-robin).
func TestWriteReport_PerPartitionThroughput(t *testing.T) {
	s := Stats{Total: 100, Elapsed: time.Second, Throughput: 400}
	cfg := benchConfig{Partitions: 4, Fsync: "per-message"}
	var buf bytes.Buffer
	writeReport(&buf, s, cfg)
	// 400 / 4 = 100 msg/s per partition.
	if !strings.Contains(buf.String(), "~100.0 msg/s across 4 partition(s)") {
		t.Fatalf("per-partition line wrong: %s", buf.String())
	}
}

// TestWriteReport_PartitionsZeroDefaultsToOne: an unset partition count must
// not divide by zero.
func TestWriteReport_PartitionsZeroDefaultsToOne(t *testing.T) {
	s := Stats{Total: 10, Elapsed: time.Second, Throughput: 50}
	cfg := benchConfig{} // Partitions == 0
	var buf bytes.Buffer
	writeReport(&buf, s, cfg)
	if !strings.Contains(buf.String(), "~50.0 msg/s across 1 partition(s)") {
		t.Fatalf("zero-partition fallback wrong: %s", buf.String())
	}
}

func TestIdx(t *testing.T) {
	if idx(100, 50) != 50 {
		t.Fatal("idx(100,50)")
	}
	if idx(100, 99) != 99 {
		t.Fatal("idx(100,99)")
	}
	if idx(1, 99) != 0 {
		t.Fatal("idx(1,99)")
	}
	if idx(0, 50) != 0 {
		t.Fatal("idx(0,50)")
	}
}
