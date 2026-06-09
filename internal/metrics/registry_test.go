package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNew_RegistersEverySeries(t *testing.T) {
	reg := NewRegistry()
	m := New(reg)

	// Exercise every public helper so the registry's
	// GatherableCount is non-zero for every series we ship.
	m.IncPublish("orders", 5)
	m.IncPublishDup("orders")
	m.IncSubscribe("orders")
	m.SetInflight("orders", "c1", 3)
	m.IncRedelivery("orders", 2)
	m.IncRedelivery("orders", 4)
	m.IncRedelivery("orders", 7)
	m.ObserveWALAppend("orders", 0.003)
	m.IncSessions()
	m.IncSubs()
	m.SetTopicCount(1)
	m.IncOffsetsFlush("orders", true)
	m.IncOffsetsFlush("orders", false)

	if v := testutil.ToFloat64(m.PublishTotal.WithLabelValues("orders")); v != 1 {
		t.Fatalf("PublishTotal = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.PublishBytes.WithLabelValues("orders")); v != 5 {
		t.Fatalf("PublishBytes = %v, want 5", v)
	}
	if v := testutil.ToFloat64(m.RedeliveryTotal.WithLabelValues("orders", "2")); v != 1 {
		t.Fatalf("RedeliveryTotal[2] = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.RedeliveryTotal.WithLabelValues("orders", "3-5")); v != 1 {
		t.Fatalf("RedeliveryTotal[3-5] = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.RedeliveryTotal.WithLabelValues("orders", "6+")); v != 1 {
		t.Fatalf("RedeliveryTotal[6+] = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.ActiveSessions); v != 1 {
		t.Fatalf("ActiveSessions = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.ActiveSubscriptions); v != 1 {
		t.Fatalf("ActiveSubscriptions = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.TopicCount); v != 1 {
		t.Fatalf("TopicCount = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.OffsetsFlushTotal.WithLabelValues("orders", "ok")); v != 1 {
		t.Fatalf("OffsetsFlushTotal[ok] = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.OffsetsFlushTotal.WithLabelValues("orders", "error")); v != 1 {
		t.Fatalf("OffsetsFlushTotal[error] = %v, want 1", v)
	}
}

func TestNilMetrics_IsNoOp(t *testing.T) {
	// Every helper must accept a nil receiver. If any of them
	// panics on a nil call, this test crashes — that's the
	// observation.
	var m *Metrics
	m.IncPublish("t", 1)
	m.IncPublishDup("t")
	m.IncSubscribe("t")
	m.SetInflight("t", "c", 1)
	m.IncRedelivery("t", 3)
	m.ObserveWALAppend("t", 0.001)
	m.IncSessions()
	m.DecSessions()
	m.IncSubs()
	m.DecSubs()
	m.SetTopicCount(1)
	m.IncOffsetsFlush("t", true)
}

func TestRedeliveryBucket(t *testing.T) {
	cases := []struct {
		attempts int
		want     string
	}{
		{1, "2"},
		{2, "2"},
		{3, "3-5"},
		{5, "3-5"},
		{6, "6+"},
		{42, "6+"},
	}
	for _, tc := range cases {
		if got := redeliveryBucket(tc.attempts); got != tc.want {
			t.Errorf("redeliveryBucket(%d) = %q, want %q",
				tc.attempts, got, tc.want)
		}
	}
}
