package broker

import "github.com/prometheus/client_golang/prometheus"

// RaftCollector exports the broker's live raft replication state as Prometheus
// gauges (v3 M4, ADR 0033). It reads ReplicationStatus() at scrape time rather
// than mirroring it into stored gauges, so the series are always fresh and no
// background sampler goroutine is needed. cmd/toymq registers one on the shared
// registry after AttachRaft; a standalone broker never registers it (the
// gauges would be meaningless), so the series simply do not appear.
type RaftCollector struct {
	b            *Broker
	role         *prometheus.Desc
	commitIndex  *prometheus.Desc
	applyIndex   *prometheus.Desc
	lastLogIndex *prometheus.Desc
	lagEntries   *prometheus.Desc
}

// NewRaftCollector builds a collector reading b's replication status.
func NewRaftCollector(b *Broker) *RaftCollector {
	return &RaftCollector{
		b: b,
		role: prometheus.NewDesc(
			"toymq_raft_role",
			"Raft role of this node: 0=follower, 1=candidate, 2=leader.",
			nil, nil),
		commitIndex: prometheus.NewDesc(
			"toymq_raft_commit_index",
			"Highest raft log index known committed on this node.",
			nil, nil),
		applyIndex: prometheus.NewDesc(
			"toymq_raft_apply_index",
			"Highest raft log index applied to the broker state machine.",
			nil, nil),
		lastLogIndex: prometheus.NewDesc(
			"toymq_raft_last_log_index",
			"Last raft log index present in this node's local log.",
			nil, nil),
		lagEntries: prometheus.NewDesc(
			"toymq_replication_lag_entries",
			"Per-follower replication lag in log entries (leader-only).",
			[]string{"peer"}, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *RaftCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.role
	ch <- c.commitIndex
	ch <- c.applyIndex
	ch <- c.lastLogIndex
	ch <- c.lagEntries
}

// Collect implements prometheus.Collector, sampling live raft state.
func (c *RaftCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.b.ReplicationStatus()
	if st.Role == "standalone" {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.role, prometheus.GaugeValue, roleValue(st.Role))
	ch <- prometheus.MustNewConstMetric(c.commitIndex, prometheus.GaugeValue, float64(st.CommitIndex))
	ch <- prometheus.MustNewConstMetric(c.applyIndex, prometheus.GaugeValue, float64(st.ApplyIndex))
	ch <- prometheus.MustNewConstMetric(c.lastLogIndex, prometheus.GaugeValue, float64(st.LastLogIndex))
	for _, p := range st.Peers {
		ch <- prometheus.MustNewConstMetric(c.lagEntries, prometheus.GaugeValue, float64(p.LagEntries), p.ID)
	}
}

// roleValue maps the INFO role label to the toymq_raft_role gauge encoding.
func roleValue(role string) float64 {
	switch role {
	case "leader":
		return 2
	case "candidate":
		return 1
	default: // follower / unknown
		return 0
	}
}

var _ prometheus.Collector = (*RaftCollector)(nil)
