// Package metrics holds the broker's Prometheus metric surface. It
// owns a private *prometheus.Registry (not the default global
// registry) so tests don't leak state across runs and so the
// /metrics handler in cmd/toymq scrapes only the series we
// intentionally export.
//
// A nil *Metrics is a valid "metrics disabled" sentinel: every
// public helper short-circuits on nil so call sites compile to a
// single load and branch. This matches the nil-safe pattern
// established by Client.log in pkg/client and documented in
// ADR 0015.
package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the registered set of Prometheus collectors the broker
// reports. Construct via New; pass the pointer down to broker /
// server / wal. A nil pointer turns every observation into a no-op.
type Metrics struct {
	PublishTotal        *prometheus.CounterVec
	PublishDupTotal     *prometheus.CounterVec
	PublishBytes        *prometheus.CounterVec
	SubscribeTotal      *prometheus.CounterVec
	InflightMessages    *prometheus.GaugeVec
	RedeliveryTotal     *prometheus.CounterVec
	WALAppendSeconds    *prometheus.HistogramVec
	ActiveSessions      prometheus.Gauge
	ActiveSubscriptions prometheus.Gauge
	TopicCount          prometheus.Gauge
	OffsetsFlushTotal   *prometheus.CounterVec

	// M7 — RED/USE depth (ADR 0027). All follow the same nil-safe
	// helper pattern and are registered on the same private registry.
	AckTotal               *prometheus.CounterVec
	NackTotal              *prometheus.CounterVec
	ConsumerLag            *prometheus.GaugeVec
	DLQTotal               *prometheus.CounterVec
	DelayedPending         *prometheus.GaugeVec
	RetentionSegmentsTotal prometheus.Counter
	RetentionBytesTotal    prometheus.Counter
	PartitionLatestMsgID   *prometheus.GaugeVec
	WALSegments            *prometheus.GaugeVec
	CommandErrorsTotal     *prometheus.CounterVec
	PublishFailureTotal    *prometheus.CounterVec
}

// NewRegistry returns a fresh Prometheus registry pre-populated with
// the Go runtime and process collectors. cmd/toymq passes this to
// promhttp.HandlerFor so /metrics covers both broker counters and
// the standard go_* / process_* series for free.
func NewRegistry() *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return r
}

// New constructs the broker's metric surface and registers every
// collector on r.
func New(r *prometheus.Registry) *Metrics {
	m := &Metrics{
		PublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_publish_total",
			Help: "Successful PUBLISH calls accepted by the broker, by topic.",
		}, []string{"topic"}),

		PublishDupTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_publish_dup_total",
			Help: "PUBLISH calls that matched the dedupe LRU, by topic.",
		}, []string{"topic"}),

		PublishBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_publish_bytes_total",
			Help: "Sum of payload bytes accepted by PUBLISH, by topic.",
		}, []string{"topic"}),

		SubscribeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_subscribe_total",
			Help: "SUBSCRIBE calls accepted by the broker, by topic.",
		}, []string{"topic"}),

		InflightMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "toymq_inflight_messages",
			Help: "Messages delivered but not yet acked, per topic/consumer.",
		}, []string{"topic", "consumer"}),

		RedeliveryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_redelivery_total",
			Help: "Messages redelivered by the visibility-timeout sweep, by topic and attempts bucket.",
		}, []string{"topic", "attempts_bucket"}),

		WALAppendSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "toymq_wal_append_seconds",
			Help:    "WAL append+fsync latency in seconds, by topic.",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		}, []string{"topic"}),

		ActiveSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "toymq_active_sessions",
			Help: "Connected TCP sessions currently being served.",
		}),

		ActiveSubscriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "toymq_active_subscriptions",
			Help: "Active subscriptions across all topics and consumers.",
		}),

		TopicCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "toymq_topic_count",
			Help: "Number of topics known to the broker.",
		}),

		OffsetsFlushTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_offsets_flush_total",
			Help: "Offsets-file flushes, by topic and result (ok|error).",
		}, []string{"topic", "result"}),

		AckTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_ack_total",
			Help: "Messages acked, by topic and partition.",
		}, []string{"topic", "partition"}),

		NackTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_nack_total",
			Help: "Messages nacked, by topic and partition.",
		}, []string{"topic", "partition"}),

		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "toymq_consumer_lag_messages",
			Help: "Per-(topic, partition, consumer) lag: latest msgID minus last acked msgID.",
		}, []string{"topic", "partition", "consumer"}),

		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_dlq_total",
			Help: "Messages dead-lettered, by topic and trigger (nack|timeout).",
		}, []string{"topic", "trigger"}),

		DelayedPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "toymq_delayed_pending",
			Help: "Un-fired delayed records currently parked, by topic and partition.",
		}, []string{"topic", "partition"}),

		RetentionSegmentsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "toymq_retention_segments_reclaimed_total",
			Help: "Sealed WAL segments dropped by the retention sweeper.",
		}),

		RetentionBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "toymq_retention_bytes_reclaimed_total",
			Help: "Bytes reclaimed by the retention sweeper.",
		}),

		PartitionLatestMsgID: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "toymq_partition_latest_msgid",
			Help: "Latest (head) msgID per topic/partition.",
		}, []string{"topic", "partition"}),

		WALSegments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "toymq_wal_segments",
			Help: "Live WAL segment count per topic/partition.",
		}, []string{"topic", "partition"}),

		CommandErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_command_errors_total",
			Help: "Command/protocol errors, by verb and error code.",
		}, []string{"verb", "code"}),

		PublishFailureTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "toymq_publish_failure_total",
			Help: "Publishes rejected or failed, by topic.",
		}, []string{"topic"}),
	}

	r.MustRegister(
		m.PublishTotal,
		m.PublishDupTotal,
		m.PublishBytes,
		m.SubscribeTotal,
		m.InflightMessages,
		m.RedeliveryTotal,
		m.WALAppendSeconds,
		m.ActiveSessions,
		m.ActiveSubscriptions,
		m.TopicCount,
		m.OffsetsFlushTotal,
		m.AckTotal,
		m.NackTotal,
		m.ConsumerLag,
		m.DLQTotal,
		m.DelayedPending,
		m.RetentionSegmentsTotal,
		m.RetentionBytesTotal,
		m.PartitionLatestMsgID,
		m.WALSegments,
		m.CommandErrorsTotal,
		m.PublishFailureTotal,
	)
	return m
}

// partLabel renders a partition index as a stable string label.
func partLabel(partition int) string {
	return strconv.Itoa(partition)
}

// IncPublish bumps the publish counter (and payload byte counter)
// for topic. No-op if m is nil.
func (m *Metrics) IncPublish(topic string, payloadBytes int) {
	if m == nil {
		return
	}
	m.PublishTotal.WithLabelValues(topic).Inc()
	m.PublishBytes.WithLabelValues(topic).Add(float64(payloadBytes))
}

// IncPublishDup bumps the dedupe-hit counter.
func (m *Metrics) IncPublishDup(topic string) {
	if m == nil {
		return
	}
	m.PublishDupTotal.WithLabelValues(topic).Inc()
}

// IncSubscribe bumps the subscribe counter.
func (m *Metrics) IncSubscribe(topic string) {
	if m == nil {
		return
	}
	m.SubscribeTotal.WithLabelValues(topic).Inc()
}

// SetInflight records the current per-(topic, consumer) inflight
// count. Called from broker.Consumer.Ack / .Nack / runDelivery
// after mutating the inflight map.
func (m *Metrics) SetInflight(topic, consumer string, count int) {
	if m == nil {
		return
	}
	m.InflightMessages.WithLabelValues(topic, consumer).Set(float64(count))
}

// IncRedelivery bumps the redelivery counter, bucketing attempts as
// "2", "3-5", or "6+".
func (m *Metrics) IncRedelivery(topic string, attempts int) {
	if m == nil {
		return
	}
	bucket := redeliveryBucket(attempts)
	m.RedeliveryTotal.WithLabelValues(topic, bucket).Inc()
}

func redeliveryBucket(attempts int) string {
	switch {
	case attempts <= 2:
		return "2"
	case attempts <= 5:
		return "3-5"
	default:
		return "6+"
	}
}

// ObserveWALAppend records the WAL Append+fsync duration in seconds.
func (m *Metrics) ObserveWALAppend(topic string, seconds float64) {
	if m == nil {
		return
	}
	m.WALAppendSeconds.WithLabelValues(topic).Observe(seconds)
}

// ObserveWALAppendExemplar records the WAL Append+fsync duration and,
// when traceID is a valid non-empty W3C trace id, attaches it as an
// exemplar so a latency spike in Grafana links straight to its trace
// (ADR 0027). Falls back to a plain Observe when the histogram does not
// support exemplars or traceID is empty.
func (m *Metrics) ObserveWALAppendExemplar(topic string, seconds float64, traceID string) {
	if m == nil {
		return
	}
	obs := m.WALAppendSeconds.WithLabelValues(topic)
	if traceID != "" {
		if eo, ok := obs.(prometheus.ExemplarObserver); ok {
			eo.ObserveWithExemplar(seconds, prometheus.Labels{"trace_id": traceID})
			return
		}
	}
	obs.Observe(seconds)
}

// IncAck bumps the ack counter for a (topic, partition).
func (m *Metrics) IncAck(topic string, partition int) {
	if m == nil {
		return
	}
	m.AckTotal.WithLabelValues(topic, partLabel(partition)).Inc()
}

// IncNack bumps the nack counter for a (topic, partition).
func (m *Metrics) IncNack(topic string, partition int) {
	if m == nil {
		return
	}
	m.NackTotal.WithLabelValues(topic, partLabel(partition)).Inc()
}

// SetConsumerLag records the per-(topic, partition, consumer) lag
// (latest msgID minus last acked). A lag of 0 means fully caught up.
func (m *Metrics) SetConsumerLag(topic string, partition int, consumer string, lag int) {
	if m == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	m.ConsumerLag.WithLabelValues(topic, partLabel(partition), consumer).Set(float64(lag))
}

// IncDLQ bumps the dead-letter counter. trigger is "nack" or "timeout".
func (m *Metrics) IncDLQ(topic, trigger string) {
	if m == nil {
		return
	}
	m.DLQTotal.WithLabelValues(topic, trigger).Inc()
}

// SetDelayedPending records the count of un-fired delayed records
// parked in a (topic, partition).
func (m *Metrics) SetDelayedPending(topic string, partition, n int) {
	if m == nil {
		return
	}
	m.DelayedPending.WithLabelValues(topic, partLabel(partition)).Set(float64(n))
}

// AddRetentionReclaimed records a retention-sweep reclaim: the number
// of sealed segments dropped and the bytes freed.
func (m *Metrics) AddRetentionReclaimed(segments int, bytes int64) {
	if m == nil {
		return
	}
	m.RetentionSegmentsTotal.Add(float64(segments))
	m.RetentionBytesTotal.Add(float64(bytes))
}

// SetPartitionLatestMsgID records the head msgID of a (topic, partition).
func (m *Metrics) SetPartitionLatestMsgID(topic string, partition int, msgID uint64) {
	if m == nil {
		return
	}
	m.PartitionLatestMsgID.WithLabelValues(topic, partLabel(partition)).Set(float64(msgID))
}

// SetWALSegments records the live segment count of a (topic, partition).
func (m *Metrics) SetWALSegments(topic string, partition, n int) {
	if m == nil {
		return
	}
	m.WALSegments.WithLabelValues(topic, partLabel(partition)).Set(float64(n))
}

// IncCommandError bumps the command-error counter for a verb and code.
func (m *Metrics) IncCommandError(verb, code string) {
	if m == nil {
		return
	}
	m.CommandErrorsTotal.WithLabelValues(verb, code).Inc()
}

// IncPublishFailure bumps the publish-failure counter for a topic.
func (m *Metrics) IncPublishFailure(topic string) {
	if m == nil {
		return
	}
	m.PublishFailureTotal.WithLabelValues(topic).Inc()
}

// IncSessions / DecSessions / IncSubs / DecSubs maintain the
// session and subscription gauges.
func (m *Metrics) IncSessions() {
	if m == nil {
		return
	}
	m.ActiveSessions.Inc()
}

// DecSessions decrements the active-sessions gauge.
func (m *Metrics) DecSessions() {
	if m == nil {
		return
	}
	m.ActiveSessions.Dec()
}

// IncSubs increments the active-subscriptions gauge.
func (m *Metrics) IncSubs() {
	if m == nil {
		return
	}
	m.ActiveSubscriptions.Inc()
}

// DecSubs decrements the active-subscriptions gauge.
func (m *Metrics) DecSubs() {
	if m == nil {
		return
	}
	m.ActiveSubscriptions.Dec()
}

// SetTopicCount records the current number of topics known to the
// broker. Called from broker.getOrCreateTopic on the new-topic path.
func (m *Metrics) SetTopicCount(n int) {
	if m == nil {
		return
	}
	m.TopicCount.Set(float64(n))
}

// IncOffsetsFlush bumps the offsets-flush counter with the result
// label set to "ok" or "error".
func (m *Metrics) IncOffsetsFlush(topic string, ok bool) {
	if m == nil {
		return
	}
	result := "ok"
	if !ok {
		result = "error"
	}
	m.OffsetsFlushTotal.WithLabelValues(topic, result).Inc()
}
