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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the registered set of Prometheus collectors the broker
// reports. Construct via New; pass the pointer down to broker /
// server / wal. A nil pointer turns every observation into a no-op.
type Metrics struct {
	PublishTotal       *prometheus.CounterVec
	PublishDupTotal    *prometheus.CounterVec
	PublishBytes       *prometheus.CounterVec
	SubscribeTotal     *prometheus.CounterVec
	InflightMessages   *prometheus.GaugeVec
	RedeliveryTotal    *prometheus.CounterVec
	WALAppendSeconds   *prometheus.HistogramVec
	ActiveSessions     prometheus.Gauge
	ActiveSubscriptions prometheus.Gauge
	TopicCount         prometheus.Gauge
	OffsetsFlushTotal  *prometheus.CounterVec
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
	)
	return m
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
