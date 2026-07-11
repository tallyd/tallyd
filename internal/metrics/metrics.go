// Package metrics exposes tallyd's Prometheus metrics. Producing packages
// (receiver, batcher) never import this package directly — they depend on
// small local interfaces that *Metrics satisfies structurally, matching
// the same decoupling pattern used for Sink/Router/Acker/DeadLetterSink.
package metrics

import (
	"net/http"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns tallyd's metric collectors and a dedicated registry so
// tests can create isolated instances without touching the global
// prometheus.DefaultRegisterer.
type Metrics struct {
	registry *prometheus.Registry

	eventsReceivedTotal prometheus.Counter
	eventsAckedTotal    *prometheus.CounterVec
	flushLatency        *prometheus.HistogramVec
	sendErrorsTotal     *prometheus.CounterVec
	walUnacked          prometheus.Gauge
	dlqDepth            *prometheus.GaugeVec
}

// New creates a Metrics instance with all collectors registered.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		eventsReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_received_total",
			Help: "Total number of events accepted by the HTTP receiver.",
		}),
		eventsAckedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_acked_total",
			Help: "Total number of event/provider acks, by terminal disposition.",
		}, []string{"provider", "disposition"}),
		flushLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "provider_flush_latency_seconds",
			Help:    "Latency of a batcher flush (Encode+Send) per provider.",
			Buckets: prometheus.DefBuckets,
		}, []string{"provider"}),
		sendErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "provider_send_errors_total",
			Help: "Total send errors per provider, by resulting disposition.",
		}, []string{"provider", "disposition"}),
		walUnacked: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wal_unacked_entries",
			Help: "Number of WAL entries not yet resolved by every target provider.",
		}),
		dlqDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dlq_depth",
			Help: "Number of events dead-lettered per provider.",
		}, []string{"provider"}),
	}

	reg.MustRegister(
		m.eventsReceivedTotal,
		m.eventsAckedTotal,
		m.flushLatency,
		m.sendErrorsTotal,
		m.walUnacked,
		m.dlqDepth,
	)

	return m
}

// RecordEventsReceived satisfies receiver.MetricsRecorder.
func (m *Metrics) RecordEventsReceived(n int) {
	m.eventsReceivedTotal.Add(float64(n))
}

// RecordAck satisfies batcher.MetricsRecorder.
func (m *Metrics) RecordAck(provider string, disposition adapter.Disposition) {
	m.eventsAckedTotal.WithLabelValues(provider, disposition.String()).Inc()
}

// ObserveFlushLatency satisfies batcher.MetricsRecorder.
func (m *Metrics) ObserveFlushLatency(provider string, d time.Duration) {
	m.flushLatency.WithLabelValues(provider).Observe(d.Seconds())
}

// RecordSendError satisfies batcher.MetricsRecorder.
func (m *Metrics) RecordSendError(provider string, disposition adapter.Disposition) {
	m.sendErrorsTotal.WithLabelValues(provider, disposition.String()).Inc()
}

// SetWALUnacked sets the wal_unacked_entries gauge. Call periodically
// (e.g. from a short-interval ticker in the pipeline) with wal.UnackedCount().
func (m *Metrics) SetWALUnacked(n int) {
	m.walUnacked.Set(float64(n))
}

// SetDLQDepth sets the dlq_depth{provider} gauge. Call periodically with
// dlq.Depth(provider) for each configured provider.
func (m *Metrics) SetDLQDepth(provider string, n int) {
	m.dlqDepth.WithLabelValues(provider).Set(float64(n))
}

// Handler returns the http.Handler to mount at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
