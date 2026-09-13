package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	HTTPDuration       *prometheus.HistogramVec
	HTTPRequests       *prometheus.CounterVec
	OutboxPending      prometheus.Gauge
	OutboxOldestAge    prometheus.Gauge
	ProducerFailures   prometheus.Counter
	ConsumerFailures   *prometheus.CounterVec
	DLQTotal           *prometheus.CounterVec
	WebSocketConnected prometheus.Gauge
}

func New(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tab_http_request_duration_seconds", Help: "HTTP request latency.",
		}, []string{"method", "route", "status"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tab_http_requests_total", Help: "HTTP requests.",
		}, []string{"method", "route", "status"}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tab_outbox_pending", Help: "Pending or claimed outbox rows.",
		}),
		OutboxOldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tab_outbox_oldest_age_seconds", Help: "Age of oldest unpublished outbox row.",
		}),
		ProducerFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tab_producer_failures_total", Help: "Outbox publication failures.",
		}),
		ConsumerFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tab_consumer_failures_total", Help: "Consumer processing failures.",
		}, []string{"consumer"}),
		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tab_dlq_events_total", Help: "Events moved to a DLQ.",
		}, []string{"consumer"}),
		WebSocketConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tab_websocket_connections", Help: "Active WebSocket connections.",
		}),
	}
	registerer.MustRegister(
		m.HTTPDuration, m.HTTPRequests, m.OutboxPending, m.OutboxOldestAge,
		m.ProducerFailures, m.ConsumerFailures, m.DLQTotal, m.WebSocketConnected,
	)
	return m
}

func (m *Metrics) ObserveHTTP(method, route string, status int, duration time.Duration) {
	code := strconv.Itoa(status)
	m.HTTPDuration.WithLabelValues(method, route, code).Observe(duration.Seconds())
	m.HTTPRequests.WithLabelValues(method, route, code).Inc()
}
