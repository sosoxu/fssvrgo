package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
	uploadSizeBytes     prometheus.Histogram
	activeUploads       prometheus.Gauge
}

func NewMetrics() *Metrics {
	m := &Metrics{
		httpRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fsserver_http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),
		httpRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "fsserver_http_request_duration_seconds",
				Help:    "HTTP request duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "path"},
		),
		uploadSizeBytes: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "fsserver_upload_size_bytes",
				Help:    "Size of uploaded files in bytes",
				Buckets: prometheus.ExponentialBuckets(1024, 2, 10),
			},
		),
		activeUploads: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "fsserver_active_uploads",
				Help: "Number of active uploads",
			},
		),
	}

	prometheus.MustRegister(m.httpRequestsTotal)
	prometheus.MustRegister(m.httpRequestDuration)
	prometheus.MustRegister(m.uploadSizeBytes)
	prometheus.MustRegister(m.activeUploads)

	return m
}

func (m *Metrics) RecordHTTPRequest(method, path string, status int, duration time.Duration) {
	m.httpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(status)).Inc()
	m.httpRequestDuration.WithLabelValues(method, path).Observe(duration.Seconds())
}

func (m *Metrics) RecordUpload(size float64) {
	m.uploadSizeBytes.Observe(size)
}

func (m *Metrics) IncActiveUploads() {
	m.activeUploads.Inc()
}

func (m *Metrics) DecActiveUploads() {
	m.activeUploads.Dec()
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.Handler()
}
