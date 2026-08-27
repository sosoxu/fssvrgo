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
	grpcRequestsTotal   *prometheus.CounterVec
	grpcRequestDuration *prometheus.HistogramVec
	uploadSizeBytes     prometheus.Histogram
	activeUploads       prometheus.Gauge
	gatherer            prometheus.Gatherer
}

func NewMetrics() *Metrics {
	return newMetrics(prometheus.DefaultRegisterer, prometheus.DefaultGatherer)
}

func NewMetricsWithRegistry(registry *prometheus.Registry) *Metrics {
	return newMetrics(registry, registry)
}

func newMetrics(registerer prometheus.Registerer, gatherer prometheus.Gatherer) *Metrics {
	durationBuckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}
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
				Buckets: durationBuckets,
			},
			[]string{"method", "path"},
		),
		grpcRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "fsserver_grpc_requests_total",
				Help: "Total number of gRPC requests",
			},
			[]string{"method", "code"},
		),
		grpcRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "fsserver_grpc_request_duration_seconds",
				Help:    "gRPC request duration in seconds",
				Buckets: durationBuckets,
			},
			[]string{"method"},
		),
		uploadSizeBytes: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "fsserver_upload_size_bytes",
				Help:    "Size of uploaded files in bytes",
				Buckets: prometheus.ExponentialBuckets(1024, 4, 12),
			},
		),
		activeUploads: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "fsserver_active_uploads",
				Help: "Number of upload data requests currently being processed",
			},
		),
		gatherer: gatherer,
	}

	registerer.MustRegister(m.httpRequestsTotal)
	registerer.MustRegister(m.httpRequestDuration)
	registerer.MustRegister(m.grpcRequestsTotal)
	registerer.MustRegister(m.grpcRequestDuration)
	registerer.MustRegister(m.uploadSizeBytes)
	registerer.MustRegister(m.activeUploads)

	return m
}

func (m *Metrics) RecordGRPCRequest(method, code string, duration time.Duration) {
	m.grpcRequestsTotal.WithLabelValues(method, code).Inc()
	m.grpcRequestDuration.WithLabelValues(method).Observe(duration.Seconds())
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
	return promhttp.HandlerFor(m.gatherer, promhttp.HandlerOpts{})
}
