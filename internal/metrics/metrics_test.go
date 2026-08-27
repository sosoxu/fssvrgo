package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsBucketsAndRegistryHandler(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewMetricsWithRegistry(registry)
	m.RecordHTTPRequest("POST", "/api/v1/files", 201, 90*time.Second)
	m.RecordGRPCRequest("/fsserver.FileService/UploadFile", "OK", 90*time.Second)
	m.RecordUpload(2 * 1024 * 1024 * 1024)
	m.IncActiveUploads()
	m.DecActiveUploads()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	assertHistogramBucket := func(name string, upperBound float64) {
		t.Helper()
		for _, family := range families {
			if family.GetName() != name || len(family.Metric) == 0 {
				continue
			}
			for _, bucket := range family.Metric[0].GetHistogram().Bucket {
				if bucket.GetUpperBound() == upperBound && bucket.GetCumulativeCount() == 1 {
					return
				}
			}
			t.Fatalf("metric %s has no populated bucket with upper bound %v", name, upperBound)
		}
		t.Fatalf("metric family %s not found", name)
	}
	assertHistogramBucket("fsserver_http_request_duration_seconds", 120)
	assertHistogramBucket("fsserver_grpc_request_duration_seconds", 120)
	assertHistogramBucket("fsserver_upload_size_bytes", 4*1024*1024*1024)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 200 {
		t.Fatalf("metrics handler status = %d", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "fsserver_upload_size_bytes_count 1") ||
		!strings.Contains(body, "fsserver_active_uploads 0") {
		t.Fatalf("metrics handler did not use the configured registry:\n%s", body)
	}
}
