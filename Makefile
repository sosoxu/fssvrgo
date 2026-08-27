.PHONY: build test test-short vet lint perf-file-io clean

build:
	go build ./cmd/fsserver/

test:
	go test ./internal/... ./tests/...

test-short:
	go test -short ./internal/...

vet:
	go vet ./...

lint: vet

PERF_SIZES ?= 1K,1M,100M,1G
PERF_OPERATIONS ?= sequential_write,sequential_read,concurrent_write,concurrent_read
PERF_CONCURRENCY ?= 4
PERF_WARMUPS ?= 0
PERF_SAMPLES ?= 3
PERF_REPORT ?= docs/performance/file_io_performance_results.json

perf-file-io:
	FSS_PERF_ENABLE=1 \
	FSS_PERF_SIZES=$(PERF_SIZES) \
	FSS_PERF_OPERATIONS=$(PERF_OPERATIONS) \
	FSS_PERF_CONCURRENCY=$(PERF_CONCURRENCY) \
	FSS_PERF_WARMUPS=$(PERF_WARMUPS) \
	FSS_PERF_SAMPLES=$(PERF_SAMPLES) \
	FSS_PERF_REPORT=$(abspath $(PERF_REPORT)) \
	go test ./tests -run '^TestFileIOPerformance$$' -count=1 -v

clean:
	rm -f fsserver
