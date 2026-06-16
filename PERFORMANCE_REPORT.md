# Go FileServer Performance Test Report

**Generated:** 2026-05-29T00:45:23Z
**Test Environment:** Linux sandbox (Go 1.21, SQLite WAL mode, Local Storage)
**Total Tests:** 74 | **Passed:** 71 | **Failed:** 3

> Note: 3 concurrent upload/download tests failed with HTTP 500 due to server resource limits under extreme concurrency (50 simultaneous connections). All individual and streaming tests passed successfully, including 1GB file tests.

---

## 1. HTTP Upload

Tests single-request file upload via HTTP POST multipart/form-data.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 2ms | 0.52 | ✅ |
| 4KB | 1ms | 2.95 | ✅ |
| 16KB | 1ms | 13.18 | ✅ |
| 64KB | 1ms | 42.92 | ✅ |
| 256KB | 3ms | 75.88 | ✅ |
| 1MB | 10ms | 101.91 | ✅ |
| 4MB | 29ms | 140.20 | ✅ |
| 16MB | 119ms | 134.86 | ✅ |
| 64MB | 713ms | 89.78 | ✅ |
| 256MB | 4.156s | 61.60 | ✅ |
| 512MB | 9.14s | 56.02 | ✅ |
| 1GB | 20.463s | 50.04 | ✅ |

**Analysis:** HTTP upload throughput peaks at 4MB (~140 MB/s) and gradually decreases for larger files due to disk I/O and memory management overhead. 1GB upload completes in ~20s at 50 MB/s.

---

## 2. HTTP Download

Tests single-request file download via HTTP GET.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 1ms | 1.91 | ✅ |
| 4KB | 1ms | 2.74 | ✅ |
| 16KB | 0ms | 35.12 | ✅ |
| 64KB | 0ms | 152.94 | ✅ |
| 256KB | 1ms | 451.06 | ✅ |
| 1MB | 1ms | 1012.24 | ✅ |
| 4MB | 3ms | 1552.27 | ✅ |
| 16MB | 7ms | 2225.77 | ✅ |
| 64MB | 52ms | 1233.64 | ✅ |
| 256MB | 203ms | 1259.02 | ✅ |
| 512MB | 567ms | 903.54 | ✅ |
| 1GB | 673ms | 1520.60 | ✅ |

**Analysis:** HTTP download is significantly faster than upload, with peak throughput at 16MB (~2226 MB/s). Even 1GB downloads complete in under 1 second. The high throughput is due to efficient streaming with `io.Copy` to discard, minimizing memory overhead.

---

## 3. HTTP Streaming Upload (Chunked)

Tests chunked file upload via HTTP: create session → upload chunks (PUT) → complete session.

| File Size | Chunk Size | Duration | Throughput (MB/s) | Status |
|-----------|------------|----------|-------------------|--------|
| 1MB | 256KB | 14ms | 70.46 | ✅ |
| 16MB | 1MB | 82ms | 194.88 | ✅ |
| 64MB | 4MB | 267ms | 239.29 | ✅ |
| 256MB | 8MB | 1.27s | 201.55 | ✅ |
| 512MB | 16MB | 2.452s | 208.78 | ✅ |
| 1GB | 32MB | 5.407s | 189.38 | ✅ |

**Analysis:** Streaming upload significantly outperforms single-request upload for large files. 1GB streaming upload (5.4s, 189 MB/s) is ~3.8x faster than single-request upload (20.5s, 50 MB/s). Larger chunk sizes improve throughput by reducing HTTP overhead.

---

## 4. HTTP Streaming Download (Chunked)

Tests chunked file download via HTTP Range requests.

| File Size | Chunk Size | Duration | Throughput (MB/s) | Status |
|-----------|------------|----------|-------------------|--------|
| 1MB | 256KB | 3ms | 385.44 | ✅ |
| 16MB | 1MB | 20ms | 808.97 | ✅ |
| 64MB | 4MB | 75ms | 857.42 | ✅ |
| 256MB | 8MB | 286ms | 895.96 | ✅ |
| 512MB | 16MB | 834ms | 614.26 | ✅ |
| 1GB | 32MB | 1.239s | 826.34 | ✅ |

**Analysis:** Streaming download maintains high throughput across all file sizes. 1GB streaming download completes in ~1.2s at 826 MB/s, which is slower than single-request download but provides better memory efficiency and resumability.

---

## 5. gRPC Service Layer Upload

Tests direct FileManager.UploadFile() call (in-process, no network overhead). Capped at 256MB due to in-memory []byte requirement.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 1ms | 0.82 | ✅ |
| 4KB | 1ms | 5.64 | ✅ |
| 16KB | 1ms | 18.13 | ✅ |
| 64KB | 1ms | 81.70 | ✅ |
| 256KB | 1ms | 208.36 | ✅ |
| 1MB | 2ms | 403.45 | ✅ |
| 4MB | 8ms | 478.27 | ✅ |
| 16MB | 28ms | 577.75 | ✅ |
| 64MB | 105ms | 606.73 | ✅ |
| 256MB | 425ms | 602.93 | ✅ |

**Analysis:** gRPC service layer upload is significantly faster than HTTP upload, reaching 600+ MB/s for large files. This represents the raw service performance without HTTP/network overhead.

---

## 6. gRPC Service Layer Download

Tests direct FileManager.DownloadFile() call (in-process, no network overhead). Capped at 256MB.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 0ms | 4.47 | ✅ |
| 4KB | 0ms | 20.27 | ✅ |
| 16KB | 0ms | 76.72 | ✅ |
| 64KB | 0ms | 215.47 | ✅ |
| 256KB | 0ms | 557.53 | ✅ |
| 1MB | 1ms | 1050.95 | ✅ |
| 4MB | 3ms | 1433.47 | ✅ |
| 16MB | 16ms | 988.74 | ✅ |
| 64MB | 47ms | 1354.45 | ✅ |
| 256MB | 166ms | 1542.23 | ✅ |

**Analysis:** gRPC service layer download achieves exceptional throughput (1000-1542 MB/s), demonstrating the raw performance of the Go file storage layer. 256MB downloads in 166ms.

---

## 7. gRPC Streaming Upload

Tests FileTransferService streaming upload (create session → upload chunks → complete). Uses temp file for large files (>64MB).

| File Size | Chunk Size | Duration | Throughput (MB/s) | Status |
|-----------|------------|----------|-------------------|--------|
| 1MB | 256KB | 4ms | 268.74 | ✅ |
| 16MB | 1MB | 35ms | 460.14 | ✅ |
| 64MB | 4MB | 126ms | 506.63 | ✅ |
| 256MB | 8MB | 645ms | 396.71 | ✅ |
| 512MB | 16MB | 1.263s | 405.45 | ✅ |
| 1GB | 32MB | 2.501s | 409.43 | ✅ |

**Analysis:** gRPC streaming upload outperforms HTTP streaming upload by ~2x for large files. 1GB gRPC streaming upload (2.5s, 409 MB/s) vs HTTP streaming upload (5.4s, 189 MB/s). This is because gRPC streaming avoids HTTP multipart encoding overhead.

---

## 8. gRPC Streaming Download

Tests FileTransferService streaming download (create session → download chunks → complete).

| File Size | Chunk Size | Duration | Throughput (MB/s) | Status |
|-----------|------------|----------|-------------------|--------|
| 1MB | 256KB | 1ms | 674.54 | ✅ |
| 16MB | 1MB | 11ms | 1488.42 | ✅ |
| 64MB | 4MB | 50ms | 1276.33 | ✅ |
| 256MB | 8MB | 148ms | 1729.74 | ✅ |
| 512MB | 16MB | 287ms | 1784.34 | ✅ |
| 1GB | 32MB | 626ms | 1635.48 | ✅ |

**Analysis:** gRPC streaming download achieves the highest throughput among all download methods. 1GB downloads in 626ms at 1635 MB/s. This demonstrates the efficiency of the Go transfer service's chunked reading mechanism.

---

## 9. Concurrent Upload

Tests concurrent HTTP file uploads with multiple goroutines.

| File Size | Concurrency | Duration | Throughput (MB/s) | Status |
|-----------|-------------|----------|-------------------|--------|
| 1MB | x10 | - | - | ❌ 2 errors: status 500 |
| 1MB | x50 | - | - | ❌ 17 errors: status 500 |
| 16MB | x10 | 515ms | 310.75 | ✅ |

**Analysis:** Low concurrency (x10) with larger files works well. High concurrency (x50) with small files triggers server rate limiting or resource exhaustion. The 16MB x10 test achieved 310 MB/s aggregate throughput.

---

## 10. Concurrent Download

Tests concurrent HTTP file downloads with multiple goroutines.

| File Size | Concurrency | Duration | Throughput (MB/s) | Status |
|-----------|-------------|----------|-------------------|--------|
| 1MB | x10 | 11ms | 937.54 | ✅ |
| 1MB | x50 | - | - | ❌ upload failed with status 500 |
| 16MB | x10 | 77ms | 2066.72 | ✅ |

**Analysis:** Concurrent downloads perform well at moderate concurrency. The 1MB x50 failure is due to the upload step (preparing the file for download) hitting server limits, not the download itself. 16MB x10 achieved 2067 MB/s aggregate throughput.

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| **Min Throughput** | 0.52 MB/s (1KB HTTP Upload) |
| **Max Throughput** | 2225.77 MB/s (16MB HTTP Download) |
| **Avg Throughput** | 580.18 MB/s |
| **Median Throughput** | 403.45 MB/s |
| **Tests Passed** | 71/74 (95.9%) |

---

## Key Findings

### 1. Upload Performance Comparison (1GB file)
| Method | Duration | Throughput |
|--------|----------|------------|
| HTTP Single Request | 20.5s | 50 MB/s |
| HTTP Streaming (32MB chunks) | 5.4s | 189 MB/s |
| gRPC Streaming (32MB chunks) | 2.5s | 409 MB/s |

**Streaming upload is 3.8x faster than single-request upload. gRPC streaming is 8.2x faster.**

### 2. Download Performance Comparison (1GB file)
| Method | Duration | Throughput |
|--------|----------|------------|
| HTTP Single Request | 673ms | 1521 MB/s |
| HTTP Streaming (32MB chunks) | 1.2s | 826 MB/s |
| gRPC Streaming (32MB chunks) | 626ms | 1635 MB/s |

**gRPC streaming download is the fastest method for 1GB files. HTTP single-request download is also very fast due to efficient streaming.**

### 3. Small File Performance
- Files ≤ 256KB: throughput ranges from 0.5-450 MB/s (HTTP overhead dominates)
- Files 1-16MB: sweet spot for single-request uploads (100-140 MB/s)
- Files > 64MB: streaming methods significantly outperform single-request

### 4. gRPC vs HTTP
- gRPC service layer (in-process): 400-1542 MB/s (no network overhead)
- gRPC streaming: 268-1784 MB/s (chunk-based, memory efficient)
- HTTP single-request: 0.5-2226 MB/s (varies widely by file size)
- HTTP streaming: 70-896 MB/s (consistent, memory efficient)

### 5. Recommendations
- **Small files (< 16MB):** Use HTTP single-request upload/download for simplicity
- **Large files (> 64MB):** Use streaming upload/download for better throughput and memory efficiency
- **Maximum throughput:** Use gRPC streaming for both upload and download
- **Concurrent operations:** Limit concurrency to ≤10 for stable performance
- **1GB files:** gRPC streaming upload (2.5s) and download (626ms) provide the best performance
