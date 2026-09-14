# Go FileServer Performance Test Report

**Generated:** 2026-09-11
**Test Environment:** Linux, PostgreSQL 16 + Redis 7, Local Storage, 3-instance cluster
**Test Suite:** `TestPostgreSQLRedis_Performance`
**Result:** PASS (14.25s) | 1/1 test passed

> **Scope:** File sizes 1KB, 64KB, 256KB, 1MB, 10MB, 50MB. Each operation runs 3 iterations and reports the average. Streaming operations use 256KB chunks.
>
> **Protocol note:** HTTP rows measure the full HTTP stack (network request → server → storage). gRPC rows (`Upload`/`Download`/`StreamUpload`/`StreamDownload`) call the service/transfer layer in-process and therefore exclude network serialization — they represent the ceiling of the storage layer, not a wire-level gRPC measurement.
>
> **Caveat:** HTTP streaming download returned 0 bytes for files smaller than one chunk (1KB, 64KB) due to a limitation of the test helper; those entries are marked N/A below.

---

## 1. HTTP Upload

Single-request file upload via HTTP POST multipart/form-data.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 6.25ms | 0.16 | ✅ |
| 64KB | 6.96ms | 8.98 | ✅ |
| 256KB | 10.17ms | 24.58 | ✅ |
| 1MB | 27.26ms | 36.69 | ✅ |
| 10MB | 109.25ms | 91.54 | ✅ |
| 50MB | 480.78ms | 104.00 | ✅ |

**Analysis:** HTTP upload throughput scales with file size; small files are dominated by fixed request overhead (0.16 MB/s at 1KB), reaching ~104 MB/s at 50MB.

---

## 2. HTTP Download

Single-request file download via HTTP GET.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 1.17ms | 0.83 | ✅ |
| 64KB | 1.54ms | 40.51 | ✅ |
| 256KB | 2.50ms | 99.89 | ✅ |
| 1MB | 4.34ms | 230.42 | ✅ |
| 10MB | 33.31ms | 300.23 | ✅ |
| 50MB | 81.66ms | 612.29 | ✅ |

**Analysis:** HTTP download is consistently faster than upload, reaching 612 MB/s at 50MB. Streaming with `io.Copy` keeps memory overhead low.

---

## 3. HTTP Streaming Upload (Chunked, 256KB chunks)

Create session → upload chunks (PUT) → complete session.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 10.50ms | 0.09 | ✅ |
| 64KB | 13.17ms | 4.74 | ✅ |
| 256KB | 18.42ms | 13.57 | ✅ |
| 1MB | 50.53ms | 19.79 | ✅ |
| 10MB | 303.36ms | 32.96 | ✅ |
| 50MB | 1.338s | 37.38 | ✅ |

**Analysis:** With 256KB chunks, streaming upload is slower than single-request upload because per-chunk session/HTTP overhead dominates. Larger chunk sizes would narrow this gap.

---

## 4. HTTP Streaming Download (Chunked, 256KB chunks)

Chunked file download via HTTP Range requests.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | — | — | ⚠️ N/A (helper < chunk) |
| 64KB | — | — | ⚠️ N/A (helper < chunk) |
| 256KB | 3.46ms | 72.35 | ✅ |
| 1MB | 14.92ms | 67.01 | ✅ |
| 10MB | 133.35ms | 74.99 | ✅ |
| 50MB | 586.75ms | 85.21 | ✅ |

**Analysis:** HTTP streaming download is stable at 67–85 MB/s for files ≥ 256KB. Small files below the chunk size are not exercised by the test helper.

---

## 5. gRPC (Service Layer) Upload

Direct `FileManager.UploadFile()` call — in-process, no network overhead.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 2.78ms | 0.35 | ✅ |
| 64KB | 4.51ms | 13.85 | ✅ |
| 256KB | 6.54ms | 38.23 | ✅ |
| 1MB | 12.80ms | 78.15 | ✅ |
| 10MB | 48.93ms | 204.39 | ✅ |
| 50MB | 189.85ms | 263.37 | ✅ |

**Analysis:** The service layer reaches 263 MB/s at 50MB, roughly 2.2–2.5x faster than the HTTP path, isolating the cost of the HTTP stack.

---

## 6. gRPC (Service Layer) Download

Direct `FileManager.DownloadFile()` call — in-process, no network overhead.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 314µs | 3.11 | ✅ |
| 64KB | 442µs | 141.42 | ✅ |
| 256KB | 965µs | 259.20 | ✅ |
| 1MB | 1.95ms | 512.48 | ✅ |
| 10MB | 10.20ms | 980.46 | ✅ |
| 50MB | 44.66ms | 1119.69 | ✅ |

**Analysis:** The fastest path in the suite — 50MB downloads in 44.7ms at 1119.69 MB/s, demonstrating the raw throughput of the Go storage layer.

---

## 7. gRPC Streaming Upload (256KB chunks)

`FileTransferService` session → chunk upload → complete.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 6.70ms | 0.15 | ✅ |
| 64KB | 8.17ms | 7.65 | ✅ |
| 256KB | 10.21ms | 24.50 | ✅ |
| 1MB | 22.30ms | 44.84 | ✅ |
| 10MB | 87.33ms | 114.50 | ✅ |
| 50MB | 361.69ms | 138.24 | ✅ |

**Analysis:** In-process streaming upload reaches 138 MB/s at 50MB — 3.7x faster than HTTP streaming upload, reflecting the absence of HTTP chunk encoding overhead.

---

## 8. gRPC Streaming Download (256KB chunks)

`FileTransferService` session → chunk download → complete.

| File Size | Duration | Throughput (MB/s) | Status |
|-----------|----------|-------------------|--------|
| 1KB | 675µs | 1.45 | ✅ |
| 64KB | 690µs | 90.61 | ✅ |
| 256KB | 1.82ms | 137.19 | ✅ |
| 1MB | 8.40ms | 119.07 | ✅ |
| 10MB | 88.65ms | 112.80 | ✅ |
| 50MB | 359.24ms | 139.18 | ✅ |

**Analysis:** gRPC streaming download is stable at 112–139 MB/s for larger files, with the 1KB case limited by fixed session setup cost.

---

## HTTP vs gRPC Comparison

| File Size | Operation | HTTP (MB/s) | gRPC (MB/s) | gRPC Speedup |
|-----------|-----------|-------------|-------------|--------------|
| 1KB | Upload | 0.16 | 0.35 | 2.24x |
| 1KB | Download | 0.83 | 3.11 | 3.74x |
| 1KB | Stream Upload | 0.09 | 0.15 | 1.57x |
| 64KB | Upload | 8.98 | 13.85 | 1.54x |
| 64KB | Download | 40.51 | 141.42 | 3.49x |
| 256KB | Upload | 24.58 | 38.23 | 1.55x |
| 256KB | Download | 99.89 | 259.20 | 2.59x |
| 1MB | Upload | 36.69 | 78.15 | 2.13x |
| 1MB | Download | 230.42 | 512.48 | 2.22x |
| 10MB | Upload | 91.54 | 204.39 | 2.23x |
| 10MB | Download | 300.23 | 980.46 | 3.27x |
| 50MB | Upload | 104.00 | 263.37 | 2.53x |
| 50MB | Download | 612.29 | 1119.69 | 1.83x |

**gRPC (service layer) is faster than HTTP across every size and operation**, by 1.5x–3.7x. The largest gaps appear on downloads of medium files (10MB: 3.27x).

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| **Min Throughput** | 0.09 MB/s (1KB HTTP Stream Upload) |
| **Max Throughput** | 1119.69 MB/s (50MB gRPC Download) |
| **Avg Throughput** | 141.37 MB/s |
| **Median Throughput** | 73.67 MB/s |
| **Tests Passed** | 1/1 (100%) |

---

## Key Findings

### 1. Protocol Comparison (50MB)
| Method | Duration | Throughput |
|--------|----------|------------|
| HTTP Upload | 480.78ms | 104.00 MB/s |
| HTTP Download | 81.66ms | 612.29 MB/s |
| HTTP Stream Upload | 1.338s | 37.38 MB/s |
| HTTP Stream Download | 586.75ms | 85.21 MB/s |
| gRPC Upload | 189.85ms | 263.37 MB/s |
| gRPC Download | 44.66ms | 1119.69 MB/s |
| gRPC Stream Upload | 361.69ms | 138.24 MB/s |
| gRPC Stream Download | 359.24ms | 139.18 MB/s |

### 2. Download vs Upload
Download is consistently faster than upload for the same protocol and size — the read path is simpler and avoids multipart parsing and write amplification. At 50MB, gRPC download is 4.25x faster than gRPC upload.

### 3. Small File Performance
- Files ≤ 256KB: throughput is dominated by fixed request/session overhead (0.09–260 MB/s).
- Files 1–10MB: throughput climbs steadily as overhead is amortized (19.79–980.46 MB/s).
- Files ≥ 10MB: sustained high throughput (up to 1119.69 MB/s).

### 4. Streaming vs Single Request
- With 256KB chunks, single-request upload outperforms chunked upload (per-chunk overhead); larger chunks would change this trade-off.
- Streaming's value is memory efficiency and resumability rather than peak throughput at this chunk size.

### 5. Recommendations
- **Small files (< 1MB):** single-request HTTP upload/download for simplicity.
- **Large files (> 10MB):** prefer larger chunk sizes for streaming to reduce per-chunk overhead.
- **Maximum throughput:** use the gRPC/transfer service path directly.
- **Downloads:** always faster than uploads — size chunk/concurrency around the download path when tuning.
