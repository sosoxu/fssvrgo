package tests

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/crypto"
	"github.com/sosoxu/fssvrgo/tests/testutil"
)

// TestEncryptedUploadDownloadRoundTrip covers the whole encrypted path end to
// end with the chunked format (#119): a payload larger than one chunk is
// uploaded through the HTTP API, stored as ciphertext, and downloaded again
// byte-for-byte — including Range requests, which are answered from the
// decrypted plaintext.
func TestEncryptedUploadDownloadRoundTrip(t *testing.T) {
	ts, err := testutil.NewTestServerWithCrypto()
	if err != nil {
		t.Skipf("test server unavailable: %v", err)
	}
	defer ts.Cleanup()

	if ts.CryptoSvc == nil || !ts.CryptoSvc.IsEnabled() {
		t.Fatal("test server did not enable encryption")
	}

	// Two-and-a-half chunks: exercises the chunk loop and a partial final chunk.
	payload := make([]byte, crypto.StreamChunkSize*2+512*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	filePath := "/enc/roundtrip.bin"

	resp := doUpload(t, ts, filePath, "roundtrip.bin", payload)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status = %d, body = %s", resp.StatusCode, body)
	}

	// The stored object must be ciphertext, not the plaintext.
	stored, err := ts.Store.Read(t.Context(), filePath)
	if err != nil {
		t.Fatalf("read stored object: %v", err)
	}
	if bytes.Contains(stored, payload[:1024]) {
		t.Error("stored object contains plaintext")
	}
	// Chunked encryption keeps the object close to the plaintext size; the old
	// whole-message base64 format inflated it by ~1/3.
	if len(stored) > len(payload)+crypto.StreamChunkSize {
		t.Errorf("stored size %d exceeds plaintext %d by more than one chunk", len(stored), len(payload))
	}

	// Full download.
	downloaded := doGet(t, ts, filePath)
	if !bytes.Equal(downloaded, payload) {
		t.Fatalf("downloaded %d bytes, want %d (content mismatch)", len(downloaded), len(payload))
	}

	// Range request over the encrypted object: the server decrypts to a temp
	// file and serves the requested plaintext slice.
	start, end := int64(700_000), int64(1_400_000)
	req, err := http.NewRequest("GET", ts.BaseURL+"/api/v1/files"+filePath, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	rangeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("range request: %v", err)
	}
	defer rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status = %d, want 206", rangeResp.StatusCode)
	}
	partial, err := io.ReadAll(rangeResp.Body)
	if err != nil {
		t.Fatalf("read range body: %v", err)
	}
	if !bytes.Equal(partial, payload[start:end+1]) {
		t.Fatalf("range body mismatch: got %d bytes, want %d", len(partial), end-start+1)
	}
}

// TestEncryptedSmallFileRoundTrip covers the buffered branch of the encrypted
// upload (payload under smallUploadThreshold).
func TestEncryptedSmallFileRoundTrip(t *testing.T) {
	ts, err := testutil.NewTestServerWithCrypto()
	if err != nil {
		t.Skipf("test server unavailable: %v", err)
	}
	defer ts.Cleanup()

	payload := []byte("small encrypted payload — 加密小文件")
	resp := doUpload(t, ts, "/enc/small.txt", "small.txt", payload)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status = %d, body = %s", resp.StatusCode, body)
	}

	if got := doGet(t, ts, "/enc/small.txt"); !bytes.Equal(got, payload) {
		t.Errorf("downloaded %q, want %q", got, payload)
	}
}

// TestEncryptedChunkedUploadAndDownloadSession covers the resumable path with
// encryption on: the session upload encrypts on completion, and the download
// session decrypts the stored object back into a readable temp file.
func TestEncryptedChunkedUploadAndDownloadSession(t *testing.T) {
	ts, err := testutil.NewTestServerWithCrypto()
	if err != nil {
		t.Skipf("test server unavailable: %v", err)
	}
	defer ts.Cleanup()

	payload := make([]byte, crypto.StreamChunkSize+4096)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	filePath := "/enc/chunked.bin"

	sessionID := doChunkedCreate(t, ts, filePath, "chunked.bin", int64(len(payload)), "")
	const chunkSize = 512 * 1024
	for offset := 0; offset < len(payload); offset += chunkSize {
		end := offset + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		doChunkedUploadChunk(t, ts, sessionID, payload[offset:end], int64(offset))
	}
	resp := doChunkedComplete(t, ts, sessionID)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("complete status = %d, body = %s", resp.StatusCode, body)
	}

	// The stored object must be ciphertext.
	stored, err := ts.Store.Read(t.Context(), filePath)
	if err != nil {
		t.Fatalf("read stored object: %v", err)
	}
	if bytes.Contains(stored, payload[:1024]) {
		t.Error("stored object contains plaintext")
	}

	// A download session decrypts the object; reading it back chunk by chunk
	// must reproduce the original payload.
	downloadID, err := ts.TransferSvc.CreateDownloadSession(t.Context(), filePath, "test")
	if err != nil {
		t.Fatalf("CreateDownloadSession: %v", err)
	}
	defer ts.TransferSvc.AbortDownload(t.Context(), downloadID)

	var got []byte
	const readSize = 300 * 1024
	for offset := 0; offset < len(payload); offset += readSize {
		chunk, err := ts.TransferSvc.DownloadChunk(t.Context(), downloadID, readSize, int64(offset))
		if err != nil {
			t.Fatalf("DownloadChunk(offset=%d): %v", offset, err)
		}
		if len(chunk) == 0 {
			break
		}
		got = append(got, chunk...)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decrypted %d bytes, want %d", len(got), len(payload))
	}

	// The HTTP download path must serve the same plaintext.
	if httpGot := doGet(t, ts, filePath); !bytes.Equal(httpGot, payload) {
		t.Errorf("HTTP download returned %d bytes, want %d", len(httpGot), len(payload))
	}
}

func doGet(t *testing.T, ts *testutil.TestServer, filePath string) []byte {
	t.Helper()
	resp, err := http.Get(ts.BaseURL + "/api/v1/files" + filePath)
	if err != nil {
		t.Fatalf("GET %s: %v", filePath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s status = %d, body = %s", filePath, resp.StatusCode, body)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return data
}
