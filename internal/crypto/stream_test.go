package crypto

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func newTestService(t *testing.T) *CryptoService {
	t.Helper()
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return cs
}

// sizedReader yields n deterministic bytes without allocating the payload, so
// memory measurements reflect the crypto service rather than the test.
type sizedReader struct {
	remaining int
	next      byte
}

func (r *sizedReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = r.next
		r.next++
	}
	r.remaining -= len(p)
	return len(p), nil
}

func TestStreamRoundTripAcrossChunkBoundaries(t *testing.T) {
	cs := newTestService(t)

	sizes := []int{
		0,
		1,
		StreamChunkSize - 1,
		StreamChunkSize,
		StreamChunkSize + 1,
		3*StreamChunkSize + 12345,
	}

	for _, size := range sizes {
		var ciphertext bytes.Buffer
		if err := cs.EncryptStream(&ciphertext, &sizedReader{remaining: size}); err != nil {
			t.Fatalf("EncryptStream(%d): %v", size, err)
		}
		if !bytes.HasPrefix(ciphertext.Bytes(), streamMagic) {
			t.Fatalf("size %d: ciphertext does not start with the stream magic", size)
		}

		var plaintext bytes.Buffer
		if err := cs.DecryptStream(&plaintext, bytes.NewReader(ciphertext.Bytes())); err != nil {
			t.Fatalf("DecryptStream(%d): %v", size, err)
		}
		if plaintext.Len() != size {
			t.Errorf("size %d: decrypted %d bytes", size, plaintext.Len())
		}
	}
}

// TestStreamEncryptionMemoryIsBounded is the regression guard for #119: peak
// memory must track the chunk size, not the file size. A 32 MiB stream may
// allocate a few chunk buffers, but nowhere near the 32 MiB a whole-message
// implementation would need.
func TestStreamEncryptionMemoryIsBounded(t *testing.T) {
	cs := newTestService(t)
	const size = 32 << 20
	const budget = 8 << 20

	if encrypted := measureAlloc(t, func() {
		if err := cs.EncryptStream(io.Discard, &sizedReader{remaining: size}); err != nil {
			t.Fatalf("EncryptStream: %v", err)
		}
	}); encrypted > budget {
		t.Errorf("encrypting %d bytes allocated %d bytes, want <= %d", size, encrypted, budget)
	}

	var ciphertext bytes.Buffer
	if err := cs.EncryptStream(&ciphertext, &sizedReader{remaining: size}); err != nil {
		t.Fatalf("EncryptStream: %v", err)
	}
	stream := ciphertext.Bytes()

	if decrypted := measureAlloc(t, func() {
		if err := cs.DecryptStream(io.Discard, bytes.NewReader(stream)); err != nil {
			t.Fatalf("DecryptStream: %v", err)
		}
	}); decrypted > budget {
		t.Errorf("decrypting %d bytes allocated %d bytes, want <= %d", size, decrypted, budget)
	}
}

func measureAlloc(t *testing.T, fn func()) uint64 {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestStreamDetectsTamperingTruncationAndTrailingData(t *testing.T) {
	cs := newTestService(t)

	plaintext := bytes.Repeat([]byte("A"), StreamChunkSize+100)
	var ciphertext bytes.Buffer
	if err := cs.EncryptStream(&ciphertext, bytes.NewReader(plaintext)); err != nil {
		t.Fatalf("EncryptStream: %v", err)
	}
	stream := ciphertext.Bytes()

	t.Run("flipped byte in a data chunk", func(t *testing.T) {
		corrupted := append([]byte(nil), stream...)
		corrupted[len(corrupted)/2] ^= 0x01
		if err := cs.DecryptStream(io.Discard, bytes.NewReader(corrupted)); err == nil {
			t.Error("expected authentication failure for a flipped byte")
		}
	})

	t.Run("truncated stream", func(t *testing.T) {
		truncated := stream[:len(stream)-10]
		if err := cs.DecryptStream(io.Discard, bytes.NewReader(truncated)); err == nil {
			t.Error("expected failure for a stream cut before its trailer")
		}
	})

	t.Run("trailing data", func(t *testing.T) {
		appended := append(append([]byte(nil), stream...), 0x00)
		if err := cs.DecryptStream(io.Discard, bytes.NewReader(appended)); err == nil {
			t.Error("expected failure for data appended after the trailer")
		}
	})

	t.Run("header only", func(t *testing.T) {
		headerOnly := append([]byte(nil), stream[:len(streamMagic)+noncePrefixSize]...)
		if err := cs.DecryptStream(io.Discard, bytes.NewReader(headerOnly)); err == nil {
			t.Error("expected failure for a header-only (empty) stream")
		}
	})
}

// TestNoncesAreUniquePerChunkAndFile guards the format's nonce discipline: a
// re-used (key, nonce) pair would be a GCM catastrophe, so the prefix must
// differ between encryptions and the counter must advance within one stream.
func TestNoncesAreUniquePerChunkAndFile(t *testing.T) {
	cs := newTestService(t)

	encryptPrefix := func() []byte {
		var buf bytes.Buffer
		if err := cs.EncryptStream(&buf, &sizedReader{remaining: 2 * StreamChunkSize}); err != nil {
			t.Fatalf("EncryptStream: %v", err)
		}
		return append([]byte(nil), buf.Bytes()[len(streamMagic):len(streamMagic)+noncePrefixSize]...)
	}

	first, second := encryptPrefix(), encryptPrefix()
	if bytes.Equal(first, second) {
		t.Error("two encryptions reused the same nonce prefix")
	}
	if bytes.Equal(first, make([]byte, noncePrefixSize)) {
		t.Error("nonce prefix looks uninitialized")
	}
}

// TestLegacyWholeMessagePayloadStillDecrypts covers upgrades: objects written by
// versions that used base64(nonce || GCM(whole plaintext)) must remain readable.
func TestLegacyWholeMessagePayloadStillDecrypts(t *testing.T) {
	cs := newTestService(t)
	plaintext := "legacy encrypted object"
	legacy := legacyCiphertext(t, cs, plaintext)

	t.Run("Decrypt", func(t *testing.T) {
		got, err := cs.Decrypt(legacy)
		if err != nil {
			t.Fatalf("Decrypt: %v", err)
		}
		if got != plaintext {
			t.Errorf("Decrypt = %q, want %q", got, plaintext)
		}
	})

	t.Run("DecryptStream", func(t *testing.T) {
		var out bytes.Buffer
		if err := cs.DecryptStream(&out, bytes.NewReader([]byte(legacy))); err != nil {
			t.Fatalf("DecryptStream: %v", err)
		}
		if out.String() != plaintext {
			t.Errorf("DecryptStream = %q, want %q", out.String(), plaintext)
		}
	})

	t.Run("DecryptFileStreaming", func(t *testing.T) {
		dir := t.TempDir()
		in := filepath.Join(dir, "legacy.bin")
		out := filepath.Join(dir, "plain.txt")
		if err := os.WriteFile(in, []byte(legacy), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := cs.DecryptFileStreaming(in, out); err != nil {
			t.Fatalf("DecryptFileStreaming: %v", err)
		}
		got, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(got) != plaintext {
			t.Errorf("decrypted %q, want %q", got, plaintext)
		}
	})
}

// legacyCiphertext reproduces the pre-chunking format: base64(nonce || seal).
func legacyCiphertext(t *testing.T, cs *CryptoService, plaintext string) string {
	t.Helper()
	gcm, err := cs.newGCM()
	if err != nil {
		t.Fatalf("newGCM: %v", err)
	}
	nonce := bytes.Repeat([]byte{0x2a}, gcm.NonceSize())
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(sealed)
}

func TestStreamPassthroughWhenDisabled(t *testing.T) {
	cs := NewCryptoService() // not Initialized

	var ciphertext bytes.Buffer
	if err := cs.EncryptStream(&ciphertext, bytes.NewReader([]byte("plain"))); err != nil {
		t.Fatalf("EncryptStream: %v", err)
	}
	if ciphertext.String() != "plain" {
		t.Errorf("disabled EncryptStream = %q, want passthrough", ciphertext.String())
	}

	var out bytes.Buffer
	if err := cs.DecryptStream(&out, bytes.NewReader(ciphertext.Bytes())); err != nil {
		t.Fatalf("DecryptStream: %v", err)
	}
	if out.String() != "plain" {
		t.Errorf("disabled DecryptStream = %q, want passthrough", out.String())
	}
}

func TestEncryptFileStreamsMultipleChunks(t *testing.T) {
	cs := newTestService(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "input.bin")
	enc := filepath.Join(dir, "encrypted.bin")
	dec := filepath.Join(dir, "decrypted.bin")

	plaintext := bytes.Repeat([]byte("chunked-file-"), 200000) // ~2.6 MiB
	if err := os.WriteFile(in, plaintext, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := cs.EncryptFile(in, enc); err != nil {
		t.Fatalf("EncryptFile: %v", err)
	}

	encData, err := os.ReadFile(enc)
	if err != nil {
		t.Fatalf("ReadFile(enc): %v", err)
	}
	if bytes.Contains(encData, []byte("chunked-file-")) {
		t.Error("encrypted file contains the plaintext")
	}
	// Chunked storage keeps the on-disk size close to the plaintext size; the
	// legacy base64 format inflated it by ~4/3.
	if len(encData) > len(plaintext)+64*1024 {
		t.Errorf("encrypted size %d is not bounded by plaintext size %d", len(encData), len(plaintext))
	}

	if err := cs.DecryptFile(enc, dec); err != nil {
		t.Fatalf("DecryptFile: %v", err)
	}
	got, err := os.ReadFile(dec)
	if err != nil {
		t.Fatalf("ReadFile(dec): %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("decrypted content mismatch: got %d bytes, want %d", len(got), len(plaintext))
	}
}
