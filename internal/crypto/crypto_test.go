package crypto

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validHexKey returns a freshly generated 32-byte hex-encoded key (64 chars).
func validHexKey(t *testing.T) string {
	t.Helper()
	cs := NewCryptoService()
	k := cs.GenerateKey()
	if k == "" {
		t.Fatalf("GenerateKey returned empty key")
	}
	return k
}

func TestInit(t *testing.T) {
	t.Run("hex key", func(t *testing.T) {
		cs := NewCryptoService()
		hexKey := validHexKey(t)
		if err := cs.Init(hexKey); err != nil {
			t.Fatalf("Init with hex key failed: %v", err)
		}
		if !cs.IsEnabled() {
			t.Errorf("expected IsEnabled=true after Init with hex key")
		}
		// Verify the key works by performing a round-trip.
		plaintext := "hello hex key"
		ct, err := cs.Encrypt(plaintext)
		if err != nil {
			t.Fatalf("Encrypt failed: %v", err)
		}
		pt, err := cs.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt failed: %v", err)
		}
		if pt != plaintext {
			t.Errorf("expected %q, got %q", plaintext, pt)
		}
	})

	t.Run("passphrase shorter than 32 bytes", func(t *testing.T) {
		cs := NewCryptoService()
		// Passphrase shorter than 32 bytes - should be zero-padded to 32 bytes.
		if err := cs.Init("my secret passphrase"); err != nil {
			t.Fatalf("Init with short passphrase failed: %v", err)
		}
		if !cs.IsEnabled() {
			t.Errorf("expected IsEnabled=true after Init with passphrase")
		}
		// Round-trip should still work.
		plaintext := "passphrase data"
		ct, err := cs.Encrypt(plaintext)
		if err != nil {
			t.Fatalf("Encrypt failed: %v", err)
		}
		pt, err := cs.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt failed: %v", err)
		}
		if pt != plaintext {
			t.Errorf("expected %q, got %q", plaintext, pt)
		}
	})

	t.Run("passphrase longer than 32 bytes", func(t *testing.T) {
		cs := NewCryptoService()
		longPass := strings.Repeat("a", 64)
		if err := cs.Init(longPass); err != nil {
			t.Fatalf("Init with long passphrase failed: %v", err)
		}
		if !cs.IsEnabled() {
			t.Errorf("expected IsEnabled=true after Init with long passphrase")
		}
		plaintext := "long passphrase round trip"
		ct, err := cs.Encrypt(plaintext)
		if err != nil {
			t.Fatalf("Encrypt failed: %v", err)
		}
		pt, err := cs.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt failed: %v", err)
		}
		if pt != plaintext {
			t.Errorf("expected %q, got %q", plaintext, pt)
		}
	})

	t.Run("empty key", func(t *testing.T) {
		cs := NewCryptoService()
		err := cs.Init("")
		if err == nil {
			t.Errorf("expected error for empty key, got nil")
		}
		if cs.IsEnabled() {
			t.Errorf("expected IsEnabled=false after failed Init with empty key")
		}
	})
}

func TestEncryptDecrypt(t *testing.T) {
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	cases := []string{
		"",
		"hello world",
		"中文测试",
		"line1\nline2\ttab",
		strings.Repeat("a", 1024),
	}

	for _, want := range cases {
		ct, err := cs.Encrypt(want)
		if err != nil {
			t.Fatalf("Encrypt(%q) failed: %v", want, err)
		}
		if ct == want {
			t.Errorf("ciphertext should differ from plaintext for %q", want)
		}
		got, err := cs.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt failed: %v", err)
		}
		if got != want {
			t.Errorf("round-trip mismatch: want %q, got %q", want, got)
		}
	}
}

func TestEncryptDifferentData(t *testing.T) {
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Different plaintexts should produce different ciphertexts.
	ct1, err := cs.Encrypt("first")
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	ct2, err := cs.Encrypt("second")
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	if ct1 == ct2 {
		t.Errorf("different plaintexts produced identical ciphertexts: %q", ct1)
	}

	// The same plaintext should also produce different ciphertexts because the
	// nonce is randomized per call.
	ct3, err := cs.Encrypt("same")
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	ct4, err := cs.Encrypt("same")
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	if ct3 == ct4 {
		t.Errorf("encrypting same plaintext twice should yield different ciphertexts due to random nonce")
	}

	// Both should still decrypt back to "same".
	if pt, err := cs.Decrypt(ct3); err != nil || pt != "same" {
		t.Errorf("Decrypt(ct3) = %q, %v; want \"same\", nil", pt, err)
	}
	if pt, err := cs.Decrypt(ct4); err != nil || pt != "same" {
		t.Errorf("Decrypt(ct4) = %q, %v; want \"same\", nil", pt, err)
	}
}

func TestDecryptInvalidData(t *testing.T) {
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	t.Run("invalid base64", func(t *testing.T) {
		// "#####" contains characters outside the base64 alphabet.
		_, err := cs.Decrypt("not valid base64!!!")
		if err == nil {
			t.Errorf("expected error for invalid base64 input, got nil")
		}
	})

	t.Run("valid base64 but too short for nonce", func(t *testing.T) {
		// 4 bytes of data, less than the 12-byte nonce size.
		short := base64.StdEncoding.EncodeToString([]byte("abcd"))
		_, err := cs.Decrypt(short)
		if err == nil {
			t.Errorf("expected error for ciphertext too short, got nil")
		}
	})

	t.Run("corrupted ciphertext", func(t *testing.T) {
		ct, err := cs.Encrypt("secret message")
		if err != nil {
			t.Fatalf("Encrypt failed: %v", err)
		}
		data, err := base64.StdEncoding.DecodeString(ct)
		if err != nil {
			t.Fatalf("base64 decode failed: %v", err)
		}
		// Flip a byte near the end to break GCM authentication.
		last := len(data) - 1
		data[last] ^= 0x01
		corrupted := base64.StdEncoding.EncodeToString(data)
		_, err = cs.Decrypt(corrupted)
		if err == nil {
			t.Errorf("expected error for corrupted ciphertext, got nil")
		}
	})
}

func TestEncryptFile(t *testing.T) {
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	encPath := filepath.Join(dir, "encrypted.txt")
	decPath := filepath.Join(dir, "decrypted.txt")

	plaintext := "file content for EncryptFile/DecryptFile round trip"
	if err := os.WriteFile(inputPath, []byte(plaintext), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := cs.EncryptFile(inputPath, encPath); err != nil {
		t.Fatalf("EncryptFile failed: %v", err)
	}

	// The encrypted file must differ from the original plaintext.
	encData, err := os.ReadFile(encPath)
	if err != nil {
		t.Fatalf("ReadFile(encPath) failed: %v", err)
	}
	if string(encData) == plaintext {
		t.Errorf("encrypted file should not contain the raw plaintext")
	}

	if err := cs.DecryptFile(encPath, decPath); err != nil {
		t.Fatalf("DecryptFile failed: %v", err)
	}

	decData, err := os.ReadFile(decPath)
	if err != nil {
		t.Fatalf("ReadFile(decPath) failed: %v", err)
	}
	if string(decData) != plaintext {
		t.Errorf("decrypted file content mismatch: want %q, got %q", plaintext, string(decData))
	}
}

func TestDecryptFileStreaming(t *testing.T) {
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.bin")
	encPath := filepath.Join(dir, "encrypted.bin")
	decPath := filepath.Join(dir, "decrypted.bin")

	// Use a moderately sized payload so the streaming path is exercised.
	plaintext := strings.Repeat("streaming-decryption-test-", 2000) // ~52 KB
	if err := os.WriteFile(inputPath, []byte(plaintext), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := cs.EncryptFile(inputPath, encPath); err != nil {
		t.Fatalf("EncryptFile failed: %v", err)
	}

	if err := cs.DecryptFileStreaming(encPath, decPath); err != nil {
		t.Fatalf("DecryptFileStreaming failed: %v", err)
	}

	decData, err := os.ReadFile(decPath)
	if err != nil {
		t.Fatalf("ReadFile(decPath) failed: %v", err)
	}
	if string(decData) != plaintext {
		t.Errorf("streaming decrypted content mismatch: want length %d, got length %d", len(plaintext), len(decData))
	}
}

func TestGenerateKey(t *testing.T) {
	cs := NewCryptoService()
	k := cs.GenerateKey()
	if k == "" {
		t.Fatalf("GenerateKey returned empty string")
	}
	// A 32-byte key encoded as hex must be 64 characters long.
	if len(k) != 64 {
		t.Errorf("expected key length 64 hex chars, got %d", len(k))
	}
	decoded, err := hex.DecodeString(k)
	if err != nil {
		t.Errorf("generated key is not valid hex: %v", err)
	}
	if len(decoded) != 32 {
		t.Errorf("expected decoded key length 32 bytes, got %d", len(decoded))
	}

	// Two consecutive calls should return different keys (randomness).
	k2 := cs.GenerateKey()
	if k2 == "" {
		t.Fatalf("second GenerateKey returned empty string")
	}
	if k == k2 {
		t.Errorf("GenerateKey should return distinct keys across calls, got %q twice", k)
	}
}

func TestIsEnabled(t *testing.T) {
	cs := NewCryptoService()
	if cs.IsEnabled() {
		t.Errorf("expected IsEnabled=false before Init")
	}
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if !cs.IsEnabled() {
		t.Errorf("expected IsEnabled=true after successful Init")
	}
}

func TestEncryptDisabled(t *testing.T) {
	cs := NewCryptoService()
	// Without Init, encryption is disabled and Encrypt should return the
	// plaintext unchanged.
	plaintext := "unencrypted because crypto service is disabled"
	got, err := cs.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt on disabled service returned error: %v", err)
	}
	if got != plaintext {
		t.Errorf("Encrypt on disabled service should return plaintext; got %q, want %q", got, plaintext)
	}

	// Decrypt should also be a no-op when disabled.
	pt, err := cs.Decrypt(plaintext)
	if err != nil {
		t.Fatalf("Decrypt on disabled service returned error: %v", err)
	}
	if pt != plaintext {
		t.Errorf("Decrypt on disabled service should return input; got %q, want %q", pt, plaintext)
	}
}

func TestLargeData(t *testing.T) {
	cs := NewCryptoService()
	if err := cs.Init(validHexKey(t)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// 1 MB payload.
	plaintext := strings.Repeat("x", 1024*1024)
	ct, err := cs.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt of 1 MB data failed: %v", err)
	}
	if ct == plaintext {
		t.Errorf("ciphertext should differ from large plaintext")
	}
	got, err := cs.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt of 1 MB data failed: %v", err)
	}
	if len(got) != len(plaintext) {
		t.Errorf("decrypted length mismatch: want %d, got %d", len(plaintext), len(got))
	}
	if got != plaintext {
		t.Errorf("decrypted content mismatch for 1 MB payload")
	}
}
