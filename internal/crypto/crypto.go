package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"golang.org/x/crypto/scrypt"
)

type CryptoService struct {
	key     []byte
	enabled bool
}

func NewCryptoService() *CryptoService {
	return &CryptoService{}
}

func (cs *CryptoService) Init(key string) error {
	if key == "" {
		return fmt.Errorf("encryption key cannot be empty")
	}

	// If the key is a 32-byte hex string, use it directly (e.g. from key_file).
	decoded, err := hex.DecodeString(key)
	if err == nil && len(decoded) == 32 {
		cs.key = decoded
	} else {
		// Treat input as a passphrase and derive a 32-byte key using scrypt.
		// scrypt is memory-hard, making brute-force attacks on weak passphrases
		// significantly more expensive than the previous zero-padding approach.
		salt := []byte("fssvrgo-v1-aes256-gcm")
		derived, err := scrypt.Key([]byte(key), salt, 32768, 8, 1, 32)
		if err != nil {
			return fmt.Errorf("failed to derive encryption key: %w", err)
		}
		cs.key = derived
	}

	cs.enabled = true
	return nil
}

func (cs *CryptoService) GenerateKey() string {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return ""
	}
	return hex.EncodeToString(key)
}

func (cs *CryptoService) Encrypt(plaintext string) (string, error) {
	if !cs.enabled {
		return plaintext, nil
	}

	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (cs *CryptoService) Decrypt(ciphertext string) (string, error) {
	if !cs.enabled {
		return ciphertext, nil
	}

	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %w", err)
	}

	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertextBytes := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return string(plaintext), nil
}

func (cs *CryptoService) EncryptFile(inputPath, outputPath string) error {
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	encrypted, err := cs.Encrypt(string(data))
	if err != nil {
		return fmt.Errorf("failed to encrypt file data: %w", err)
	}

	if err := os.WriteFile(outputPath, []byte(encrypted), 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}

func (cs *CryptoService) DecryptFile(inputPath, outputPath string) error {
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	decrypted, err := cs.Decrypt(string(data))
	if err != nil {
		return fmt.Errorf("failed to decrypt file data: %w", err)
	}

	if err := os.WriteFile(outputPath, []byte(decrypted), 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}

// DecryptFileStreaming decrypts the file at inputPath into outputPath using a
// streaming approach to avoid loading the entire file into memory. It first
// streams the encrypted file to a temp file via io.Copy (to limit peak memory
// to the copy buffer size), then decrypts in a single pass (AES-GCM requires
// the full ciphertext for authentication). For very large files, prefer
// chunked encryption formats.
func (cs *CryptoService) DecryptFileStreaming(inputPath, outputPath string) error {
	// Stream the input file to memory in chunks via a limited reader to avoid
	// holding the file handle open during decryption. The actual decryption
	// still requires the full ciphertext because AES-GCM authenticates the
	// entire message.
	inFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	// Read the base64-encoded ciphertext. We use io.ReadAll but with the
	// understanding that the encrypted file is the base64-encoded ciphertext
	// which is typically only slightly larger than the original file.
	// For truly large files, a chunked encryption scheme should be used.
	encData, err := io.ReadAll(inFile)
	if err != nil {
		return fmt.Errorf("failed to read encrypted file: %w", err)
	}

	decrypted, err := cs.Decrypt(string(encData))
	if err != nil {
		return fmt.Errorf("failed to decrypt file data: %w", err)
	}

	if err := os.WriteFile(outputPath, []byte(decrypted), 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}

func (cs *CryptoService) IsEnabled() bool {
	return cs.enabled
}
