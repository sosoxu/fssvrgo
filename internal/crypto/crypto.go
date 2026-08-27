package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"golang.org/x/crypto/scrypt"
)

const (
	chunkedMagic      = "FSSENC01"
	chunkedChunkSize  = 4 * 1024 * 1024
	chunkedHeaderSize = 8 + 4 + 8
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
	in, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat input file: %w", err)
	}
	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create encrypted file: %w", err)
	}
	if err := cs.encryptChunked(in, out, info.Size()); err != nil {
		out.Close()
		os.Remove(outputPath)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(outputPath)
		return fmt.Errorf("failed to sync encrypted file: %w", err)
	}
	return out.Close()
}

func (cs *CryptoService) DecryptFile(inputPath, outputPath string) error {
	return cs.DecryptFileStreaming(inputPath, outputPath)
}

func (cs *CryptoService) DecryptFileStreaming(inputPath, outputPath string) error {
	inFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	header := make([]byte, len(chunkedMagic))
	if _, err := io.ReadFull(inFile, header); err != nil {
		return fmt.Errorf("failed to read encrypted file header: %w", err)
	}
	if string(header) != chunkedMagic {
		if _, err := inFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		return cs.decryptLegacyFile(inFile, outputPath)
	}
	if _, err := inFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create decrypted file: %w", err)
	}
	if err := cs.decryptChunked(inFile, outFile); err != nil {
		outFile.Close()
		os.Remove(outputPath)
		return err
	}
	if err := outFile.Sync(); err != nil {
		outFile.Close()
		os.Remove(outputPath)
		return fmt.Errorf("failed to sync decrypted file: %w", err)
	}
	return outFile.Close()
}

func (cs *CryptoService) encryptChunked(src io.Reader, dst io.Writer, plaintextSize int64) error {
	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("failed to create GCM: %w", err)
	}
	header := make([]byte, chunkedHeaderSize)
	copy(header, chunkedMagic)
	binary.BigEndian.PutUint32(header[8:12], chunkedChunkSize)
	binary.BigEndian.PutUint64(header[12:20], uint64(plaintextSize))
	if _, err := dst.Write(header); err != nil {
		return fmt.Errorf("failed to write encrypted header: %w", err)
	}
	buffer := make([]byte, chunkedChunkSize)
	var written int64
	for {
		n, readErr := io.ReadFull(src, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return fmt.Errorf("failed to read plaintext chunk: %w", readErr)
		}
		if n == 0 {
			break
		}
		nonce := make([]byte, gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return fmt.Errorf("failed to generate chunk nonce: %w", err)
		}
		ciphertext := gcm.Seal(nil, nonce, buffer[:n], nil)
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(n))
		if _, err := dst.Write(length); err != nil {
			return err
		}
		if _, err := dst.Write(nonce); err != nil {
			return err
		}
		if _, err := dst.Write(ciphertext); err != nil {
			return err
		}
		written += int64(n)
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if written != plaintextSize {
		return fmt.Errorf("plaintext size changed during encryption: expected %d, got %d", plaintextSize, written)
	}
	return nil
}

func (cs *CryptoService) decryptChunked(src io.Reader, dst io.Writer) error {
	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	header := make([]byte, chunkedHeaderSize)
	if _, err := io.ReadFull(src, header); err != nil {
		return fmt.Errorf("failed to read encrypted header: %w", err)
	}
	if string(header[:8]) != chunkedMagic {
		return fmt.Errorf("unsupported encrypted file format")
	}
	chunkSize := binary.BigEndian.Uint32(header[8:12])
	expectedSize := int64(binary.BigEndian.Uint64(header[12:20]))
	if chunkSize == 0 || chunkSize > 64*1024*1024 {
		return fmt.Errorf("invalid encrypted chunk size: %d", chunkSize)
	}
	var plaintextSize int64
	length := make([]byte, 4)
	for {
		if _, err := io.ReadFull(src, length); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read encrypted chunk length: %w", err)
		}
		plainLen := binary.BigEndian.Uint32(length)
		if plainLen == 0 || plainLen > chunkSize {
			return fmt.Errorf("invalid encrypted plaintext chunk length: %d", plainLen)
		}
		nonce := make([]byte, gcm.NonceSize())
		if _, err := io.ReadFull(src, nonce); err != nil {
			return fmt.Errorf("failed to read encrypted chunk nonce: %w", err)
		}
		ciphertext := make([]byte, int(plainLen)+gcm.Overhead())
		if _, err := io.ReadFull(src, ciphertext); err != nil {
			return fmt.Errorf("failed to read encrypted chunk: %w", err)
		}
		plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return fmt.Errorf("failed to authenticate encrypted chunk: %w", err)
		}
		if _, err := dst.Write(plaintext); err != nil {
			return fmt.Errorf("failed to write plaintext chunk: %w", err)
		}
		plaintextSize += int64(len(plaintext))
	}
	if plaintextSize != expectedSize {
		return fmt.Errorf("decrypted size mismatch: expected %d, got %d", expectedSize, plaintextSize)
	}
	return nil
}

func (cs *CryptoService) decryptLegacyFile(src io.Reader, outputPath string) error {
	encData, err := io.ReadAll(src)
	if err != nil {
		return fmt.Errorf("failed to read legacy encrypted file: %w", err)
	}
	decrypted, err := cs.Decrypt(string(encData))
	if err != nil {
		return fmt.Errorf("failed to decrypt legacy file data: %w", err)
	}
	if err := os.WriteFile(outputPath, []byte(decrypted), 0600); err != nil {
		return fmt.Errorf("failed to write decrypted legacy file: %w", err)
	}
	return nil
}

func (cs *CryptoService) IsEnabled() bool {
	return cs.enabled
}
