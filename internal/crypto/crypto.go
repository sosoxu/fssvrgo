// Package crypto encrypts stored objects with AES-256-GCM.
//
// # Formats
//
// The streaming format ("FSSGCM" v1) encrypts the plaintext in independent
// authenticated chunks, so encryption and decryption hold one chunk in memory
// regardless of file size:
//
//	header   magic (8 bytes) || nonce prefix (8 random bytes)
//	records  kind (1 byte) || sealed length (4 bytes, big endian) || sealed
//
// where sealed = ciphertext || 16-byte GCM tag and the per-record nonce is
// nonce-prefix || big-endian record counter. Record kinds are 0x01 for a data
// chunk and 0x02 for the trailer: the trailer's authenticated payload is the
// total plaintext length, so a truncated file (dangling data chunk) and an
// appended file (bytes after the trailer) are both detected rather than
// silently accepted.
//
// Files written before this format are a single base64-encoded
// nonce || GCM(whole plaintext) blob. Decryption still accepts them (see
// DecryptStream) but buffering is unavoidable for that shape, which is exactly
// why it is legacy: new writes never produce it.
package crypto

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/scrypt"
)

// StreamChunkSize is the plaintext size of one encrypted chunk. It bounds the
// memory a single encrypt/decrypt call holds, including when many transfers run
// concurrently.
const StreamChunkSize = 1 << 20

// streamMagic starts with a NUL byte so it can never collide with the legacy
// format, whose on-disk representation is base64 text (base64 never contains
// NUL).
var streamMagic = []byte{0x00, 'F', 'S', 'S', 'G', 'C', 'M', 0x01}

const (
	recordData = 0x01
	recordEnd  = 0x02

	noncePrefixSize = 8
	recordHeaderLen = 5 // kind + uint32 length
	endMarkerLen    = 8 // authenticated trailer payload

	// maxRecordLen rejects absurd lengths from a corrupt or hostile stream
	// before allocating for them.
	maxRecordLen = StreamChunkSize + 64
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

func (cs *CryptoService) IsEnabled() bool {
	return cs.enabled
}

func (cs *CryptoService) newGCM() (cipher.AEAD, error) {
	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	if gcm.NonceSize() != noncePrefixSize+4 {
		return nil, fmt.Errorf("unexpected GCM nonce size %d", gcm.NonceSize())
	}
	return gcm, nil
}

// EncryptStream encrypts src into dst in independent authenticated chunks.
// Peak memory is one chunk plus the GCM state, no matter how large src is.
//
// When encryption is disabled the plaintext is copied through unchanged, which
// mirrors Encrypt's behavior.
func (cs *CryptoService) EncryptStream(dst io.Writer, src io.Reader) error {
	if !cs.enabled {
		_, err := io.Copy(dst, src)
		return err
	}

	gcm, err := cs.newGCM()
	if err != nil {
		return err
	}

	prefix := make([]byte, noncePrefixSize)
	if _, err := io.ReadFull(rand.Reader, prefix); err != nil {
		return fmt.Errorf("failed to generate nonce prefix: %w", err)
	}
	if _, err := dst.Write(append(append([]byte(nil), streamMagic...), prefix...)); err != nil {
		return fmt.Errorf("failed to write stream header: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	copy(nonce, prefix)

	buf := make([]byte, StreamChunkSize)
	sealed := make([]byte, 0, StreamChunkSize+gcm.Overhead())
	counter := uint32(0)
	var total int64

	for {
		n, readErr := io.ReadFull(src, buf)
		// A hard read error (including the upload-size guard's sentinel) must
		// not contribute its partial chunk: only a clean EOF or a short final
		// read are legitimate endings.
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return fmt.Errorf("failed to read plaintext: %w", readErr)
		}
		if n > 0 {
			binary.BigEndian.PutUint32(nonce[noncePrefixSize:], counter)
			sealed = gcm.Seal(sealed[:0], nonce, buf[:n], nil)
			if err := writeRecord(dst, recordData, sealed); err != nil {
				return err
			}
			counter++
			total += int64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	// Authenticated trailer: records the plaintext length, which is what makes
	// truncation detectable (a stream without a valid trailer is rejected).
	binary.BigEndian.PutUint32(nonce[noncePrefixSize:], counter)
	trailer := make([]byte, endMarkerLen)
	binary.BigEndian.PutUint64(trailer, uint64(total))
	sealed = gcm.Seal(sealed[:0], nonce, trailer, nil)
	return writeRecord(dst, recordEnd, sealed)
}

func writeRecord(dst io.Writer, kind byte, payload []byte) error {
	if len(payload) > maxRecordLen {
		return fmt.Errorf("encrypted record too large: %d bytes", len(payload))
	}
	header := make([]byte, recordHeaderLen)
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := dst.Write(header); err != nil {
		return fmt.Errorf("failed to write record header: %w", err)
	}
	if _, err := dst.Write(payload); err != nil {
		return fmt.Errorf("failed to write record payload: %w", err)
	}
	return nil
}

// readRecordInto reads one record, reusing scratch when it is large enough so
// that decrypting a large stream does not allocate one buffer per chunk.
func readRecordInto(br *bufio.Reader, scratch []byte) (byte, []byte, error) {
	header := make([]byte, recordHeaderLen)
	if _, err := io.ReadFull(br, header); err != nil {
		// A clean EOF at a record boundary is reported as such; a partial
		// header is a truncated stream.
		if err == io.EOF {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("truncated record header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[1:])
	if length == 0 || length > maxRecordLen {
		return 0, nil, fmt.Errorf("invalid record length %d", length)
	}

	if uint32(cap(scratch)) < length {
		scratch = make([]byte, length)
	}
	payload := scratch[:length]
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, fmt.Errorf("truncated record payload: %w", err)
	}
	return header[0], payload, nil
}

// DecryptStream decrypts src into dst. It accepts both the chunked streaming
// format and the legacy base64 whole-message format; the latter still has to be
// buffered, because a single GCM tag can only be verified against the complete
// ciphertext. Peak memory for the streaming format is one chunk.
func (cs *CryptoService) DecryptStream(dst io.Writer, src io.Reader) error {
	if !cs.enabled {
		_, err := io.Copy(dst, src)
		return err
	}

	br := bufio.NewReaderSize(src, 64*1024)
	if isChunkedStream(br) {
		return cs.decryptChunked(dst, br)
	}

	// Legacy: whole-message GCM, stored base64-encoded.
	data, err := io.ReadAll(br)
	if err != nil {
		return fmt.Errorf("failed to read encrypted data: %w", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		// Not base64 and not a chunked stream: report the framing problem
		// rather than a confusing "too short" error later.
		return fmt.Errorf("unrecognized encrypted payload: not a chunked stream and not legacy base64: %w", err)
	}
	return cs.decryptWholeMessage(dst, bytes.NewReader(decoded))
}

// isChunkedStream peeks at the magic without consuming it.
func isChunkedStream(br *bufio.Reader) bool {
	head, err := br.Peek(len(streamMagic))
	return err == nil && bytes.Equal(head, streamMagic)
}

func (cs *CryptoService) decryptChunked(dst io.Writer, br *bufio.Reader) error {
	gcm, err := cs.newGCM()
	if err != nil {
		return err
	}

	magic := make([]byte, len(streamMagic))
	if _, err := io.ReadFull(br, magic); err != nil {
		return fmt.Errorf("truncated stream header: %w", err)
	}
	prefix := make([]byte, noncePrefixSize)
	if _, err := io.ReadFull(br, prefix); err != nil {
		return fmt.Errorf("truncated stream header: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	copy(nonce, prefix)

	var total int64
	counter := uint32(0)

	// Reused per-chunk buffers: a large stream decrypts with one payload and
	// one plaintext buffer, not one pair per chunk.
	payloadBuf := make([]byte, 0, StreamChunkSize+16)
	plainBuf := make([]byte, 0, StreamChunkSize)

	for {
		kind, payload, err := readRecordInto(br, payloadBuf)
		if err == io.EOF {
			return fmt.Errorf("encrypted stream is truncated: no end marker")
		}
		if err != nil {
			return err
		}

		switch kind {
		case recordData:
			binary.BigEndian.PutUint32(nonce[noncePrefixSize:], counter)
			plain, err := gcm.Open(plainBuf[:0], nonce, payload, nil)
			if err != nil {
				return fmt.Errorf("failed to decrypt chunk %d: %w", counter, err)
			}
			if _, err := dst.Write(plain); err != nil {
				return fmt.Errorf("failed to write plaintext: %w", err)
			}
			plainBuf = plain[:0]
			total += int64(len(plain))
			counter++

		case recordEnd:
			binary.BigEndian.PutUint32(nonce[noncePrefixSize:], counter)
			trailer, err := gcm.Open(nil, nonce, payload, nil)
			if err != nil {
				return fmt.Errorf("failed to authenticate stream trailer: %w", err)
			}
			if len(trailer) != endMarkerLen {
				return fmt.Errorf("malformed stream trailer: %d bytes", len(trailer))
			}
			if want := binary.BigEndian.Uint64(trailer); want != uint64(total) {
				return fmt.Errorf("encrypted stream length mismatch: trailer=%d decoded=%d", want, total)
			}
			if _, err := br.Peek(1); err != io.EOF {
				return fmt.Errorf("encrypted stream has trailing data after the end marker")
			}
			return nil

		default:
			return fmt.Errorf("unknown encrypted record type 0x%02x", kind)
		}
	}
}

// decryptWholeMessage decrypts the legacy format: nonce || GCM(plaintext).
func (cs *CryptoService) decryptWholeMessage(dst io.Writer, src io.Reader) error {
	gcm, err := cs.newGCM()
	if err != nil {
		return err
	}

	data, err := io.ReadAll(src)
	if err != nil {
		return fmt.Errorf("failed to read ciphertext: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt: %w", err)
	}
	if _, err := dst.Write(plaintext); err != nil {
		return fmt.Errorf("failed to write plaintext: %w", err)
	}
	return nil
}

// Encrypt encrypts a string and returns it base64-encoded. It buffers the whole
// value; callers with file or request bodies should use EncryptStream or
// EncryptFile instead.
func (cs *CryptoService) Encrypt(plaintext string) (string, error) {
	if !cs.enabled {
		return plaintext, nil
	}
	var buf bytes.Buffer
	if err := cs.EncryptStream(&buf, strings.NewReader(plaintext)); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// Decrypt reverses Encrypt. It accepts both the chunked format and payloads
// written by older versions (legacy whole-message base64).
func (cs *CryptoService) Decrypt(ciphertext string) (string, error) {
	if !cs.enabled {
		return ciphertext, nil
	}
	data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(ciphertext))
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %w", err)
	}

	var buf bytes.Buffer
	br := bufio.NewReader(bytes.NewReader(data))
	if isChunkedStream(br) {
		if err := cs.decryptChunked(&buf, br); err != nil {
			return "", err
		}
		return buf.String(), nil
	}
	// The base64 layer already decoded the payload, so the legacy branch sees
	// raw nonce || ciphertext.
	if err := cs.decryptWholeMessage(&buf, bytes.NewReader(data)); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// EncryptFile encrypts inputPath into outputPath in chunks, streaming both ends.
func (cs *CryptoService) EncryptFile(inputPath, outputPath string) error {
	in, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	if err := cs.EncryptStream(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("failed to close output file: %w", err)
	}
	return nil
}

// DecryptFile decrypts inputPath into outputPath. It is the buffered-era name
// for what DecryptStream does; both formats are accepted.
func (cs *CryptoService) DecryptFile(inputPath, outputPath string) error {
	return cs.DecryptFileStreaming(inputPath, outputPath)
}

// DecryptFileStreaming decrypts the file at inputPath into outputPath. New
// (chunked) files decrypt one chunk at a time; legacy whole-message files are
// still read into memory, which is the reason the format was replaced.
func (cs *CryptoService) DecryptFileStreaming(inputPath, outputPath string) error {
	in, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	if err := cs.DecryptStream(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("failed to close output file: %w", err)
	}
	return nil
}
