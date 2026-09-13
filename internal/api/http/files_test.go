package http

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// TestMaxBytesReaderEnforcesPlaintextLimit covers the guard that keeps the
// encrypted upload path from streaming more than maxUploadSize of plaintext
// into storage.
func TestMaxBytesReaderEnforcesPlaintextLimit(t *testing.T) {
	tests := []struct {
		name      string
		limit     int64
		payload   string
		wantData  string
		wantError bool
	}{
		{name: "under the limit", limit: 10, payload: "abc", wantData: "abc"},
		{name: "exactly at the limit", limit: 3, payload: "abc", wantData: "abc"},
		// An io.Reader may return data together with an error; the crossing
		// byte is reported along with the sentinel and the caller
		// (EncryptStream) discards it because it checks the error first.
		{name: "over the limit", limit: 3, payload: "abcd", wantData: "abcd", wantError: true},
		{name: "far over the limit", limit: 1, payload: strings.Repeat("x", 4096), wantData: "xx", wantError: true},
		{name: "empty body at zero limit", limit: 0, payload: "", wantData: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &maxBytesReader{r: strings.NewReader(tc.payload), remaining: tc.limit}
			data, err := io.ReadAll(reader)

			if tc.wantError {
				if !errors.Is(err, errPlaintextTooLarge) {
					t.Fatalf("err = %v, want errPlaintextTooLarge", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(data) != tc.wantData {
				t.Errorf("read %q, want %q", data, tc.wantData)
			}
		})
	}
}

// TestMaxBytesReaderStaysFailed pins the behavior after the first violation:
// the reader keeps reporting the sentinel instead of silently resuming.
func TestMaxBytesReaderStaysFailed(t *testing.T) {
	reader := &maxBytesReader{r: strings.NewReader("0123456789"), remaining: 2}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(reader, buf); !errors.Is(err, errPlaintextTooLarge) {
		t.Fatalf("first read err = %v, want errPlaintextTooLarge", err)
	}
	if _, err := reader.Read(buf); !errors.Is(err, errPlaintextTooLarge) {
		t.Errorf("second read err = %v, want errPlaintextTooLarge", err)
	}
}
