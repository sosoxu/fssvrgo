package storage

import (
	"errors"
	"os"

	"github.com/minio/minio-go/v7"
)

// IsNotExist reports whether err indicates that a storage object/path does not
// exist. It works across both backends:
//   - LocalStorage: os.ErrNotExist (wrapped *os.PathError)
//   - MinIOStorage: S3 NoSuchKey / NotFound error codes
//
// Use this instead of os.IsNotExist when handling errors returned by the
// StorageAdapter interface, so the same code path works for both local and
// object storage backends.
func IsNotExist(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var errResp minio.ErrorResponse
	if errors.As(err, &errResp) {
		if errResp.Code == "NoSuchKey" || errResp.Code == "NotFound" {
			return true
		}
	}
	return false
}
