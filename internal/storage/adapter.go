package storage

import (
	"context"
	"io"
)

// StorageAdapter is the seam between the service layer and a storage backend.
//
// The contract is intentionally narrow and backend-agnostic: operations on
// whole objects addressed by a slash-separated path, plus streaming reads.
//
// # What is deliberately NOT in this interface
//
//   - Random-access writes (WriteAt). In-place random writes are a
//     local-filesystem capability; object storage cannot provide them without
//     a read-modify-write of the whole object, which is neither atomic nor
//     memory-safe for large files. Callers that need large-file assembly write
//     to a local temp file and then hand the finished file to
//     WriteFromTempFile. WriteAt remains available on *LocalStorage as an
//     implementation detail for tests.
//   - Path validation (ValidatePath). Escaping the storage root must be
//     rejected by every operation, so it is an invariant of this interface
//     rather than a method callers are expected to make. Backends implement it
//     internally (LocalStorage.validatePath / MinIOStorage.validatePath).
//
// # Implementing the interface
//
// Every operation must reject paths that resolve outside the storage root.
// Backends that can enumerate their contents additionally implement
// ObjectLister (see internal/database/reconcile.go) so metadata/storage
// reconciliation can detect drift.
//
// Every method takes a context as its first parameter. Object-storage backends
// pass it to the SDK so a cancelled request stops an in-flight upload or
// download; local backends accept it for signature uniformity (their
// operations are non-blocking local syscalls).
type StorageAdapter interface {
	Write(ctx context.Context, path string, data []byte) error
	WriteFromTempFile(ctx context.Context, path string, tempFilePath string) error
	WriteFromReader(ctx context.Context, path string, reader io.Reader) error
	Read(ctx context.Context, path string) ([]byte, error)
	ReadAt(ctx context.Context, path string, size int, offset int64) ([]byte, error)
	OpenReader(ctx context.Context, path string) (io.ReadCloser, error)
	Remove(ctx context.Context, path string) error
	// Exists reports whether path exists. A non-nil error means the backend
	// could not determine existence (network failure, permission error, ...);
	// callers must not collapse that into "absent", because doing so turns an
	// infrastructure fault into a silent overwrite or a bogus "file missing".
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, directory string) ([]string, error)
	GetSize(ctx context.Context, path string) (int64, error)
	Rename(ctx context.Context, oldPath, newPath string) error
	CreateDirectory(ctx context.Context, path string) error
	RemoveDirectory(ctx context.Context, path string) error
	StorageType() string
}
