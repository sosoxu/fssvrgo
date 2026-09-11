package storage

import "io"

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
type StorageAdapter interface {
	Write(path string, data []byte) error
	WriteFromTempFile(path string, tempFilePath string) error
	WriteFromReader(path string, reader io.Reader) error
	Read(path string) ([]byte, error)
	ReadAt(path string, size int, offset int64) ([]byte, error)
	OpenReader(path string) (io.ReadCloser, error)
	Remove(path string) error
	// Exists reports whether path exists. A non-nil error means the backend
	// could not determine existence (network failure, permission error, ...);
	// callers must not collapse that into "absent", because doing so turns an
	// infrastructure fault into a silent overwrite or a bogus "file missing".
	Exists(path string) (bool, error)
	List(directory string) ([]string, error)
	GetSize(path string) (int64, error)
	Rename(oldPath, newPath string) error
	CreateDirectory(path string) error
	RemoveDirectory(path string) error
	StorageType() string
}
