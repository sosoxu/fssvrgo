package database

import (
	"database/sql"
	"fmt"
)

type TransferSessionResult struct {
	SessionID     string
	SessionType   string
	Status        string
	FileID        string
	FilePath      string
	FileName      string
	FileHash      string
	FileCreatedAt string
	HashProvided  bool
	HashVerified  bool
	UploadedSize  int64
	StorageType   string
	UpdatedAt     string
	ExpiresAt     string
}

func InitTransferSessionResultTable(db *DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS transfer_session_results (
		session_id VARCHAR(36) PRIMARY KEY,
		session_type VARCHAR(32) NOT NULL,
		status VARCHAR(16) NOT NULL,
		file_id VARCHAR(36),
		file_path VARCHAR(1024),
		file_name VARCHAR(255),
		file_hash VARCHAR(64),
		file_created_at TIMESTAMP,
		hash_provided BOOLEAN NOT NULL DEFAULT FALSE,
		hash_verified BOOLEAN NOT NULL DEFAULT FALSE,
		uploaded_size BIGINT NOT NULL DEFAULT 0,
		storage_type VARCHAR(32),
		updated_at TIMESTAMP NOT NULL,
		expires_at TIMESTAMP NOT NULL
	)`); err != nil {
		return fmt.Errorf("failed to create transfer session results table: %w", err)
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_transfer_results_expiry ON transfer_session_results(expires_at)"); err != nil {
		return fmt.Errorf("failed to create transfer session result expiry index: %w", err)
	}
	return nil
}

type TransferSessionResultService struct {
	db *DB
}

func NewTransferSessionResultService(db *DB) *TransferSessionResultService {
	return &TransferSessionResultService{db: db}
}

func scanTransferSessionResult(row *sql.Row) (*TransferSessionResult, error) {
	var result TransferSessionResult
	err := row.Scan(
		&result.SessionID, &result.SessionType, &result.Status, &result.FileID,
		&result.FilePath, &result.FileName, &result.FileHash, &result.FileCreatedAt,
		&result.HashProvided, &result.HashVerified, &result.UploadedSize,
		&result.StorageType, &result.UpdatedAt, &result.ExpiresAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan transfer session result: %w", err)
	}
	return &result, nil
}

func transferSessionResultSelect() string {
	return `SELECT session_id, session_type, status, COALESCE(file_id, ''),
		COALESCE(file_path, ''), COALESCE(file_name, ''), COALESCE(file_hash, ''),
		COALESCE(file_created_at, updated_at), hash_provided, hash_verified,
		uploaded_size, COALESCE(storage_type, ''), updated_at, expires_at
		FROM transfer_session_results WHERE session_id = ? AND expires_at > CURRENT_TIMESTAMP`
}

func (s *TransferSessionResultService) Get(sessionID string) (*TransferSessionResult, error) {
	return scanTransferSessionResult(s.db.QueryRow(transferSessionResultSelect(), sessionID))
}

func (s *TransferSessionResultService) Save(result *TransferSessionResult) error {
	_, err := s.db.Exec(transferSessionResultUpsert(), transferSessionResultArgs(result)...)
	if err != nil {
		return fmt.Errorf("failed to save transfer session result: %w", err)
	}
	return nil
}

func (s *TransferSessionResultService) SaveTx(tx *Tx, result *TransferSessionResult) error {
	_, err := tx.Exec(transferSessionResultUpsert(), transferSessionResultArgs(result)...)
	if err != nil {
		return fmt.Errorf("failed to save transfer session result: %w", err)
	}
	return nil
}

func (s *TransferSessionResultService) DeleteExpired() error {
	_, err := s.db.Exec("DELETE FROM transfer_session_results WHERE expires_at <= CURRENT_TIMESTAMP")
	return err
}

func transferSessionResultUpsert() string {
	return `INSERT INTO transfer_session_results (
		session_id, session_type, status, file_id, file_path, file_name, file_hash,
		file_created_at, hash_provided, hash_verified, uploaded_size, storage_type,
		updated_at, expires_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(session_id) DO UPDATE SET
		session_type = excluded.session_type, status = excluded.status,
		file_id = excluded.file_id, file_path = excluded.file_path,
		file_name = excluded.file_name, file_hash = excluded.file_hash,
		file_created_at = excluded.file_created_at,
		hash_provided = excluded.hash_provided, hash_verified = excluded.hash_verified,
		uploaded_size = excluded.uploaded_size, storage_type = excluded.storage_type,
		updated_at = excluded.updated_at, expires_at = excluded.expires_at`
}

func transferSessionResultArgs(result *TransferSessionResult) []interface{} {
	var fileCreatedAt interface{}
	if result.FileCreatedAt != "" {
		fileCreatedAt = result.FileCreatedAt
	}
	return []interface{}{
		result.SessionID, result.SessionType, result.Status, result.FileID,
		result.FilePath, result.FileName, result.FileHash, fileCreatedAt,
		result.HashProvided, result.HashVerified, result.UploadedSize,
		result.StorageType, result.UpdatedAt, result.ExpiresAt,
	}
}
