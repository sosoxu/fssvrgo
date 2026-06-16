package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/sosoxu/fssvrgo/internal/utils"
)

type FileMetadata struct {
	ID              string
	Path            string
	Name            string
	Size            int64
	Hash            string
	StorageType     string
	StorageLocation string
	CreatedAt       string
	UpdatedAt       string
	IsDeleted       bool
}

type DirectoryMetadata struct {
	ID        string
	Path      string
	Name      string
	CreatedAt string
	UpdatedAt string
	IsDeleted bool
}

type TransferTask struct {
	ID        string
	Type      string
	FileID    string
	ClientID  string
	Offset    int64
	TotalSize int64
	Status    string
	CreatedAt string
	UpdatedAt string
}

type AuditLog struct {
	ID             string
	Timestamp      string
	Operation      string
	ResourcePath   string
	UserIdentifier string
	ClientIP       string
	UserAgent      string
	Success        bool
	Details        string
}

type ApiKey struct {
	ID          string
	KeyHash     string
	Name        string
	Description string
	Permissions string
	CreatedAt   string
	ExpiresAt   string
	LastUsedAt  string
	IsActive    bool
}

func InitTables(db *DB) error {
	if err := initFileTable(db); err != nil {
		return err
	}
	if err := initDirectoryTable(db); err != nil {
		return err
	}
	if err := initTransferTaskTable(db); err != nil {
		return err
	}
	if err := initAuditLogTable(db); err != nil {
		return err
	}
	if err := initApiKeyTable(db); err != nil {
		return err
	}
	return nil
}

func initFileTable(db *DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS files (
		id VARCHAR(36) PRIMARY KEY,
		path VARCHAR(1024) UNIQUE NOT NULL,
		name VARCHAR(255) NOT NULL,
		size BIGINT NOT NULL DEFAULT 0,
		hash VARCHAR(64),
		storage_type VARCHAR(32) NOT NULL DEFAULT 'local',
		storage_location VARCHAR(512),
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		is_deleted BOOLEAN NOT NULL DEFAULT FALSE
	)`)
	if err != nil {
		return fmt.Errorf("failed to create files table: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
	if err != nil {
		return fmt.Errorf("failed to create files path index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_files_name ON files(name)")
	if err != nil {
		return fmt.Errorf("failed to create files name index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_files_is_deleted ON files(is_deleted)")
	if err != nil {
		return fmt.Errorf("failed to create files is_deleted index: %w", err)
	}
	return nil
}

func initDirectoryTable(db *DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS directories (
		id VARCHAR(36) PRIMARY KEY,
		path VARCHAR(1024) UNIQUE NOT NULL,
		name VARCHAR(255) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		is_deleted BOOLEAN NOT NULL DEFAULT FALSE
	)`)
	if err != nil {
		return fmt.Errorf("failed to create directories table: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_dirs_path ON directories(path)")
	if err != nil {
		return fmt.Errorf("failed to create directories path index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_dirs_is_deleted ON directories(is_deleted)")
	if err != nil {
		return fmt.Errorf("failed to create directories is_deleted index: %w", err)
	}
	return nil
}

func initTransferTaskTable(db *DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS transfer_tasks (
		id VARCHAR(36) PRIMARY KEY,
		type VARCHAR(16) NOT NULL,
		file_id VARCHAR(36),
		client_id VARCHAR(128),
		"offset" BIGINT NOT NULL DEFAULT 0,
		total_size BIGINT NOT NULL DEFAULT 0,
		status VARCHAR(16) NOT NULL DEFAULT 'pending',
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("failed to create transfer_tasks table: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_tasks_file_id ON transfer_tasks(file_id)")
	if err != nil {
		return fmt.Errorf("failed to create transfer_tasks file_id index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_tasks_client_id ON transfer_tasks(client_id)")
	if err != nil {
		return fmt.Errorf("failed to create transfer_tasks client_id index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_tasks_status ON transfer_tasks(status)")
	if err != nil {
		return fmt.Errorf("failed to create transfer_tasks status index: %w", err)
	}
	return nil
}

func initAuditLogTable(db *DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS audit_log (
		id VARCHAR(36) PRIMARY KEY,
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		operation VARCHAR(32) NOT NULL,
		resource_path VARCHAR(1024) NOT NULL,
		user_identifier VARCHAR(128),
		client_ip VARCHAR(64),
		user_agent VARCHAR(256),
		success BOOLEAN NOT NULL DEFAULT TRUE,
		details TEXT
	)`)
	if err != nil {
		return fmt.Errorf("failed to create audit_log table: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp)")
	if err != nil {
		return fmt.Errorf("failed to create audit_log timestamp index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_operation ON audit_log(operation)")
	if err != nil {
		return fmt.Errorf("failed to create audit_log operation index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_log(resource_path)")
	if err != nil {
		return fmt.Errorf("failed to create audit_log resource index: %w", err)
	}
	return nil
}

func initApiKeyTable(db *DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS api_keys (
		id VARCHAR(36) PRIMARY KEY,
		key_hash VARCHAR(256) NOT NULL UNIQUE,
		name VARCHAR(128) NOT NULL,
		description VARCHAR(512),
		permissions TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		expires_at TIMESTAMP,
		last_used_at TIMESTAMP,
		is_active BOOLEAN NOT NULL DEFAULT TRUE
	)`)
	if err != nil {
		return fmt.Errorf("failed to create api_keys table: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)")
	if err != nil {
		return fmt.Errorf("failed to create api_keys hash index: %w", err)
	}
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active)")
	if err != nil {
		return fmt.Errorf("failed to create api_keys active index: %w", err)
	}
	return nil
}

type FileMetadataService struct {
	db *DB
}

func NewFileMetadataService(db *DB) *FileMetadataService {
	return &FileMetadataService{db: db}
}

func (s *FileMetadataService) Create(m *FileMetadata) error {
	m.Path = utils.NormalizePath(m.Path)
	var existingID string
	err := s.db.QueryRow("SELECT id FROM files WHERE path = ? AND is_deleted = TRUE", m.Path).Scan(&existingID)
	if err == nil {
		_, err := s.db.Exec(`UPDATE files SET id = ?, name = ?, size = ?, hash = ?, storage_type = ?,
			storage_location = ?, created_at = ?, updated_at = ?, is_deleted = FALSE WHERE path = ?`,
			m.ID, m.Name, m.Size, m.Hash, m.StorageType, m.StorageLocation, m.CreatedAt, m.UpdatedAt, m.Path)
		if err != nil {
			return fmt.Errorf("failed to restore deleted file record: %w", err)
		}
		return nil
	}

	_, err = s.db.Exec(`INSERT INTO files (id, path, name, size, hash, storage_type, storage_location, created_at, updated_at, is_deleted)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Path, m.Name, m.Size, m.Hash, m.StorageType, m.StorageLocation, m.CreatedAt, m.UpdatedAt, m.IsDeleted)
	if err != nil {
		return fmt.Errorf("failed to create file metadata: %w", err)
	}
	return nil
}

func (s *FileMetadataService) Update(m *FileMetadata) error {
	_, err := s.db.Exec(`UPDATE files SET path = ?, name = ?, size = ?, hash = ?, storage_type = ?,
		storage_location = ?, updated_at = ?, is_deleted = ? WHERE id = ?`,
		m.Path, m.Name, m.Size, m.Hash, m.StorageType, m.StorageLocation, m.UpdatedAt, m.IsDeleted, m.ID)
	if err != nil {
		return fmt.Errorf("failed to update file metadata: %w", err)
	}
	return nil
}

func (s *FileMetadataService) Remove(id string) error {
	_, err := s.db.Exec("UPDATE files SET is_deleted = TRUE, updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("failed to remove file metadata: %w", err)
	}
	return nil
}

func (s *FileMetadataService) GetById(id string) (*FileMetadata, error) {
	row := s.db.QueryRow(`SELECT id, path, name, size, hash, storage_type, storage_location, created_at, updated_at, is_deleted
		FROM files WHERE id = ? AND is_deleted = FALSE`, id)
	return scanFileMetadata(row)
}

func (s *FileMetadataService) GetByPath(path string) (*FileMetadata, error) {
	path = utils.NormalizePath(path)
	row := s.db.QueryRow(`SELECT id, path, name, size, hash, storage_type, storage_location, created_at, updated_at, is_deleted
		FROM files WHERE path = ? AND is_deleted = FALSE`, path)
	return scanFileMetadata(row)
}

func (s *FileMetadataService) List(directoryPath string, sortBy string, sortOrder string, page int, pageSize int) ([]FileMetadata, error) {
	directoryPath = utils.NormalizePath(directoryPath)
	query := "SELECT id, path, name, size, hash, storage_type, storage_location, created_at, updated_at, is_deleted FROM files WHERE is_deleted = FALSE"
	var args []interface{}

	if directoryPath != "" {
		query += " AND path LIKE ?"
		args = append(args, directoryPath+"%")
	}

	allowedSortColumns := map[string]bool{"name": true, "path": true, "size": true, "created_at": true, "updated_at": true}
	if !allowedSortColumns[sortBy] {
		sortBy = "name"
	}
	order := "ASC"
	if strings.EqualFold(sortOrder, "desc") {
		order = "DESC"
	}
	query += fmt.Sprintf(" ORDER BY %s %s", sortBy, order)

	offset := (page - 1) * pageSize
	query += " LIMIT ? OFFSET ?"
	args = append(args, pageSize, offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}
	defer rows.Close()

	var results []FileMetadata
	for rows.Next() {
		var m FileMetadata
		err := rows.Scan(&m.ID, &m.Path, &m.Name, &m.Size, &m.Hash, &m.StorageType, &m.StorageLocation,
			&m.CreatedAt, &m.UpdatedAt, &m.IsDeleted)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file metadata: %w", err)
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating file rows: %w", err)
	}
	return results, nil
}

func (s *FileMetadataService) Exists(path string) (bool, error) {
	path = utils.NormalizePath(path)
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM files WHERE path = ? AND is_deleted = FALSE", path).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check file existence: %w", err)
	}
	return count > 0, nil
}

func scanFileMetadata(row *sql.Row) (*FileMetadata, error) {
	var m FileMetadata
	err := row.Scan(&m.ID, &m.Path, &m.Name, &m.Size, &m.Hash, &m.StorageType, &m.StorageLocation,
		&m.CreatedAt, &m.UpdatedAt, &m.IsDeleted)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan file metadata: %w", err)
	}
	return &m, nil
}

type DirectoryMetadataService struct {
	db *DB
}

func NewDirectoryMetadataService(db *DB) *DirectoryMetadataService {
	return &DirectoryMetadataService{db: db}
}

func (s *DirectoryMetadataService) Create(m *DirectoryMetadata) error {
	m.Path = utils.NormalizePath(m.Path)
	var existingID string
	err := s.db.QueryRow("SELECT id FROM directories WHERE path = ? AND is_deleted = TRUE", m.Path).Scan(&existingID)
	if err == nil {
		_, err := s.db.Exec(`UPDATE directories SET id = ?, name = ?, created_at = ?, updated_at = ?, is_deleted = FALSE WHERE path = ?`,
			m.ID, m.Name, m.CreatedAt, m.UpdatedAt, m.Path)
		if err != nil {
			return fmt.Errorf("failed to restore deleted directory record: %w", err)
		}
		return nil
	}

	_, err = s.db.Exec(`INSERT INTO directories (id, path, name, created_at, updated_at, is_deleted)
		VALUES (?, ?, ?, ?, ?, ?)`,
		m.ID, m.Path, m.Name, m.CreatedAt, m.UpdatedAt, m.IsDeleted)
	if err != nil {
		return fmt.Errorf("failed to create directory metadata: %w", err)
	}
	return nil
}

func (s *DirectoryMetadataService) Update(m *DirectoryMetadata) error {
	_, err := s.db.Exec(`UPDATE directories SET path = ?, name = ?, updated_at = ?, is_deleted = ? WHERE id = ?`,
		m.Path, m.Name, m.UpdatedAt, m.IsDeleted, m.ID)
	if err != nil {
		return fmt.Errorf("failed to update directory metadata: %w", err)
	}
	return nil
}

func (s *DirectoryMetadataService) Remove(id string) error {
	_, err := s.db.Exec("UPDATE directories SET is_deleted = TRUE, updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("failed to remove directory metadata: %w", err)
	}
	return nil
}

func (s *DirectoryMetadataService) GetById(id string) (*DirectoryMetadata, error) {
	row := s.db.QueryRow(`SELECT id, path, name, created_at, updated_at, is_deleted
		FROM directories WHERE id = ? AND is_deleted = FALSE`, id)
	return scanDirectoryMetadata(row)
}

func (s *DirectoryMetadataService) GetByPath(path string) (*DirectoryMetadata, error) {
	path = utils.NormalizePath(path)
	row := s.db.QueryRow(`SELECT id, path, name, created_at, updated_at, is_deleted
		FROM directories WHERE path = ? AND is_deleted = FALSE`, path)
	return scanDirectoryMetadata(row)
}

func (s *DirectoryMetadataService) List(parentPath string, page int, pageSize int) ([]DirectoryMetadata, error) {
	parentPath = utils.NormalizePath(parentPath)
	query := "SELECT id, path, name, created_at, updated_at, is_deleted FROM directories WHERE is_deleted = FALSE"
	var args []interface{}

	if parentPath != "" {
		query += " AND path LIKE ?"
		args = append(args, parentPath+"/%")
	}

	query += " ORDER BY name ASC LIMIT ? OFFSET ?"
	offset := (page - 1) * pageSize
	args = append(args, pageSize, offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list directories: %w", err)
	}
	defer rows.Close()

	var results []DirectoryMetadata
	for rows.Next() {
		var m DirectoryMetadata
		err := rows.Scan(&m.ID, &m.Path, &m.Name, &m.CreatedAt, &m.UpdatedAt, &m.IsDeleted)
		if err != nil {
			return nil, fmt.Errorf("failed to scan directory metadata: %w", err)
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating directory rows: %w", err)
	}
	return results, nil
}

func (s *DirectoryMetadataService) Exists(path string) (bool, error) {
	path = utils.NormalizePath(path)
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM directories WHERE path = ? AND is_deleted = FALSE", path).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check directory existence: %w", err)
	}
	return count > 0, nil
}

func scanDirectoryMetadata(row *sql.Row) (*DirectoryMetadata, error) {
	var m DirectoryMetadata
	err := row.Scan(&m.ID, &m.Path, &m.Name, &m.CreatedAt, &m.UpdatedAt, &m.IsDeleted)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory metadata: %w", err)
	}
	return &m, nil
}

type TransferTaskService struct {
	db *DB
}

func NewTransferTaskService(db *DB) *TransferTaskService {
	return &TransferTaskService{db: db}
}

func (s *TransferTaskService) Create(t *TransferTask) error {
	_, err := s.db.Exec(`INSERT INTO transfer_tasks (id, type, file_id, client_id, "offset", total_size, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.Type, t.FileID, t.ClientID, t.Offset, t.TotalSize, t.Status, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create transfer task: %w", err)
	}
	return nil
}

func (s *TransferTaskService) Update(t *TransferTask) error {
	_, err := s.db.Exec(`UPDATE transfer_tasks SET type = ?, file_id = ?, client_id = ?, "offset" = ?,
		total_size = ?, status = ?, updated_at = ? WHERE id = ?`,
		t.Type, t.FileID, t.ClientID, t.Offset, t.TotalSize, t.Status, t.UpdatedAt, t.ID)
	if err != nil {
		return fmt.Errorf("failed to update transfer task: %w", err)
	}
	return nil
}

func (s *TransferTaskService) Remove(id string) error {
	_, err := s.db.Exec("DELETE FROM transfer_tasks WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to remove transfer task: %w", err)
	}
	return nil
}

func (s *TransferTaskService) GetById(id string) (*TransferTask, error) {
	row := s.db.QueryRow(`SELECT id, type, file_id, client_id, "offset", total_size, status, created_at, updated_at
		FROM transfer_tasks WHERE id = ?`, id)
	return scanTransferTask(row)
}

func (s *TransferTaskService) ListByFileId(fileId string) ([]TransferTask, error) {
	rows, err := s.db.Query(`SELECT id, type, file_id, client_id, "offset", total_size, status, created_at, updated_at
		FROM transfer_tasks WHERE file_id = ? ORDER BY created_at DESC`, fileId)
	if err != nil {
		return nil, fmt.Errorf("failed to list transfer tasks by file id: %w", err)
	}
	defer rows.Close()

	var results []TransferTask
	for rows.Next() {
		var t TransferTask
		err := rows.Scan(&t.ID, &t.Type, &t.FileID, &t.ClientID, &t.Offset, &t.TotalSize, &t.Status, &t.CreatedAt, &t.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transfer task: %w", err)
		}
		results = append(results, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transfer task rows: %w", err)
	}
	return results, nil
}

func (s *TransferTaskService) UpdateProgress(taskId string, offset int64) error {
	_, err := s.db.Exec(`UPDATE transfer_tasks SET "offset" = ?, updated_at = ? WHERE id = ?`,
		offset, time.Now().UTC().Format(time.RFC3339), taskId)
	if err != nil {
		return fmt.Errorf("failed to update transfer task progress: %w", err)
	}
	return nil
}

func (s *TransferTaskService) CompleteTask(taskId string) error {
	_, err := s.db.Exec("UPDATE transfer_tasks SET status = 'completed', updated_at = ? WHERE id = ?",
		time.Now().UTC().Format(time.RFC3339), taskId)
	if err != nil {
		return fmt.Errorf("failed to complete transfer task: %w", err)
	}
	return nil
}

func (s *TransferTaskService) FailTask(taskId string) error {
	_, err := s.db.Exec("UPDATE transfer_tasks SET status = 'failed', updated_at = ? WHERE id = ?",
		time.Now().UTC().Format(time.RFC3339), taskId)
	if err != nil {
		return fmt.Errorf("failed to fail transfer task: %w", err)
	}
	return nil
}

func scanTransferTask(row *sql.Row) (*TransferTask, error) {
	var t TransferTask
	err := row.Scan(&t.ID, &t.Type, &t.FileID, &t.ClientID, &t.Offset, &t.TotalSize, &t.Status, &t.CreatedAt, &t.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan transfer task: %w", err)
	}
	return &t, nil
}

type AuditLogService struct {
	db *DB
}

func NewAuditLogService(db *DB) *AuditLogService {
	return &AuditLogService{db: db}
}

func (s *AuditLogService) Create(log *AuditLog) error {
	_, err := s.db.Exec(`INSERT INTO audit_log (id, timestamp, operation, resource_path, user_identifier, client_ip, user_agent, success, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		log.ID, log.Timestamp, log.Operation, log.ResourcePath, log.UserIdentifier, log.ClientIP, log.UserAgent, log.Success, log.Details)
	if err != nil {
		return fmt.Errorf("failed to create audit log: %w", err)
	}
	return nil
}

func (s *AuditLogService) GetById(id string) (*AuditLog, error) {
	row := s.db.QueryRow(`SELECT id, timestamp, operation, resource_path, user_identifier, client_ip, user_agent, success, details
		FROM audit_log WHERE id = ?`, id)
	return scanAuditLog(row)
}

func (s *AuditLogService) List(operation string, resourcePath string, page int, pageSize int) ([]AuditLog, error) {
	query := "SELECT id, timestamp, operation, resource_path, user_identifier, client_ip, user_agent, success, details FROM audit_log WHERE 1=1"
	var args []interface{}

	if operation != "" {
		query += " AND operation = ?"
		args = append(args, operation)
	}

	if resourcePath != "" {
		query += " AND resource_path LIKE ?"
		args = append(args, resourcePath+"%")
	}

	query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
	offset := (page - 1) * pageSize
	args = append(args, pageSize, offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list audit logs: %w", err)
	}
	defer rows.Close()

	var results []AuditLog
	for rows.Next() {
		var log AuditLog
		err := rows.Scan(&log.ID, &log.Timestamp, &log.Operation, &log.ResourcePath, &log.UserIdentifier,
			&log.ClientIP, &log.UserAgent, &log.Success, &log.Details)
		if err != nil {
			return nil, fmt.Errorf("failed to scan audit log: %w", err)
		}
		results = append(results, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating audit log rows: %w", err)
	}
	return results, nil
}

func (s *AuditLogService) ListByTimeRange(startTime string, endTime string, page int, pageSize int) ([]AuditLog, error) {
	query := `SELECT id, timestamp, operation, resource_path, user_identifier, client_ip, user_agent, success, details
		FROM audit_log WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
	offset := (page - 1) * pageSize
	rows, err := s.db.Query(query, startTime, endTime, pageSize, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list audit logs by time range: %w", err)
	}
	defer rows.Close()

	var results []AuditLog
	for rows.Next() {
		var log AuditLog
		err := rows.Scan(&log.ID, &log.Timestamp, &log.Operation, &log.ResourcePath, &log.UserIdentifier,
			&log.ClientIP, &log.UserAgent, &log.Success, &log.Details)
		if err != nil {
			return nil, fmt.Errorf("failed to scan audit log: %w", err)
		}
		results = append(results, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating audit log rows: %w", err)
	}
	return results, nil
}

func scanAuditLog(row *sql.Row) (*AuditLog, error) {
	var log AuditLog
	err := row.Scan(&log.ID, &log.Timestamp, &log.Operation, &log.ResourcePath, &log.UserIdentifier,
		&log.ClientIP, &log.UserAgent, &log.Success, &log.Details)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan audit log: %w", err)
	}
	return &log, nil
}

type ApiKeyService struct {
	db *DB
}

func NewApiKeyService(db *DB) *ApiKeyService {
	return &ApiKeyService{db: db}
}

func (s *ApiKeyService) Create(key *ApiKey) error {
	expiresAt := toNullString(key.ExpiresAt)
	_, err := s.db.Exec(`INSERT INTO api_keys (id, key_hash, name, description, permissions, created_at, expires_at, is_active)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		key.ID, key.KeyHash, key.Name, key.Description, key.Permissions, key.CreatedAt, expiresAt, key.IsActive)
	if err != nil {
		return fmt.Errorf("failed to create api key: %w", err)
	}
	return nil
}

func (s *ApiKeyService) Update(key *ApiKey) error {
	expiresAt := toNullString(key.ExpiresAt)
	_, err := s.db.Exec(`UPDATE api_keys SET name = ?, description = ?, permissions = ?, expires_at = ?, is_active = ? WHERE id = ?`,
		key.Name, key.Description, key.Permissions, expiresAt, key.IsActive, key.ID)
	if err != nil {
		return fmt.Errorf("failed to update api key: %w", err)
	}
	return nil
}

func (s *ApiKeyService) Remove(id string) error {
	_, err := s.db.Exec("DELETE FROM api_keys WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to remove api key: %w", err)
	}
	return nil
}

func (s *ApiKeyService) GetById(id string) (*ApiKey, error) {
	row := s.db.QueryRow(`SELECT id, key_hash, name, description, permissions, created_at, expires_at, last_used_at, is_active
		FROM api_keys WHERE id = ?`, id)
	return scanApiKey(row)
}

func (s *ApiKeyService) GetByKeyHash(keyHash string) (*ApiKey, error) {
	row := s.db.QueryRow(`SELECT id, key_hash, name, description, permissions, created_at, expires_at, last_used_at, is_active
		FROM api_keys WHERE key_hash = ? AND is_active = TRUE`, keyHash)
	key, err := scanApiKey(row)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, nil
	}
	if s.IsExpired(key) {
		return nil, nil
	}
	return key, nil
}

func (s *ApiKeyService) List(activeOnly bool, page int, pageSize int) ([]ApiKey, error) {
	query := "SELECT id, key_hash, name, description, permissions, created_at, expires_at, last_used_at, is_active FROM api_keys"
	if activeOnly {
		query += " WHERE is_active = TRUE"
	}
	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	offset := (page - 1) * pageSize
	rows, err := s.db.Query(query, pageSize, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list api keys: %w", err)
	}
	defer rows.Close()

	var results []ApiKey
	for rows.Next() {
		var key ApiKey
		err := rows.Scan(&key.ID, &key.KeyHash, &key.Name, &key.Description, &key.Permissions,
			&key.CreatedAt, &key.ExpiresAt, &key.LastUsedAt, &key.IsActive)
		if err != nil {
			return nil, fmt.Errorf("failed to scan api key: %w", err)
		}
		results = append(results, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating api key rows: %w", err)
	}
	return results, nil
}

func (s *ApiKeyService) UpdateLastUsed(id string) error {
	now := utils.GetCurrentTimestamp()
	_, err := s.db.Exec("UPDATE api_keys SET last_used_at = ? WHERE id = ?", now, id)
	if err != nil {
		return fmt.Errorf("failed to update api key last used: %w", err)
	}
	return nil
}

func (s *ApiKeyService) Deactivate(id string) error {
	_, err := s.db.Exec("UPDATE api_keys SET is_active = FALSE WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to deactivate api key: %w", err)
	}
	return nil
}

func (s *ApiKeyService) IsExpired(key *ApiKey) bool {
	if key.ExpiresAt == "" {
		return false
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return key.ExpiresAt < now
}

func scanApiKey(row *sql.Row) (*ApiKey, error) {
	var key ApiKey
	var expiresAt, lastUsedAt sql.NullString
	err := row.Scan(&key.ID, &key.KeyHash, &key.Name, &key.Description, &key.Permissions,
		&key.CreatedAt, &expiresAt, &lastUsedAt, &key.IsActive)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan api key: %w", err)
	}
	if expiresAt.Valid {
		key.ExpiresAt = expiresAt.String
	}
	if lastUsedAt.Valid {
		key.LastUsedAt = lastUsedAt.String
	}
	return &key, nil
}

var allowedSortColumns = map[string]bool{
	"name": true, "path": true, "size": true, "created_at": true, "updated_at": true,
}

func toNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
