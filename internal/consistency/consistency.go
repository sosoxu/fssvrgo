package consistency

import (
	"fmt"
	"sync"
	"time"

	"github.com/sosoxu/fssvrgo/internal/logger"
)

type ConsistencyLevel int

const (
	LevelNone     ConsistencyLevel = iota
	LevelEventual
	LevelStrong
)

type ConsistencyManager struct {
	level         ConsistencyLevel
	replicaCount  int
	readQuorum    int
	writeQuorum   int
	syncInterval  time.Duration
	localVersion  int64
	versionMu     sync.RWMutex
	replicaStatus map[string]*ReplicaStatus
	stopCh        chan struct{}
}

type ReplicaStatus struct {
	ID        string
	Address   string
	LastSync  time.Time
	Version   int64
	Available bool
}

type SyncRequest struct {
	Operation string    `json:"operation"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
	Version   int64     `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}

type SyncResponse struct {
	Success bool  `json:"success"`
	Version int64 `json:"version"`
}

func NewConsistencyManager(level string, replicaCount, readQuorum, writeQuorum, syncIntervalMs int) *ConsistencyManager {
	cm := &ConsistencyManager{
		replicaCount:  replicaCount,
		readQuorum:    readQuorum,
		writeQuorum:   writeQuorum,
		syncInterval:  time.Duration(syncIntervalMs) * time.Millisecond,
		replicaStatus: make(map[string]*ReplicaStatus),
		stopCh:        make(chan struct{}),
	}

	switch level {
	case "strong":
		cm.level = LevelStrong
	case "eventual":
		cm.level = LevelEventual
	default:
		cm.level = LevelNone
	}

	if cm.level != LevelNone {
		go cm.syncLoop()
	}

	logger.Info("Consistency manager initialized (level=%s, replicas=%d, read_quorum=%d, write_quorum=%d)",
		level, replicaCount, readQuorum, writeQuorum)

	return cm
}

func (cm *ConsistencyManager) Stop() {
	close(cm.stopCh)
}

func (cm *ConsistencyManager) GetLevel() ConsistencyLevel {
	return cm.level
}

func (cm *ConsistencyManager) IncrementVersion() int64 {
	cm.versionMu.Lock()
	defer cm.versionMu.Unlock()
	cm.localVersion++
	return cm.localVersion
}

func (cm *ConsistencyManager) GetVersion() int64 {
	cm.versionMu.RLock()
	defer cm.versionMu.RUnlock()
	return cm.localVersion
}

// BeforeWrite checks if a write operation can proceed based on consistency level.
// Returns true if the write can proceed, false if quorum cannot be reached.
func (cm *ConsistencyManager) BeforeWrite() bool {
	switch cm.level {
	case LevelStrong:
		availableReplicas := cm.countAvailableReplicas()
		return availableReplicas+1 >= cm.writeQuorum // +1 for local
	case LevelEventual:
		return true // Always allow writes in eventual consistency
	default:
		return true
	}
}

// AfterWrite handles post-write consistency operations.
func (cm *ConsistencyManager) AfterWrite(key string, value []byte) {
	version := cm.IncrementVersion()

	switch cm.level {
	case LevelStrong:
		// Synchronously replicate to quorum
		cm.syncWrite(key, value, version)
	case LevelEventual:
		// Queue for async replication
		go cm.asyncReplicate(key, value, version)
	default:
		// No replication
	}
}

// BeforeRead checks if a read operation can proceed based on consistency level.
func (cm *ConsistencyManager) BeforeRead() bool {
	switch cm.level {
	case LevelStrong:
		availableReplicas := cm.countAvailableReplicas()
		return availableReplicas+1 >= cm.readQuorum
	default:
		return true
	}
}

func (cm *ConsistencyManager) RegisterReplica(id, address string) {
	cm.replicaStatus[id] = &ReplicaStatus{
		ID:        id,
		Address:   address,
		Version:   0,
		Available: true,
	}
	logger.Info("Registered replica: %s (%s)", id, address)
}

func (cm *ConsistencyManager) countAvailableReplicas() int {
	count := 0
	for _, r := range cm.replicaStatus {
		if r.Available {
			count++
		}
	}
	return count
}

func (cm *ConsistencyManager) syncLoop() {
	if cm.syncInterval == 0 {
		cm.syncInterval = 5 * time.Second
	}

	ticker := time.NewTicker(cm.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.checkReplicaHealth()
		case <-cm.stopCh:
			return
		}
	}
}

func (cm *ConsistencyManager) checkReplicaHealth() {
	for id, replica := range cm.replicaStatus {
		if time.Since(replica.LastSync) > cm.syncInterval*3 {
			if replica.Available {
				replica.Available = false
				logger.Warn("Replica %s marked as unavailable", id)
			}
		}
	}
}

func (cm *ConsistencyManager) syncWrite(key string, value []byte, version int64) {
	successCount := 1 // Local write
	for id, replica := range cm.replicaStatus {
		if !replica.Available {
			continue
		}

		// In a real implementation, this would make an HTTP/gRPC call to the replica
		// For now, we log the sync operation
		logger.Debug("Syncing write to replica %s: key=%s, version=%d", id, key, version)
		successCount++

		replica.LastSync = time.Now()
		replica.Version = version
	}

	if successCount < cm.writeQuorum {
		logger.Warn("Write quorum not reached: %d/%d replicas confirmed", successCount, cm.writeQuorum)
	}
}

func (cm *ConsistencyManager) asyncReplicate(key string, value []byte, version int64) {
	for id, replica := range cm.replicaStatus {
		if !replica.Available {
			continue
		}

		// In a real implementation, this would make an async HTTP/gRPC call
		logger.Debug("Async replicating to replica %s: key=%s, version=%d", id, key, version)
		replica.LastSync = time.Now()
		replica.Version = version
	}
}

func (cm *ConsistencyManager) GetReplicaStatus() []ReplicaStatus {
	var statuses []ReplicaStatus
	for _, r := range cm.replicaStatus {
		statuses = append(statuses, *r)
	}
	return statuses
}

func (cm *ConsistencyManager) HandleSyncRequest(req *SyncRequest) (*SyncResponse, error) {
	cm.versionMu.Lock()
	if req.Version > cm.localVersion {
		cm.localVersion = req.Version
	}
	cm.versionMu.Unlock()

	return &SyncResponse{
		Success: true,
		Version: cm.localVersion,
	}, nil
}

func (cm *ConsistencyManager) AddSyncEndpoint(registerFunc func(pattern string, handler func(*SyncRequest) *SyncResponse)) {
	if cm.level == LevelNone {
		return
	}
	registerFunc("/sync", func(req *SyncRequest) *SyncResponse {
		resp, err := cm.HandleSyncRequest(req)
		if err != nil {
			return &SyncResponse{Success: false}
		}
		return resp
	})
	logger.Info("Sync endpoint registered for consistency level: %d", cm.level)
}

// ValidateQuorum validates that quorum settings are correct
func ValidateQuorum(replicaCount, readQuorum, writeQuorum int) error {
	if replicaCount > 0 {
		if readQuorum <= 0 || readQuorum > replicaCount+1 {
			return fmt.Errorf("read_quorum must be between 1 and %d", replicaCount+1)
		}
		if writeQuorum <= 0 || writeQuorum > replicaCount+1 {
			return fmt.Errorf("write_quorum must be between 1 and %d", replicaCount+1)
		}
		// Ensure read + write quorum overlap (for strong consistency)
		if readQuorum+writeQuorum <= replicaCount+1 {
			return fmt.Errorf("read_quorum + write_quorum must be > replica_count+1 for strong consistency (got %d + %d <= %d)",
				readQuorum, writeQuorum, replicaCount+1)
		}
	}
	return nil
}
