package consistency

import (
	"testing"
)

func TestNewConsistencyManager(t *testing.T) {
	cm := NewConsistencyManager("strong", 3, 3, 3, 1000)
	if cm.GetLevel() != LevelStrong {
		t.Errorf("Expected LevelStrong, got %d", cm.GetLevel())
	}
	cm.Stop()

	cm2 := NewConsistencyManager("eventual", 2, 2, 2, 1000)
	if cm2.GetLevel() != LevelEventual {
		t.Errorf("Expected LevelEventual, got %d", cm2.GetLevel())
	}
	cm2.Stop()

	cm3 := NewConsistencyManager("none", 0, 0, 0, 0)
	if cm3.GetLevel() != LevelNone {
		t.Errorf("Expected LevelNone, got %d", cm3.GetLevel())
	}
	cm3.Stop()
}

func TestValidateQuorum(t *testing.T) {
	// Valid: read + write > replica + 1
	err := ValidateQuorum(3, 3, 3)
	if err != nil {
		t.Errorf("Expected no error for valid quorum, got %v", err)
	}

	// Invalid: read + write <= replica + 1
	err = ValidateQuorum(3, 2, 2)
	if err == nil {
		t.Error("Expected error for invalid quorum (read+write <= replica+1)")
	}

	// Invalid: read quorum out of range
	err = ValidateQuorum(3, 5, 3)
	if err == nil {
		t.Error("Expected error for read_quorum > replica+1")
	}
}

func TestConsistencyManager_VersionManagement(t *testing.T) {
	cm := NewConsistencyManager("eventual", 2, 2, 2, 60000)
	defer cm.Stop()

	v1 := cm.GetVersion()
	if v1 != 0 {
		t.Errorf("Expected initial version 0, got %d", v1)
	}

	v2 := cm.IncrementVersion()
	if v2 != 1 {
		t.Errorf("Expected version 1, got %d", v2)
	}

	v3 := cm.IncrementVersion()
	if v3 != 2 {
		t.Errorf("Expected version 2, got %d", v3)
	}
}

func TestConsistencyManager_ReplicaManagement(t *testing.T) {
	cm := NewConsistencyManager("strong", 2, 2, 2, 60000)
	defer cm.Stop()

	cm.RegisterReplica("replica1", "localhost:9001")
	cm.RegisterReplica("replica2", "localhost:9002")

	statuses := cm.GetReplicaStatus()
	if len(statuses) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(statuses))
	}
}
