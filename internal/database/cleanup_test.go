package database

import (
	"testing"

	"github.com/sosoxu/fssvrgo/internal/config"
	"github.com/sosoxu/fssvrgo/internal/storage"
)

func TestCleanupService_StartStop(t *testing.T) {
	db := NewDatabase()

	cfg := config.DatabaseConfig{
		Type: "sqlite",
		Path: ":memory:",
	}
	if err := db.Connect(cfg); err != nil {
		t.Skipf("Cannot connect to database: %v", err)
	}
	defer db.Close()

	store := storage.NewLocalStorage(t.TempDir())

	svc := NewCleanupService(db.GetQueryDB(), store, 1, 1)
	svc.Start()
	svc.Stop()
}
