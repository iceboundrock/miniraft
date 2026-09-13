package storage

import (
	"testing"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage/storagetest"
)

func TestStorageConformance(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		storagetest.Run(t, func(t *testing.T) raft.Storage { return NewMemoryStorage() })
	})
	t.Run("File", func(t *testing.T) {
		storagetest.Run(t, func(t *testing.T) raft.Storage { return mustOpen(t, t.TempDir()) })
	})
}
