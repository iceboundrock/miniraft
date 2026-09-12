package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// ErrClosed is returned by operations on a closed store.
var ErrClosed = errors.New("storage: closed")

// MemoryStorage is an in-memory raft.Storage used by the simulator and unit
// tests. It survives a simulated node "restart" (a new raft.Node constructed
// over the same MemoryStorage) but not a process exit.
//
// The mutex protects every field; it exists so that tests may inspect a store
// from a goroutine other than the node's owner.
type MemoryStorage struct {
	mu       sync.Mutex
	term     raft.Term
	votedFor raft.NodeID
	entries  []raft.LogEntry
	closed   bool
}

// NewMemoryStorage returns an empty store.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// Load implements raft.Storage.
func (m *MemoryStorage) Load() (raft.PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return raft.PersistentState{}, ErrClosed
	}
	return raft.PersistentState{
		CurrentTerm: m.term,
		VotedFor:    m.votedFor,
		Entries:     raft.CloneEntries(m.entries),
	}, nil
}

// SaveTermVote implements raft.Storage.
func (m *MemoryStorage) SaveTermVote(term raft.Term, votedFor raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.term, m.votedFor = term, votedFor
	return nil
}

// AppendEntries implements raft.Storage.
func (m *MemoryStorage) AppendEntries(entries []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	// Validate the whole batch before touching m.entries so that a rejected
	// call leaves the store exactly as it was (all-or-nothing).
	if err := raft.ValidateEntries(entries, raft.Index(len(m.entries)+1)); err != nil {
		return fmt.Errorf("storage: append: %w", err)
	}
	m.entries = append(m.entries, raft.CloneEntries(entries)...)
	return nil
}

// TruncateSuffix implements raft.Storage.
func (m *MemoryStorage) TruncateSuffix(from raft.Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if from == 0 {
		m.entries = nil
		return nil
	}
	if from <= raft.Index(len(m.entries)) {
		m.entries = m.entries[:from-1]
	}
	return nil
}

// Close implements raft.Storage.
func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
