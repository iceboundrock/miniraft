package raft

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"
)

// testConfig returns a Config with deterministic randomness and quiet logging.
func testConfig(id NodeID, peers ...NodeID) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		Rand:               rand.New(rand.NewSource(1)),
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// memStorage is a minimal in-package raft.Storage so that raft tests do not
// import internal/storage (which imports raft). It is intentionally tiny but
// honors the same contract as storage.MemoryStorage: contiguous, all-or-nothing
// appends and no shared Command bytes.
type memStorage struct {
	state PersistentState
	// failSave, when set, makes SaveTermVote fail without writing once
	// failSaveAfter more calls have succeeded (0 = fail the next call).
	failSave      error
	failSaveAfter int
}

func (m *memStorage) Load() (PersistentState, error) {
	s := m.state
	s.Entries = CloneEntries(m.state.Entries)
	return s, nil
}

func (m *memStorage) SaveTermVote(term Term, votedFor NodeID) error {
	if m.failSave != nil {
		if m.failSaveAfter == 0 {
			return m.failSave
		}
		m.failSaveAfter--
	}
	m.state.CurrentTerm, m.state.VotedFor = term, votedFor
	return nil
}

func (m *memStorage) AppendEntries(entries []LogEntry) error {
	if err := ValidateEntries(entries, Index(len(m.state.Entries)+1)); err != nil {
		return fmt.Errorf("memStorage: append: %w", err)
	}
	m.state.Entries = append(m.state.Entries, CloneEntries(entries)...)
	return nil
}

func (m *memStorage) TruncateSuffix(from Index) error {
	if from == 0 {
		m.state.Entries = nil
	} else if from <= Index(len(m.state.Entries)) {
		m.state.Entries = m.state.Entries[:from-1]
	}
	return nil
}

func (m *memStorage) Close() error { return nil }

// nopSM is a StateMachine that records nothing.
type nopSM struct{}

func (nopSM) Apply(LogEntry) ([]byte, error) { return nil, nil }

// newTestNode builds a node over the given storage, failing the test on error.
func newTestNode(t *testing.T, cfg Config, st Storage) *Node {
	t.Helper()
	n, err := NewNode(cfg, st, nopSM{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// entries builds a log from (index, term) pairs with a synthetic command.
func entries(pairs ...[2]uint64) []LogEntry {
	out := make([]LogEntry, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, LogEntry{Index: Index(p[0]), Term: Term(p[1]), Command: []byte{byte(p[0])}})
	}
	return out
}
