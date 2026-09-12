package raft_test

import (
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

type nopSM struct{}

func (nopSM) Apply(raft.LogEntry) ([]byte, error) { return nil, nil }

// TestNewNodeLoadsFromMemoryStorage is the cross-package counterpart of
// TestNewNodeLoadsPersistentState: it drives NewNode with the real
// storage.MemoryStorage, which package raft's internal tests cannot import
// (storage imports raft). It lives in the external raft_test package, where
// the import is legal.
func TestNewNodeLoadsFromMemoryStorage(t *testing.T) {
	st := storage.NewMemoryStorage()
	if err := st.SaveTermVote(7, "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries([]raft.LogEntry{
		{Index: 1, Term: 2, Command: []byte("SET x 1")},
		{Index: 2, Term: 5, Command: []byte("SET y 2")},
		{Index: 3, Term: 7, Command: []byte("SET z 3")},
	}); err != nil {
		t.Fatal(err)
	}

	cfg := raft.Config{
		ID:                 "a",
		Peers:              []raft.NodeID{"b", "c"},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		Rand:               rand.New(rand.NewSource(1)),
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	n, err := raft.NewNode(cfg, st, nopSM{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	got := n.Status()
	want := raft.Status{ID: "a", Role: raft.Follower, Term: 7, VotedFor: "b", LastLogIndex: 3, LastLogTerm: 7}
	if got != want {
		t.Fatalf("Status() = %+v, want %+v", got, want)
	}

	// A "restart" over the same store must load the same state.
	n2, err := raft.NewNode(cfg, st, nopSM{})
	if err != nil {
		t.Fatalf("NewNode (restart): %v", err)
	}
	if n2.Status() != want {
		t.Fatalf("Status() after restart = %+v, want %+v", n2.Status(), want)
	}
}
