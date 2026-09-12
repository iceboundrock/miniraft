package raft

import (
	"errors"
	"testing"
	"time"
)

func TestNewNodeLoadsPersistentState(t *testing.T) {
	st := &memStorage{state: PersistentState{
		CurrentTerm: 7,
		VotedFor:    "b",
		Entries:     entries([2]uint64{1, 2}, [2]uint64{2, 5}, [2]uint64{3, 7}),
	}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)

	got := n.Status()
	want := Status{ID: "a", Role: Follower, Term: 7, VotedFor: "b", CommitIndex: 0, LastApplied: 0, LastLogIndex: 3, LastLogTerm: 7}
	if got != want {
		t.Fatalf("Status() = %+v, want %+v", got, want)
	}

	// The node must own its copy of the log, including Command bytes.
	st.state.Entries[0].Term = 99
	st.state.Entries[0].Command[0] = 0xff
	if n.Status().LastLogIndex != 3 || n.log.termAt(1) != 2 || n.log.entryAt(1).Command[0] != 1 {
		t.Fatal("node must deep-copy loaded entries")
	}
}

func TestNewNodeFreshStorage(t *testing.T) {
	n := newTestNode(t, testConfig("a", "b", "c"), &memStorage{})
	got := n.Status()
	if got.Term != 0 || got.VotedFor != None || got.LastLogIndex != 0 || got.Role != Follower {
		t.Fatalf("fresh node Status() = %+v", got)
	}
}

func TestNewNodeRejectsNonContiguousLog(t *testing.T) {
	st := &memStorage{state: PersistentState{Entries: entries([2]uint64{1, 1}, [2]uint64{3, 1})}}
	if _, err := NewNode(testConfig("a"), st, nopSM{}); err == nil {
		t.Fatal("NewNode accepted a log with an index gap")
	}
}

func TestNewNodeValidatesConfig(t *testing.T) {
	bad := map[string]Config{
		"empty ID":       func() Config { c := testConfig("a"); c.ID = None; return c }(),
		"self peer":      testConfig("a", "a"),
		"duplicate peer": testConfig("a", "b", "b"),
		"None peer":      testConfig("a", "b", None),
		"max < min": func() Config {
			c := testConfig("a")
			c.ElectionTimeoutMax = c.ElectionTimeoutMin - time.Millisecond
			return c
		}(),
		"zero heartbeat": func() Config { c := testConfig("a"); c.HeartbeatInterval = 0; return c }(),
		"heartbeat == election min": func() Config {
			c := testConfig("a")
			c.HeartbeatInterval = c.ElectionTimeoutMin
			return c
		}(),
		"nil Rand": func() Config { c := testConfig("a"); c.Rand = nil; return c }(),
	}
	for name, cfg := range bad {
		if _, err := NewNode(cfg, &memStorage{}, nopSM{}); err == nil {
			t.Errorf("%s: NewNode accepted invalid config", name)
		}
	}
}

func TestNewNodeRejectsNilDependencies(t *testing.T) {
	if _, err := NewNode(testConfig("a"), nil, nopSM{}); err == nil {
		t.Error("NewNode accepted nil storage")
	}
	if _, err := NewNode(testConfig("a"), &memStorage{}, nil); err == nil {
		t.Error("NewNode accepted nil state machine")
	}
}

func TestProtocolEntryPointsNotImplemented(t *testing.T) {
	n := newTestNode(t, testConfig("a", "b"), &memStorage{})
	if _, err := n.ElectionTimeout(); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("ElectionTimeout: %v", err)
	}
	if _, err := n.HeartbeatTimeout(); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("HeartbeatTimeout: %v", err)
	}
	if _, err := n.Propose([]byte("x")); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Propose: %v", err)
	}
	msg := Message{From: "b", To: "a", Type: MsgRequestVote, RequestVote: &RequestVote{Term: 1, CandidateID: "b"}}
	if _, err := n.Step(msg); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Step: %v", err)
	}
	if _, err := n.Step(Message{Type: MsgRequestVote}); err == nil || errors.Is(err, ErrNotImplemented) {
		t.Errorf("Step must reject a malformed message before anything else, got %v", err)
	}
}
