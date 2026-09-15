package raft

import (
	"errors"
	"testing"
)

// propose drives n.Propose, failing the test on error.
func propose(t *testing.T, n *Node, cmd string) (Index, []Action) {
	t.Helper()
	idx, actions, err := n.Propose([]byte(cmd))
	if err != nil {
		t.Fatalf("Propose(%q): %v", cmd, err)
	}
	return idx, actions
}

// requireLog fails unless n's log (in memory and in storage) is exactly want.
func requireLog(t *testing.T, n *Node, st *memStorage, want []LogEntry) {
	t.Helper()
	got := n.log.entriesFrom(1)
	if !equalEntries(got, want) {
		t.Fatalf("log = %v, want %v", got, want)
	}
	if !equalEntries(st.state.Entries, want) {
		t.Fatalf("persisted log = %v, want %v", st.state.Entries, want)
	}
}

func equalEntries(a, b []LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || string(a[i].Command) != string(b[i].Command) {
			return false
		}
	}
	return true
}

// TestProposeAppendsAndReplicates: a Leader's Propose appends the command
// at lastIndex+1 in its own term, persists it, and sends every peer an
// AppendEntries carrying the new entry right away instead of waiting for
// the next heartbeat.
func TestProposeAppendsAndReplicates(t *testing.T) {
	n, st := makeLeader(t)
	idx, actions := propose(t, n, "SET x 1")
	if idx != 1 {
		t.Fatalf("Propose returned index %d, want 1", idx)
	}
	want := []LogEntry{{Index: 1, Term: 1, Command: []byte("SET x 1")}}
	requireLog(t, n, st, want)

	msgs := sends(actions)
	if len(msgs) != 2 {
		t.Fatalf("sent %d messages, want one per peer", len(msgs))
	}
	for i, to := range []NodeID{"b", "c"} {
		m := msgs[i]
		if m.To != to || m.Type != MsgAppendEntries || m.AppendEntries == nil {
			t.Fatalf("message %d = %+v, want AppendEntries to %s", i, m, to)
		}
		ae := m.AppendEntries
		if ae.Term != 1 || ae.PrevLogIndex != 0 || ae.PrevLogTerm != 0 || !equalEntries(ae.Entries, want) {
			t.Fatalf("AppendEntries to %s = %+v, want prev (0,0) carrying entry 1", to, *ae)
		}
	}
	if countActions(actions, ResetHeartbeatTimer{}) != 0 || countActions(actions, ResetElectionTimer{}) != 0 {
		t.Fatalf("actions = %v, want no timer actions", actions)
	}

	// A second proposal follows on at index 2 with prev (1,1) for peers that
	// have not acknowledged anything yet... except nextIndex is still 1, so
	// the Leader resends from index 1 (the follower has not confirmed it).
	idx, actions = propose(t, n, "SET y 2")
	if idx != 2 {
		t.Fatalf("second Propose returned index %d, want 2", idx)
	}
	want = append(want, LogEntry{Index: 2, Term: 1, Command: []byte("SET y 2")})
	requireLog(t, n, st, want)
	for _, m := range sends(actions) {
		ae := m.AppendEntries
		if ae.PrevLogIndex != 0 || !equalEntries(ae.Entries, want) {
			t.Fatalf("AppendEntries to %s = %+v, want prev 0 carrying entries 1..2 (nothing acked yet)", m.To, *ae)
		}
	}
}

// TestProposeCommandIsCopied: the log owns its Command bytes; a client
// mutating its buffer after Propose must not change the entry.
func TestProposeCommandIsCopied(t *testing.T) {
	n, st := makeLeader(t)
	cmd := []byte("SET x 1")
	if _, _, err := n.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	cmd[0] = 'X'
	requireLog(t, n, st, []LogEntry{{Index: 1, Term: 1, Command: []byte("SET x 1")}})
}

// TestProposeOnFollowerReturnsErrNotLeader: a non-Leader refuses the
// proposal and, when it knows one, names the Leader so the client can be
// redirected.
func TestProposeOnFollowerReturnsErrNotLeader(t *testing.T) {
	t.Run("follower with known leader", func(t *testing.T) {
		n, st := makeFollower(t)
		step(t, n, heartbeat("b", "a", 1, 0, 0))
		_, actions, err := n.Propose([]byte("SET x 1"))
		if !errors.Is(err, ErrNotLeader) || len(actions) != 0 {
			t.Fatalf("Propose = (%v, %v), want ErrNotLeader and no actions", actions, err)
		}
		var nl *NotLeaderError
		if !errors.As(err, &nl) || nl.LeaderID != "b" {
			t.Fatalf("error %v does not carry leader hint b", err)
		}
		requireLog(t, n, st, nil)
	})
	t.Run("candidate without leader", func(t *testing.T) {
		st := &memStorage{}
		n := newTestNode(t, testConfig("a", "b", "c"), st)
		electionTimeout(t, n)
		_, _, err := n.Propose([]byte("SET x 1"))
		var nl *NotLeaderError
		if !errors.As(err, &nl) || nl.LeaderID != None {
			t.Fatalf("Propose = %v, want NotLeaderError with no leader hint", err)
		}
		requireLog(t, n, st, nil)
	})
}

// TestProposeStorageFailure: the entry is persisted before it exists in
// memory; a failed append leaves the log unchanged and sends nothing.
func TestProposeStorageFailure(t *testing.T) {
	n, st := makeLeader(t)
	boom := errors.New("disk full")
	st.failAppend = boom
	_, actions, err := n.Propose([]byte("SET x 1"))
	if !errors.Is(err, boom) || len(actions) != 0 {
		t.Fatalf("Propose = (%v, %v), want the storage error and no actions", actions, err)
	}
	requireLog(t, n, st, nil)
	requireStatus(t, n, Leader, 1, "a")

	// The Leader is still usable once storage recovers.
	st.failAppend = nil
	if idx, _ := propose(t, n, "SET x 1"); idx != 1 {
		t.Fatalf("index after recovery = %d, want 1", idx)
	}
}

// TestSingleNodeProposeSendsNothing: with no peers there is nobody to
// replicate to; the entry is still appended.
func TestSingleNodeProposeSendsNothing(t *testing.T) {
	st := &memStorage{}
	n := newTestNode(t, testConfig("a"), st)
	electionTimeout(t, n)
	requireStatus(t, n, Leader, 1, "a")
	idx, actions := propose(t, n, "SET x 1")
	if idx != 1 || len(actions) != 0 {
		t.Fatalf("Propose = (%d, %v), want index 1 and no actions", idx, actions)
	}
	requireLog(t, n, st, []LogEntry{{Index: 1, Term: 1, Command: []byte("SET x 1")}})
}
