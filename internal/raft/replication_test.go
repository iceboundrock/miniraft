package raft

import (
	"errors"
	"testing"
	"time"
)

// heartbeat builds an empty AppendEntries from leader to follower.
func heartbeat(leader, follower NodeID, term Term, prevIndex Index, prevTerm Term) Message {
	return Message{From: leader, To: follower, Type: MsgAppendEntries, AppendEntries: &AppendEntries{
		Term: term, LeaderID: leader, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
	}}
}

// appendResp builds an AppendEntriesResponse from follower to leader.
func appendResp(follower, leader NodeID, term Term, success bool, match Index) Message {
	return Message{From: follower, To: leader, Type: MsgAppendEntriesResponse,
		AppendEntriesResponse: &AppendEntriesResponse{Term: term, Success: success, MatchIndex: match}}
}

// requireReply fails unless actions carry exactly one message, an
// AppendEntriesResponse to `to` with the given fields.
func requireReply(t *testing.T, actions []Action, to NodeID, term Term, success bool, match Index) {
	t.Helper()
	msgs := sends(actions)
	if len(msgs) != 1 {
		t.Fatalf("sent %v, want exactly one reply", msgs)
	}
	m := msgs[0]
	if m.To != to || m.Type != MsgAppendEntriesResponse || m.AppendEntriesResponse == nil {
		t.Fatalf("sent %+v, want AppendEntriesResponse to %s", m, to)
	}
	got := *m.AppendEntriesResponse
	want := AppendEntriesResponse{Term: term, Success: success, MatchIndex: match}
	if got != want {
		t.Fatalf("reply = %+v, want %+v", got, want)
	}
}

// makeFollower drives a fresh 3-node node a into term 1 as a Follower that
// voted for b, so that b is the legitimate Leader of a's term.
func makeFollower(t *testing.T) (*Node, *memStorage) {
	t.Helper()
	st := &memStorage{}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	step(t, n, voteReq("b", "a", 1, 0, 0))
	requireStatus(t, n, Follower, 1, "b")
	return n, st
}

func TestHeartbeatBroadcast(t *testing.T) {
	n, _ := makeLeader(t)
	n.log.append(LogEntry{Index: 1, Term: 1, Command: []byte("x")})

	actions, err := n.HeartbeatTimeout()
	if err != nil {
		t.Fatalf("HeartbeatTimeout: %v", err)
	}
	msgs := sends(actions)
	if len(msgs) != 2 {
		t.Fatalf("sent %d messages, want one per peer", len(msgs))
	}
	for i, to := range []NodeID{"b", "c"} {
		m := msgs[i]
		if m.From != "a" || m.To != to || m.Type != MsgAppendEntries || m.AppendEntries == nil {
			t.Fatalf("message %d = %+v, want AppendEntries a->%s", i, m, to)
		}
		ae := m.AppendEntries
		if ae.Term != 1 || ae.LeaderID != "a" || ae.PrevLogIndex != 1 || ae.PrevLogTerm != 1 ||
			len(ae.Entries) != 0 || ae.LeaderCommit != 0 {
			t.Fatalf("heartbeat to %s = %+v, want term 1, leader a, prev (1,1), no entries, commit 0", to, *ae)
		}
	}
	if countActions(actions, ResetHeartbeatTimer{}) != 1 || countActions(actions, ResetElectionTimer{}) != 0 {
		t.Fatalf("actions = %v, want exactly one ResetHeartbeatTimer and no election timer", actions)
	}
	for _, a := range actions {
		if r, ok := a.(ResetHeartbeatTimer); ok && r.Interval != 50*time.Millisecond {
			t.Fatalf("ResetHeartbeatTimer interval = %v, want the configured 50ms", r.Interval)
		}
	}
}

// TestHeartbeatTimeoutIgnoredByNonLeader: a heartbeat timer that fires on a
// node that is no longer Leader is stale (the host stops it on step-down)
// and must not make a Follower impersonate a Leader.
func TestHeartbeatTimeoutIgnoredByNonLeader(t *testing.T) {
	n, _ := makeFollower(t)
	actions, err := n.HeartbeatTimeout()
	if err != nil || len(actions) != 0 {
		t.Fatalf("HeartbeatTimeout on a Follower = (%v, %v), want (nil, nil)", actions, err)
	}
	requireStatus(t, n, Follower, 1, "b")
}

func TestFollowerAcceptsHeartbeat(t *testing.T) {
	n, _ := makeFollower(t)
	if st := n.Status(); st.LeaderID != None {
		t.Fatalf("LeaderID = %q before any heartbeat, want none", st.LeaderID)
	}

	actions := step(t, n, heartbeat("b", "a", 1, 0, 0))
	requireStatus(t, n, Follower, 1, "b")
	requireReply(t, actions, "b", 1, true, 0)
	if countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("actions = %v, want a ResetElectionTimer", actions)
	}
	if st := n.Status(); st.LeaderID != "b" {
		t.Fatalf("LeaderID = %q, want b", st.LeaderID)
	}

	// Every valid heartbeat resets the timer, not only the first.
	actions = step(t, n, heartbeat("b", "a", 1, 0, 0))
	if countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("second heartbeat actions = %v, want a ResetElectionTimer", actions)
	}
}

func TestCandidateStepsDownOnAppendEntries(t *testing.T) {
	st := &memStorage{}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	electionTimeout(t, n)
	requireStatus(t, n, Candidate, 1, "a")

	actions := step(t, n, heartbeat("b", "a", 1, 0, 0))
	// Same term: the vote for itself stays (Vote Safety), only the role
	// changes, and nothing needs to be re-persisted.
	requireStatus(t, n, Follower, 1, "a")
	requirePersisted(t, st, 1, "a")
	requireReply(t, actions, "b", 1, true, 0)
	if countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("actions = %v, want a ResetElectionTimer", actions)
	}
	if st := n.Status(); st.LeaderID != "b" {
		t.Fatalf("LeaderID = %q, want b", st.LeaderID)
	}
}

func TestStaleAppendEntriesRejected(t *testing.T) {
	n, _ := makeFollower(t)
	step(t, n, voteReq("c", "a", 2, 0, 0)) // a is now in term 2, voted for c
	requireStatus(t, n, Follower, 2, "c")

	actions := step(t, n, heartbeat("b", "a", 1, 0, 0))
	requireReply(t, actions, "b", 2, false, 0)
	if countActions(actions, ResetElectionTimer{}) != 0 {
		t.Fatalf("actions = %v, want no timer reset for a stale leader", actions)
	}
	if st := n.Status(); st.LeaderID != None {
		t.Fatalf("LeaderID = %q, want none: a stale leader is not recognized", st.LeaderID)
	}
}

// TestHeartbeatConsistencyCheckFails: a follower whose log does not contain
// prevLogIndex with prevLogTerm rejects the heartbeat, but the Leader itself
// is legitimate, so the timer is still reset and the leader recorded.
func TestHeartbeatConsistencyCheckFails(t *testing.T) {
	cases := []struct {
		name      string
		log       []LogEntry
		prevIndex Index
		prevTerm  Term
		success   bool
	}{
		{"empty log lacks index", nil, 2, 1, false},
		{"term mismatch at index", entries([2]uint64{1, 1}), 1, 2, false},
		{"index beyond log", entries([2]uint64{1, 1}), 2, 1, false},
		{"match", entries([2]uint64{1, 1}), 1, 1, true},
		{"sentinel always matches", entries([2]uint64{1, 1}), 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := &memStorage{state: PersistentState{Entries: c.log}}
			n := newTestNode(t, testConfig("a", "b", "c"), st)
			step(t, n, voteReq("b", "a", 1, 10, 10)) // b's log is ahead, vote granted

			actions := step(t, n, heartbeat("b", "a", 1, c.prevIndex, c.prevTerm))
			match := Index(0)
			if c.success {
				match = c.prevIndex
			}
			requireReply(t, actions, "b", 1, c.success, match)
			if countActions(actions, ResetElectionTimer{}) != 1 {
				t.Fatalf("actions = %v, want a ResetElectionTimer", actions)
			}
			if st := n.Status(); st.LeaderID != "b" {
				t.Fatalf("LeaderID = %q, want b", st.LeaderID)
			}
		})
	}
}

func TestOldLeaderStepsDownOnHigherTerm(t *testing.T) {
	t.Run("append entries", func(t *testing.T) {
		n, st := makeLeader(t)
		actions := step(t, n, heartbeat("b", "a", 2, 0, 0))
		requireStatus(t, n, Follower, 2, None)
		requirePersisted(t, st, 2, None)
		requireReply(t, actions, "b", 2, true, 0)
		// The step-down arms an election timer (a Leader had none) and the
		// heartbeat itself resets it again; two resets in one batch are fine
		// because a reset replaces the previously armed timer.
		if countActions(actions, StopHeartbeatTimer{}) != 1 || countActions(actions, ResetElectionTimer{}) < 1 {
			t.Fatalf("actions = %v, want StopHeartbeatTimer and ResetElectionTimer", actions)
		}
		if s := n.Status(); s.LeaderID != "b" {
			t.Fatalf("LeaderID = %q, want b", s.LeaderID)
		}
	})
	t.Run("append entries response", func(t *testing.T) {
		n, st := makeLeader(t)
		actions := step(t, n, appendResp("b", "a", 2, false, 0))
		requireStatus(t, n, Follower, 2, None)
		requirePersisted(t, st, 2, None)
		if len(sends(actions)) != 0 {
			t.Fatalf("sent %v, want nothing", sends(actions))
		}
		if countActions(actions, StopHeartbeatTimer{}) != 1 || countActions(actions, ResetElectionTimer{}) != 1 {
			t.Fatalf("actions = %v, want StopHeartbeatTimer and ResetElectionTimer", actions)
		}
		if s := n.Status(); s.LeaderID != None {
			t.Fatalf("LeaderID = %q, want none after stepping down into a new term", s.LeaderID)
		}
	})
}

// TestLeaderIDLifecycle: the recorded leader is the node itself while it
// leads, and is forgotten when it starts an election or learns a new term.
func TestLeaderIDLifecycle(t *testing.T) {
	n, _ := makeLeader(t)
	if s := n.Status(); s.LeaderID != "a" {
		t.Fatalf("Leader's LeaderID = %q, want itself", s.LeaderID)
	}
	step(t, n, voteResp("b", "a", 2, false)) // higher term: step down
	step(t, n, heartbeat("c", "a", 2, 0, 0))
	if s := n.Status(); s.LeaderID != "c" {
		t.Fatalf("LeaderID = %q, want c", s.LeaderID)
	}
	electionTimeout(t, n)
	if s := n.Status(); s.Role != Candidate || s.LeaderID != None {
		t.Fatalf("after starting an election role=%s LeaderID=%q, want Candidate with no leader", s.Role, s.LeaderID)
	}
}

// TestAppendEntriesResponseBookkeepingDeferred: in this issue a response
// only gets logged; it must never produce actions or errors, and responses
// that are stale or arrive at a non-Leader are ignored.
func TestAppendEntriesResponseIgnored(t *testing.T) {
	t.Run("leader, current term", func(t *testing.T) {
		n, _ := makeLeader(t)
		for _, success := range []bool{true, false} {
			actions, err := n.Step(appendResp("b", "a", 1, success, 0))
			if err != nil || len(actions) != 0 {
				t.Fatalf("Step(response success=%v) = (%v, %v), want (nil, nil)", success, actions, err)
			}
		}
		requireStatus(t, n, Leader, 1, "a")
	})
	t.Run("leader, stale term", func(t *testing.T) {
		n, _ := makeLeader(t)
		actions, err := n.Step(appendResp("b", "a", 0, true, 0))
		if err != nil || len(actions) != 0 {
			t.Fatalf("Step = (%v, %v), want (nil, nil)", actions, err)
		}
		requireStatus(t, n, Leader, 1, "a")
	})
	t.Run("follower", func(t *testing.T) {
		n, _ := makeFollower(t)
		actions, err := n.Step(appendResp("b", "a", 1, true, 0))
		if err != nil || len(actions) != 0 {
			t.Fatalf("Step = (%v, %v), want (nil, nil)", actions, err)
		}
		requireStatus(t, n, Follower, 1, "b")
	})
}

// TestLeaderRejectsSameTermAppendEntries: two Leaders in one term is an
// Election Safety violation. The node reports it and changes nothing rather
// than silently yielding to the impostor.
func TestLeaderRejectsSameTermAppendEntries(t *testing.T) {
	n, _ := makeLeader(t)
	actions, err := n.Step(heartbeat("b", "a", 1, 0, 0))
	if err == nil || errors.Is(err, ErrNotImplemented) || len(actions) != 0 {
		t.Fatalf("Step = (%v, %v), want an error and no actions", actions, err)
	}
	requireStatus(t, n, Leader, 1, "a")
	if s := n.Status(); s.LeaderID != "a" {
		t.Fatalf("LeaderID = %q, want a", s.LeaderID)
	}
}

// TestAppendEntriesWithEntriesNotImplemented: log replication is issue #7.
// The heartbeat part of the message (term, leader, timer) is still honored,
// but no reply is produced and the host is told the entries were not
// handled.
func TestAppendEntriesWithEntriesNotImplemented(t *testing.T) {
	n, _ := makeFollower(t)
	msg := heartbeat("b", "a", 1, 0, 0)
	msg.AppendEntries.Entries = entries([2]uint64{1, 1})
	actions, err := n.Step(msg)
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Step = %v, want ErrNotImplemented", err)
	}
	if len(sends(actions)) != 0 || countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("actions = %v, want only a ResetElectionTimer", actions)
	}
	if s := n.Status(); s.LeaderID != "b" || s.LastLogIndex != 0 {
		t.Fatalf("status = %+v, want leader b recognized and nothing appended", s)
	}
}
