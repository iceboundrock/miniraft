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

// TestHeartbeatBroadcast: a heartbeat is the same AppendEntries replication
// uses, built per peer from nextIndex. A peer that has acknowledged the
// whole log gets an empty message with prev = last entry; a peer that has
// not gets the missing suffix.
func TestHeartbeatBroadcast(t *testing.T) {
	n, _ := makeLeader(t)
	propose(t, n, "x")
	step(t, n, appendResp("b", "a", 1, true, 1)) // b has entry 1, c has not acknowledged

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
	}
	b, c := msgs[0].AppendEntries, msgs[1].AppendEntries
	if b.Term != 1 || b.LeaderID != "a" || b.PrevLogIndex != 1 || b.PrevLogTerm != 1 || len(b.Entries) != 0 || b.LeaderCommit != 0 {
		t.Fatalf("heartbeat to b = %+v, want term 1, leader a, prev (1,1), no entries, commit 0", *b)
	}
	if c.PrevLogIndex != 0 || c.PrevLogTerm != 0 || len(c.Entries) != 1 || c.Entries[0].Index != 1 {
		t.Fatalf("heartbeat to c = %+v, want prev (0,0) carrying entry 1", *c)
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

// TestAppendEntriesResponseAdvancesMatchIndex: a successful reply moves
// matchIndex to the follower's MatchIndex and nextIndex just past it; a
// stale or duplicated reply never moves matchIndex backwards.
func TestAppendEntriesResponseAdvancesMatchIndex(t *testing.T) {
	n, _ := makeLeader(t)
	propose(t, n, "x")
	propose(t, n, "y")
	propose(t, n, "z")

	actions := step(t, n, appendResp("b", "a", 1, true, 3))
	if len(actions) != 0 {
		t.Fatalf("actions after a success = %v, want none (nothing left to send)", actions)
	}
	if n.matchIndex["b"] != 3 || n.nextIndex["b"] != 4 {
		t.Fatalf("b: match=%d next=%d, want 3/4", n.matchIndex["b"], n.nextIndex["b"])
	}
	if n.matchIndex["c"] != 0 || n.nextIndex["c"] != 1 {
		t.Fatalf("c: match=%d next=%d, want untouched 0/1", n.matchIndex["c"], n.nextIndex["c"])
	}

	// A delayed success for an older prefix arrives after: matchIndex stays.
	step(t, n, appendResp("b", "a", 1, true, 1))
	if n.matchIndex["b"] != 3 || n.nextIndex["b"] != 4 {
		t.Fatalf("b after a stale success: match=%d next=%d, want 3/4", n.matchIndex["b"], n.nextIndex["b"])
	}
	// A duplicate of the current one is a no-op too.
	step(t, n, appendResp("b", "a", 1, true, 3))
	if n.matchIndex["b"] != 3 || n.nextIndex["b"] != 4 {
		t.Fatalf("b after a duplicate success: match=%d next=%d, want 3/4", n.matchIndex["b"], n.nextIndex["b"])
	}
}

// TestAppendEntriesResponseRejectsImpossibleMatch: a follower cannot match
// entries the Leader does not have. Such a reply is a protocol violation
// and is reported instead of being trusted (it would let the commit rule
// count an index that does not exist).
func TestAppendEntriesResponseRejectsImpossibleMatch(t *testing.T) {
	n, _ := makeLeader(t)
	propose(t, n, "x")
	actions, err := n.Step(appendResp("b", "a", 1, true, 2))
	if err == nil || len(actions) != 0 {
		t.Fatalf("Step = (%v, %v), want an error and no actions", actions, err)
	}
	if n.matchIndex["b"] != 0 || n.nextIndex["b"] != 1 {
		t.Fatalf("b: match=%d next=%d, want untouched 0/1", n.matchIndex["b"], n.nextIndex["b"])
	}
}

// TestNextIndexRollback: each rejection in the current term walks
// nextIndex back by one and resends at once; the resend's prevIndex is the
// new nextIndex-1 and carries everything from nextIndex on. It never walks
// below matchIndex+1, which is also what bounds it at 1.
func TestNextIndexRollback(t *testing.T) {
	n, _ := makeLeader(t)
	for _, c := range []string{"v", "w", "x", "y", "z"} {
		propose(t, n, c)
	}
	n.nextIndex["b"] = 6 // as if b had been caught up before it diverged
	for want := Index(4); ; want-- {
		actions := step(t, n, appendResp("b", "a", 1, false, 0))
		msgs := sends(actions)
		if len(msgs) != 1 || msgs[0].To != "b" || msgs[0].AppendEntries == nil {
			t.Fatalf("after rejection: sent %v, want one AppendEntries to b", msgs)
		}
		ae := msgs[0].AppendEntries
		if ae.PrevLogIndex != want || ae.PrevLogTerm != n.log.termAt(want) || Index(len(ae.Entries)) != 5-want {
			t.Fatalf("resend = %+v, want prevIndex %d carrying %d entries", *ae, want, 5-want)
		}
		if n.nextIndex["b"] != want+1 {
			t.Fatalf("nextIndex[b] = %d, want %d", n.nextIndex["b"], want+1)
		}
		if want == 0 {
			break
		}
	}
	// Rejections at nextIndex 1 cannot go lower.
	actions := step(t, n, appendResp("b", "a", 1, false, 0))
	if n.nextIndex["b"] != 1 || len(sends(actions)) != 1 || sends(actions)[0].AppendEntries.PrevLogIndex != 0 {
		t.Fatalf("nextIndex[b] = %d after rejection at 1, want 1 and a resend from index 1", n.nextIndex["b"])
	}
}

// TestNextIndexRollbackStopsAtMatchIndex: a rejection that is stale (the
// follower has since acknowledged a higher prefix) must not move nextIndex
// below what is known to match.
func TestNextIndexRollbackStopsAtMatchIndex(t *testing.T) {
	n, _ := makeLeader(t)
	propose(t, n, "x")
	propose(t, n, "y")
	step(t, n, appendResp("b", "a", 1, true, 2))
	step(t, n, appendResp("b", "a", 1, false, 0)) // late rejection of an earlier probe
	if n.matchIndex["b"] != 2 || n.nextIndex["b"] != 3 {
		t.Fatalf("b: match=%d next=%d, want 2/3 (a stale rejection must not roll back past matchIndex)",
			n.matchIndex["b"], n.nextIndex["b"])
	}
}

// TestAppendEntriesResponseIgnored: responses that are stale or arrive at a
// non-Leader belong to another leadership and change nothing.
func TestAppendEntriesResponseIgnored(t *testing.T) {
	t.Run("leader, stale term", func(t *testing.T) {
		n, _ := makeLeader(t)
		propose(t, n, "x")
		actions, err := n.Step(appendResp("b", "a", 0, true, 1))
		if err != nil || len(actions) != 0 || n.matchIndex["b"] != 0 {
			t.Fatalf("Step = (%v, %v) match=%d, want (nil, nil) and match 0", actions, err, n.matchIndex["b"])
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
	if err == nil || len(actions) != 0 {
		t.Fatalf("Step = (%v, %v), want an error and no actions", actions, err)
	}
	requireStatus(t, n, Leader, 1, "a")
	if s := n.Status(); s.LeaderID != "a" {
		t.Fatalf("LeaderID = %q, want a", s.LeaderID)
	}
}

// appendMsg builds an AppendEntries from leader to follower carrying entries.
func appendMsg(leader, follower NodeID, term Term, prevIndex Index, prevTerm Term, entries []LogEntry) Message {
	m := heartbeat(leader, follower, term, prevIndex, prevTerm)
	m.AppendEntries.Entries = entries
	return m
}

// followerWithLog builds a term-1 Follower of b over a pre-populated log.
// The vote request claims a far-ahead log so it is always granted.
func followerWithLog(t *testing.T, log []LogEntry) (*Node, *memStorage) {
	t.Helper()
	st := &memStorage{state: PersistentState{Entries: log}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	step(t, n, voteReq("b", "a", 1, 100, 100))
	requireStatus(t, n, Follower, 1, "b")
	return n, st
}

// TestFollowerAppendsEntries: entries that follow the follower's log are
// persisted, then acknowledged with MatchIndex = prevLogIndex + len(entries).
func TestFollowerAppendsEntries(t *testing.T) {
	n, st := followerWithLog(t, nil)
	e := entries([2]uint64{1, 1}, [2]uint64{2, 1})
	actions := step(t, n, appendMsg("b", "a", 1, 0, 0, e))
	requireReply(t, actions, "b", 1, true, 2)
	if countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("actions = %v, want a ResetElectionTimer", actions)
	}
	requireLog(t, n, st, e)

	more := entries([2]uint64{3, 1})
	actions = step(t, n, appendMsg("b", "a", 1, 2, 1, more))
	requireReply(t, actions, "b", 1, true, 3)
	requireLog(t, n, st, append(e, more...))
}

// TestFollowerRejectsEntriesOnMismatch: the consistency check gates the
// whole batch; on mismatch nothing is appended or truncated and the reply
// is Success=false, while the Leader is still recognized.
func TestFollowerRejectsEntriesOnMismatch(t *testing.T) {
	have := entries([2]uint64{1, 1}, [2]uint64{2, 1})
	cases := []struct {
		name      string
		prevIndex Index
		prevTerm  Term
	}{
		{"prevLogIndex beyond log", 3, 1},
		{"prevLogTerm differs", 2, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, st := followerWithLog(t, have)
			actions := step(t, n, appendMsg("b", "a", 1, c.prevIndex, c.prevTerm, entries([2]uint64{uint64(c.prevIndex) + 1, 2})))
			requireReply(t, actions, "b", 1, false, 0)
			if countActions(actions, ResetElectionTimer{}) != 1 {
				t.Fatalf("actions = %v, want a ResetElectionTimer", actions)
			}
			requireLog(t, n, st, have)
			if s := n.Status(); s.LeaderID != "b" {
				t.Fatalf("LeaderID = %q, want b", s.LeaderID)
			}
		})
	}
}

// TestDivergentSuffixRepair: an existing entry whose term differs from the
// Leader's entry at the same index is deleted together with everything
// after it, and the Leader's suffix takes its place (Figure 2, receiver
// step 3). Truncation happens in storage as well as in memory.
func TestDivergentSuffixRepair(t *testing.T) {
	have := entries([2]uint64{1, 1}, [2]uint64{2, 2}, [2]uint64{3, 2}, [2]uint64{4, 2})
	n, st := followerWithLog(t, have)
	// Leader's log: (1,1) (2,3) (3,3); prev = (1,1).
	incoming := entries([2]uint64{2, 3}, [2]uint64{3, 3})
	actions := step(t, n, appendMsg("b", "a", 3, 1, 1, incoming))
	requireReply(t, actions, "b", 3, true, 3)
	requireLog(t, n, st, append(entries([2]uint64{1, 1}), incoming...))
}

// TestDuplicateAppendEntriesIsIdempotent: entries already present with the
// same term are skipped, not re-appended, and — crucially — an older
// duplicate or reordered AppendEntries must not truncate entries that a
// newer one already appended. Only a term conflict deletes anything.
func TestDuplicateAppendEntriesIsIdempotent(t *testing.T) {
	n, st := followerWithLog(t, nil)
	first := entries([2]uint64{1, 1}, [2]uint64{2, 1})
	second := entries([2]uint64{3, 1})
	step(t, n, appendMsg("b", "a", 1, 0, 0, first))
	step(t, n, appendMsg("b", "a", 1, 2, 1, second))
	full := append(append([]LogEntry(nil), first...), second...)
	requireLog(t, n, st, full)

	t.Run("exact duplicate of an older batch", func(t *testing.T) {
		actions := step(t, n, appendMsg("b", "a", 1, 0, 0, first))
		requireReply(t, actions, "b", 1, true, 2)
		requireLog(t, n, st, full)
	})
	t.Run("reordered shorter batch", func(t *testing.T) {
		actions := step(t, n, appendMsg("b", "a", 1, 0, 0, first[:1]))
		requireReply(t, actions, "b", 1, true, 1)
		requireLog(t, n, st, full)
	})
	t.Run("overlapping batch with a new tail", func(t *testing.T) {
		batch := entries([2]uint64{2, 1}, [2]uint64{3, 1}, [2]uint64{4, 1})
		actions := step(t, n, appendMsg("b", "a", 1, 1, 1, batch))
		requireReply(t, actions, "b", 1, true, 4)
		requireLog(t, n, st, append(full, batch[2]))
	})
	t.Run("heartbeat against a longer log", func(t *testing.T) {
		actions := step(t, n, heartbeat("b", "a", 1, 2, 1))
		requireReply(t, actions, "b", 1, true, 2)
		requireLog(t, n, st, append(full, entries([2]uint64{4, 1})...))
	})
}

// TestFollowerStorageFailureOnAppend: the reply is produced only after the
// entries are durable. A failed write returns the error and the heartbeat
// part's timer reset, no reply, and the in-memory log matches storage.
func TestFollowerStorageFailureOnAppend(t *testing.T) {
	t.Run("append fails", func(t *testing.T) {
		n, st := followerWithLog(t, nil)
		boom := errors.New("disk full")
		st.failAppend = boom
		actions, err := n.Step(appendMsg("b", "a", 1, 0, 0, entries([2]uint64{1, 1})))
		if !errors.Is(err, boom) {
			t.Fatalf("Step = %v, want the storage error", err)
		}
		if len(sends(actions)) != 0 || countActions(actions, ResetElectionTimer{}) != 1 {
			t.Fatalf("actions = %v, want only a ResetElectionTimer", actions)
		}
		requireLog(t, n, st, nil)
	})
	t.Run("truncate fails", func(t *testing.T) {
		have := entries([2]uint64{1, 1}, [2]uint64{2, 1})
		n, st := followerWithLog(t, have)
		boom := errors.New("disk full")
		st.failTruncate = boom
		actions, err := n.Step(appendMsg("b", "a", 2, 1, 1, entries([2]uint64{2, 2})))
		if !errors.Is(err, boom) {
			t.Fatalf("Step = %v, want the storage error", err)
		}
		if len(sends(actions)) != 0 {
			t.Fatalf("actions = %v, want no reply", actions)
		}
		requireLog(t, n, st, have)
	})
	t.Run("append after truncate fails", func(t *testing.T) {
		have := entries([2]uint64{1, 1}, [2]uint64{2, 1})
		n, st := followerWithLog(t, have)
		boom := errors.New("disk full")
		st.failAppend = boom
		_, err := n.Step(appendMsg("b", "a", 2, 1, 1, entries([2]uint64{2, 2})))
		if !errors.Is(err, boom) {
			t.Fatalf("Step = %v, want the storage error", err)
		}
		// The truncation is durable, so memory must reflect it too.
		requireLog(t, n, st, have[:1])
	})
}

// TestLeaderNeverTruncatesOwnLog (Leader Append-Only, §5.3): the only path
// that deletes entries is the follower's conflict repair, and a Leader
// never takes it — a same-term AppendEntries is an Election Safety error
// handled before any log change, and the truncation helper itself refuses
// to run on a Leader.
func TestLeaderNeverTruncatesOwnLog(t *testing.T) {
	n, st := makeLeader(t)
	propose(t, n, "x")
	propose(t, n, "y")
	want := n.log.entriesFrom(1)

	_, err := n.Step(appendMsg("b", "a", 1, 0, 0, entries([2]uint64{1, 1}, [2]uint64{2, 2})))
	if err == nil {
		t.Fatal("Leader accepted a same-term AppendEntries")
	}
	requireLog(t, n, st, want)

	defer func() {
		if recover() == nil {
			t.Fatal("truncateSuffix on a Leader did not panic")
		}
		requireLog(t, n, st, want)
	}()
	_ = n.truncateSuffix(2)
}
