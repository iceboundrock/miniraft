package raft

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

// voteReq builds a RequestVote from candidate to voter.
func voteReq(candidate, voter NodeID, term Term, lastIndex Index, lastTerm Term) Message {
	return Message{From: candidate, To: voter, Type: MsgRequestVote, RequestVote: &RequestVote{
		Term: term, CandidateID: candidate, LastLogIndex: lastIndex, LastLogTerm: lastTerm,
	}}
}

// voteResp builds a RequestVoteResponse from voter to candidate.
func voteResp(voter, candidate NodeID, term Term, granted bool) Message {
	return Message{From: voter, To: candidate, Type: MsgRequestVoteResponse,
		RequestVoteResponse: &RequestVoteResponse{Term: term, VoteGranted: granted}}
}

// step drives n with msg, failing the test on error.
func step(t *testing.T, n *Node, msg Message) []Action {
	t.Helper()
	actions, err := n.Step(msg)
	if err != nil {
		t.Fatalf("Step(%s from %s): %v", msg.Type, msg.From, err)
	}
	return actions
}

// electionTimeout drives n's election timer, failing the test on error.
func electionTimeout(t *testing.T, n *Node) []Action {
	t.Helper()
	actions, err := n.ElectionTimeout()
	if err != nil {
		t.Fatalf("ElectionTimeout: %v", err)
	}
	return actions
}

// sends returns the messages carried by SendMessage actions, in order.
func sends(actions []Action) []Message {
	var out []Message
	for _, a := range actions {
		if s, ok := a.(SendMessage); ok {
			out = append(out, s.Message)
		}
	}
	return out
}

// countActions returns how many actions have the same concrete type as want.
func countActions(actions []Action, want Action) int {
	n := 0
	for _, a := range actions {
		if reflect.TypeOf(a) == reflect.TypeOf(want) {
			n++
		}
	}
	return n
}

// requireStatus fails unless n's role, term and vote are as given.
func requireStatus(t *testing.T, n *Node, role Role, term Term, votedFor NodeID) {
	t.Helper()
	st := n.Status()
	if st.Role != role || st.Term != term || st.VotedFor != votedFor {
		t.Fatalf("status = role=%s term=%d votedFor=%q, want role=%s term=%d votedFor=%q",
			st.Role, st.Term, st.VotedFor, role, term, votedFor)
	}
}

// requirePersisted fails unless st holds the given term and vote.
func requirePersisted(t *testing.T, st *memStorage, term Term, votedFor NodeID) {
	t.Helper()
	if st.state.CurrentTerm != term || st.state.VotedFor != votedFor {
		t.Fatalf("persisted (term=%d, votedFor=%q), want (%d, %q)", st.state.CurrentTerm, st.state.VotedFor, term, votedFor)
	}
}

// makeLeader drives a fresh 3-node candidate a to Leader in term 1 by
// granting it b's vote, and returns the node and its storage.
func makeLeader(t *testing.T) (*Node, *memStorage) {
	t.Helper()
	st := &memStorage{}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	electionTimeout(t, n)
	step(t, n, voteResp("b", "a", 1, true))
	requireStatus(t, n, Leader, 1, "a")
	return n, st
}

// TestCandidateLogUpToDate pins the §5.4.1 election restriction: the last
// term decides, and only equal last terms compare the index.
func TestCandidateLogUpToDate(t *testing.T) {
	cases := []struct {
		name       string
		candTerm   Term
		candIndex  Index
		localTerm  Term
		localIndex Index
		want       bool
	}{
		{"higher term, shorter log", 3, 1, 2, 10, true},
		{"lower term, longer log", 2, 10, 3, 1, false},
		{"equal term, longer log", 2, 5, 2, 3, true},
		{"equal term, equal length", 2, 3, 2, 3, true},
		{"equal term, shorter log", 2, 2, 2, 3, false},
		{"both empty", 0, 0, 0, 0, true},
		{"candidate empty, local not", 0, 0, 1, 1, false},
		{"local empty, candidate not", 1, 1, 0, 0, true},
	}
	for _, c := range cases {
		if got := candidateLogUpToDate(c.candTerm, c.candIndex, c.localTerm, c.localIndex); got != c.want {
			t.Errorf("%s: candidateLogUpToDate(%d,%d,%d,%d) = %v, want %v",
				c.name, c.candTerm, c.candIndex, c.localTerm, c.localIndex, got, c.want)
		}
	}
}

func TestStartArmsElectionTimer(t *testing.T) {
	cfg := testConfig("a", "b", "c")
	n := newTestNode(t, cfg, &memStorage{})
	// Repeated draws must all land in [Min, Max] and not all be equal.
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		actions := n.Start()
		if len(actions) != 1 {
			t.Fatalf("Start() = %v, want exactly one action", actions)
		}
		r, ok := actions[0].(ResetElectionTimer)
		if !ok {
			t.Fatalf("Start() = %T, want ResetElectionTimer", actions[0])
		}
		if r.Timeout < cfg.ElectionTimeoutMin || r.Timeout > cfg.ElectionTimeoutMax {
			t.Fatalf("timeout %v outside [%v, %v]", r.Timeout, cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax)
		}
		seen[r.Timeout] = true
	}
	if len(seen) < 2 {
		t.Fatal("election timeout is not randomized")
	}
	requireStatus(t, n, Follower, 0, None)
}

func TestElectionTimeoutBecomesCandidate(t *testing.T) {
	st := &memStorage{state: PersistentState{Entries: entries([2]uint64{1, 1}, [2]uint64{2, 3})}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	actions := electionTimeout(t, n)

	requireStatus(t, n, Candidate, 1, "a")
	requirePersisted(t, st, 1, "a")
	if countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("actions = %v, want one ResetElectionTimer", actions)
	}
	msgs := sends(actions)
	if len(msgs) != 2 {
		t.Fatalf("sent %d messages, want one RequestVote per peer", len(msgs))
	}
	to := map[NodeID]bool{}
	for _, m := range msgs {
		if m.Type != MsgRequestVote || m.From != "a" {
			t.Fatalf("sent %+v, want RequestVote from a", m)
		}
		rv := *m.RequestVote
		if rv != (RequestVote{Term: 1, CandidateID: "a", LastLogIndex: 2, LastLogTerm: 3}) {
			t.Fatalf("RequestVote = %+v", rv)
		}
		to[m.To] = true
	}
	if !to["b"] || !to["c"] {
		t.Fatalf("RequestVote recipients = %v, want b and c", to)
	}

	// A second timeout while still a Candidate starts a new election in a
	// new term (split-vote retry).
	electionTimeout(t, n)
	requireStatus(t, n, Candidate, 2, "a")
	requirePersisted(t, st, 2, "a")
}

func TestSingleNodeBecomesLeaderOnTimeout(t *testing.T) {
	st := &memStorage{}
	n := newTestNode(t, testConfig("a"), st)
	actions := electionTimeout(t, n)
	requireStatus(t, n, Leader, 1, "a")
	requirePersisted(t, st, 1, "a")
	if len(sends(actions)) != 0 {
		t.Fatalf("single node sent %v", sends(actions))
	}
	if countActions(actions, StopElectionTimer{}) != 1 || countActions(actions, ResetHeartbeatTimer{}) != 1 {
		t.Fatalf("actions = %v, want StopElectionTimer and ResetHeartbeatTimer", actions)
	}
}

func TestLeaderIgnoresElectionTimeout(t *testing.T) {
	n, _ := makeLeader(t)
	actions := electionTimeout(t, n)
	if len(actions) != 0 {
		t.Fatalf("Leader on ElectionTimeout returned %v, want nothing", actions)
	}
	requireStatus(t, n, Leader, 1, "a")
}

func TestGrantVote(t *testing.T) {
	st := &memStorage{}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	actions := step(t, n, voteReq("b", "a", 1, 0, 0))

	requireStatus(t, n, Follower, 1, "b")
	requirePersisted(t, st, 1, "b")
	msgs := sends(actions)
	if len(msgs) != 1 || msgs[0].To != "b" || msgs[0].Type != MsgRequestVoteResponse {
		t.Fatalf("sent %v, want one RequestVoteResponse to b", msgs)
	}
	if r := *msgs[0].RequestVoteResponse; r != (RequestVoteResponse{Term: 1, VoteGranted: true}) {
		t.Fatalf("response = %+v, want granted in term 1", r)
	}
	if countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("granting a vote must reset the election timer: %v", actions)
	}
}

func TestOneVotePerTerm(t *testing.T) {
	st := &memStorage{}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	step(t, n, voteReq("b", "a", 1, 0, 0))

	actions := step(t, n, voteReq("c", "a", 1, 0, 0))
	msgs := sends(actions)
	if len(msgs) != 1 || msgs[0].RequestVoteResponse.VoteGranted {
		t.Fatalf("second candidate in the same term got %v, want a denial", msgs)
	}
	requireStatus(t, n, Follower, 1, "b")
	requirePersisted(t, st, 1, "b")

	// The candidate we voted for may ask again (a retransmission) and gets
	// the same answer.
	actions = step(t, n, voteReq("b", "a", 1, 0, 0))
	if msgs := sends(actions); len(msgs) != 1 || !msgs[0].RequestVoteResponse.VoteGranted {
		t.Fatalf("repeated request from the voted-for candidate got %v, want a grant", msgs)
	}
	requireStatus(t, n, Follower, 1, "b")
}

func TestStaleLogCandidateDenied(t *testing.T) {
	st := &memStorage{state: PersistentState{Entries: entries([2]uint64{1, 1}, [2]uint64{2, 2})}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)

	// Lower last term loses even with a longer log.
	actions := step(t, n, voteReq("b", "a", 3, 5, 1))
	if msgs := sends(actions); len(msgs) != 1 || msgs[0].RequestVoteResponse.VoteGranted || msgs[0].RequestVoteResponse.Term != 3 {
		t.Fatalf("stale-term candidate got %v, want a denial in term 3", msgs)
	}
	// The term is adopted even though the vote is denied.
	requireStatus(t, n, Follower, 3, None)
	requirePersisted(t, st, 3, None)

	// Equal last term but shorter log loses.
	actions = step(t, n, voteReq("c", "a", 3, 1, 2))
	if msgs := sends(actions); len(msgs) != 1 || msgs[0].RequestVoteResponse.VoteGranted {
		t.Fatalf("shorter-log candidate got %v, want a denial", msgs)
	}
	requireStatus(t, n, Follower, 3, None)

	// Equal last term and equal length wins.
	actions = step(t, n, voteReq("c", "a", 3, 2, 2))
	if msgs := sends(actions); len(msgs) != 1 || !msgs[0].RequestVoteResponse.VoteGranted {
		t.Fatalf("up-to-date candidate got %v, want a grant", msgs)
	}
	requireStatus(t, n, Follower, 3, "c")
	requirePersisted(t, st, 3, "c")
}

func TestOldTermRequestVoteDenied(t *testing.T) {
	st := &memStorage{state: PersistentState{CurrentTerm: 5, VotedFor: "c"}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	actions := step(t, n, voteReq("b", "a", 3, 0, 0))
	msgs := sends(actions)
	if len(msgs) != 1 || msgs[0].RequestVoteResponse.VoteGranted || msgs[0].RequestVoteResponse.Term != 5 {
		t.Fatalf("old-term candidate got %v, want a denial carrying term 5", msgs)
	}
	requireStatus(t, n, Follower, 5, "c")
	if len(actions) != 1 {
		t.Fatalf("actions = %v, want only the response", actions)
	}
}

// TestDeniedVoteDoesNotResetTimer: a denial never produces a timer action,
// not even when the request carried a higher term that the node adopted.
func TestDeniedVoteDoesNotResetTimer(t *testing.T) {
	st := &memStorage{state: PersistentState{Entries: entries([2]uint64{1, 1})}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	step(t, n, voteReq("b", "a", 1, 1, 1)) // granted: term 1, votedFor b

	// Same term, already voted.
	actions := step(t, n, voteReq("c", "a", 1, 1, 1))
	if countActions(actions, ResetElectionTimer{}) != 0 {
		t.Fatalf("denial in the same term reset the timer: %v", actions)
	}
	// Higher term, stale log.
	actions = step(t, n, voteReq("c", "a", 2, 0, 0))
	if countActions(actions, ResetElectionTimer{}) != 0 {
		t.Fatalf("denial with a higher term reset the timer: %v", actions)
	}
	requireStatus(t, n, Follower, 2, None)
}

func TestCandidateWinsWithMajority(t *testing.T) {
	st := &memStorage{state: PersistentState{Entries: entries([2]uint64{1, 1})}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)
	electionTimeout(t, n)

	actions := step(t, n, voteResp("b", "a", 1, true))
	requireStatus(t, n, Leader, 1, "a")
	if countActions(actions, StopElectionTimer{}) != 1 || countActions(actions, ResetHeartbeatTimer{}) != 1 {
		t.Fatalf("actions = %v, want StopElectionTimer and ResetHeartbeatTimer", actions)
	}
	if countActions(actions, SendMessage{}) != 0 {
		t.Fatalf("becoming Leader must not send anything in this issue: %v", actions)
	}
	for _, p := range []NodeID{"b", "c"} {
		if n.nextIndex[p] != 2 || n.matchIndex[p] != 0 {
			t.Fatalf("peer %s: nextIndex=%d matchIndex=%d, want 2 and 0", p, n.nextIndex[p], n.matchIndex[p])
		}
	}

	// A late vote changes nothing.
	actions = step(t, n, voteResp("c", "a", 1, true))
	if len(actions) != 0 {
		t.Fatalf("late vote produced %v", actions)
	}
	requireStatus(t, n, Leader, 1, "a")
}

func TestDuplicateVoteResponseNotDoubleCounted(t *testing.T) {
	n := newTestNode(t, testConfig("a", "b", "c", "d", "e"), &memStorage{})
	electionTimeout(t, n)
	step(t, n, voteResp("b", "a", 1, true))
	step(t, n, voteResp("b", "a", 1, true))
	requireStatus(t, n, Candidate, 1, "a")
	step(t, n, voteResp("c", "a", 1, true))
	requireStatus(t, n, Leader, 1, "a")
}

func TestVoteResponsesThatDoNotCount(t *testing.T) {
	n := newTestNode(t, testConfig("a", "b", "c"), &memStorage{})
	electionTimeout(t, n)

	step(t, n, voteResp("b", "a", 1, false)) // denied
	step(t, n, voteResp("b", "a", 0, true))  // stale term
	step(t, n, voteResp("zz", "a", 1, true)) // not a peer
	requireStatus(t, n, Candidate, 1, "a")

	// A Follower ignores responses entirely.
	f := newTestNode(t, testConfig("a", "b", "c"), &memStorage{})
	if actions := step(t, f, voteResp("b", "a", 0, true)); len(actions) != 0 {
		t.Fatalf("Follower produced %v on a vote response", actions)
	}
	requireStatus(t, f, Follower, 0, None)
}

func TestHigherTermStepsDown(t *testing.T) {
	t.Run("candidate on vote response", func(t *testing.T) {
		st := &memStorage{}
		n := newTestNode(t, testConfig("a", "b", "c"), st)
		electionTimeout(t, n)
		actions := step(t, n, voteResp("b", "a", 5, false))
		requireStatus(t, n, Follower, 5, None)
		requirePersisted(t, st, 5, None)
		// The Candidate's election timer is still running; nothing to arm.
		if len(actions) != 0 {
			t.Fatalf("actions = %v, want none", actions)
		}
	})
	t.Run("leader on denied request vote", func(t *testing.T) {
		n, st := makeLeader(t)
		st.state.Entries = nil
		// Give the Leader a log so the candidate's is stale.
		n.log.append(LogEntry{Index: 1, Term: 1, Command: []byte("x")})
		actions := step(t, n, voteReq("c", "a", 2, 0, 0))
		requireStatus(t, n, Follower, 2, None)
		requirePersisted(t, st, 2, None)
		msgs := sends(actions)
		if len(msgs) != 1 || msgs[0].RequestVoteResponse.VoteGranted {
			t.Fatalf("sent %v, want a denial", msgs)
		}
		// A Leader has no election timer, so stepping down must arm one and
		// stop the heartbeat timer, even though the vote was denied.
		if countActions(actions, StopHeartbeatTimer{}) != 1 || countActions(actions, ResetElectionTimer{}) != 1 {
			t.Fatalf("actions = %v, want StopHeartbeatTimer and ResetElectionTimer", actions)
		}
	})
	t.Run("leader on vote response", func(t *testing.T) {
		n, _ := makeLeader(t)
		actions := step(t, n, voteResp("c", "a", 3, false))
		requireStatus(t, n, Follower, 3, None)
		if countActions(actions, StopHeartbeatTimer{}) != 1 || countActions(actions, ResetElectionTimer{}) != 1 {
			t.Fatalf("actions = %v, want StopHeartbeatTimer and ResetElectionTimer", actions)
		}
	})
	t.Run("candidate on append entries", func(t *testing.T) {
		n := newTestNode(t, testConfig("a", "b", "c"), &memStorage{})
		electionTimeout(t, n)
		msg := Message{From: "b", To: "a", Type: MsgAppendEntries, AppendEntries: &AppendEntries{Term: 4, LeaderID: "b"}}
		if _, err := n.Step(msg); !errors.Is(err, ErrNotImplemented) {
			t.Fatalf("AppendEntries handling: %v, want ErrNotImplemented", err)
		}
		requireStatus(t, n, Follower, 4, None)
	})
}

func TestSaveTermVoteFailureLeavesStateUnchanged(t *testing.T) {
	boom := errors.New("disk full")
	st := &memStorage{failSave: boom}
	n := newTestNode(t, testConfig("a", "b", "c"), st)

	actions, err := n.ElectionTimeout()
	if !errors.Is(err, boom) || len(actions) != 0 {
		t.Fatalf("ElectionTimeout = (%v, %v), want (nil, disk full)", actions, err)
	}
	requireStatus(t, n, Follower, 0, None)
	requirePersisted(t, st, 0, None)

	actions, err = n.Step(voteReq("b", "a", 1, 0, 0))
	if !errors.Is(err, boom) || len(actions) != 0 {
		t.Fatalf("Step = (%v, %v), want (nil, disk full)", actions, err)
	}
	requireStatus(t, n, Follower, 0, None)
	requirePersisted(t, st, 0, None)
}

// TestStepDownActionsSurviveVoteWriteFailure: a Leader that receives a
// higher-term RequestVote first steps down (persisting the term and emitting
// StopHeartbeatTimer + ResetElectionTimer) and then persists the vote grant.
// If only the second write fails, the node is durably a Follower in the new
// term, so Step must still return the step-down actions alongside the error;
// dropping them would leave the host with a stale heartbeat timer and no
// election timer. No vote response is sent because no vote was recorded.
func TestStepDownActionsSurviveVoteWriteFailure(t *testing.T) {
	boom := errors.New("disk full")
	n, st := makeLeader(t)
	st.failSave, st.failSaveAfter = boom, 1 // term write succeeds, vote write fails

	actions, err := n.Step(voteReq("c", "a", 2, 0, 0))
	if !errors.Is(err, boom) {
		t.Fatalf("Step error = %v, want disk full", err)
	}
	want := []Action{StopHeartbeatTimer{}, ResetElectionTimer{}}
	if len(actions) != len(want) ||
		countActions(actions, StopHeartbeatTimer{}) != 1 || countActions(actions, ResetElectionTimer{}) != 1 {
		t.Fatalf("actions = %v, want exactly %v", actions, want)
	}
	if msgs := sends(actions); len(msgs) != 0 {
		t.Fatalf("sent %v, want no vote response", msgs)
	}
	requireStatus(t, n, Follower, 2, None)
	requirePersisted(t, st, 2, None)
}

// TestElectionTimeoutAtMaxTermFails: currentTerm+1 would wrap to 0 at the
// maximum term, which breaks Term Monotonicity. The node must refuse to start
// the election, leaving state and storage untouched and producing no actions.
func TestElectionTimeoutAtMaxTermFails(t *testing.T) {
	st := &memStorage{state: PersistentState{CurrentTerm: math.MaxUint64}}
	n := newTestNode(t, testConfig("a", "b", "c"), st)

	actions, err := n.ElectionTimeout()
	if !errors.Is(err, ErrTermOverflow) || len(actions) != 0 {
		t.Fatalf("ElectionTimeout = (%v, %v), want (nil, ErrTermOverflow)", actions, err)
	}
	requireStatus(t, n, Follower, math.MaxUint64, None)
	requirePersisted(t, st, math.MaxUint64, None)
}

func TestBecomeFollowerRejectsLowerTerm(t *testing.T) {
	n := newTestNode(t, testConfig("a", "b", "c"), &memStorage{state: PersistentState{CurrentTerm: 3}})
	defer func() {
		if recover() == nil {
			t.Fatal("becomeFollower(2) on a term-3 node did not panic")
		}
	}()
	n.becomeFollower(2)
}
