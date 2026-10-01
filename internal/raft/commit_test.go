package raft

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// Commit and apply tests (issue #8): the Leader's majority commit rule with
// the current-term restriction (§5.3, §5.4.2 / Figure 8), the follower's
// LeaderCommit rule, and strict in-order apply.

// recordingSM records every entry applied to it, in order.
type recordingSM struct {
	applied []LogEntry
	// fail makes Apply of these indexes return the error without recording
	// (and, per the StateMachine contract, without changing state).
	fail map[Index]error
	// st, when set, is checked on every Apply: an entry must already be in
	// storage when it is applied. notPersisted collects the ones that were not.
	st           *memStorage
	notPersisted []Index
}

func (r *recordingSM) Apply(e LogEntry) ([]byte, error) {
	if err := r.fail[e.Index]; err != nil {
		return nil, err
	}
	if r.st != nil && e.Index > Index(len(r.st.state.Entries)) {
		r.notPersisted = append(r.notPersisted, e.Index)
	}
	r.applied = append(r.applied, e)
	return append([]byte("ok:"), e.Command...), nil
}

// appliedIndexes returns the indexes the machine applied, in order.
func (r *recordingSM) appliedIndexes() []Index {
	var out []Index
	for _, e := range r.applied {
		out = append(out, e.Index)
	}
	return out
}

// applieds returns the Applied actions, in order.
func applieds(actions []Action) []Applied {
	var out []Applied
	for _, a := range actions {
		if ap, ok := a.(Applied); ok {
			out = append(out, ap)
		}
	}
	return out
}

// requireApplied fails unless actions carry exactly the Applied actions for
// the given (index, term) pairs, in order.
func requireApplied(t *testing.T, actions []Action, pairs ...[2]uint64) {
	t.Helper()
	got := applieds(actions)
	if len(got) != len(pairs) {
		t.Fatalf("Applied actions = %+v, want %d of them", got, len(pairs))
	}
	for i, p := range pairs {
		if got[i].Index != Index(p[0]) || got[i].Term != Term(p[1]) {
			t.Fatalf("Applied[%d] = %+v, want index %d term %d", i, got[i], p[0], p[1])
		}
	}
}

// requireCommit fails unless n's commitIndex and lastApplied are as given.
func requireCommit(t *testing.T, n *Node, commitIndex, lastApplied Index) {
	t.Helper()
	st := n.Status()
	if st.CommitIndex != commitIndex || st.LastApplied != lastApplied {
		t.Fatalf("commitIndex=%d lastApplied=%d, want %d and %d", st.CommitIndex, st.LastApplied, commitIndex, lastApplied)
	}
	if st.CommitIndex > st.LastLogIndex {
		t.Fatalf("commitIndex %d is beyond the last log index %d", st.CommitIndex, st.LastLogIndex)
	}
}

// heartbeatTimeout drives n's heartbeat timer, failing the test on error.
func heartbeatTimeout(t *testing.T, n *Node) []Action {
	t.Helper()
	actions, err := n.HeartbeatTimeout()
	if err != nil {
		t.Fatalf("HeartbeatTimeout: %v", err)
	}
	return actions
}

// requireLeaderCommit fails unless every AppendEntries in actions carries
// LeaderCommit == want, and there is at least one.
func requireLeaderCommit(t *testing.T, actions []Action, want Index) {
	t.Helper()
	msgs := sends(actions)
	if len(msgs) == 0 {
		t.Fatal("no AppendEntries sent")
	}
	for _, m := range msgs {
		if m.AppendEntries == nil || m.AppendEntries.LeaderCommit != want {
			t.Fatalf("sent %+v, want AppendEntries with LeaderCommit %d", m, want)
		}
	}
}

func newTestNodeSM(t *testing.T, cfg Config, st Storage, sm StateMachine) *Node {
	t.Helper()
	n, err := NewNode(cfg, st, sm)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// followerWithSM is followerWithLog with a recording state machine that
// checks every applied entry was persisted first.
func followerWithSM(t *testing.T, log []LogEntry) (*Node, *memStorage, *recordingSM) {
	t.Helper()
	st := &memStorage{state: PersistentState{Entries: log}}
	sm := &recordingSM{st: st}
	n := newTestNodeSM(t, testConfig("a", "b", "c"), st, sm)
	step(t, n, voteReq("b", "a", 1, 100, 100))
	requireStatus(t, n, Follower, 1, "b")
	return n, st, sm
}

// withCommit sets LeaderCommit on an AppendEntries message.
func withCommit(m Message, leaderCommit Index) Message {
	m.AppendEntries.LeaderCommit = leaderCommit
	return m
}

// TestLeaderCommitsOnMajorityAck: proposing commits nothing by itself; the
// first follower acknowledgment makes a majority of 3 (with the Leader),
// the entry is committed and applied, and the next AppendEntries tells the
// followers.
func TestLeaderCommitsOnMajorityAck(t *testing.T) {
	n, _ := makeLeader(t)
	propose(t, n, "x")
	requireCommit(t, n, 0, 0)
	requireLeaderCommit(t, heartbeatTimeout(t, n), 0)

	actions := step(t, n, appendResp("b", "a", 1, true, 1))
	requireCommit(t, n, 1, 1)
	requireApplied(t, actions, [2]uint64{1, 1})
	requireLeaderCommit(t, heartbeatTimeout(t, n), 1)

	// c's late acknowledgment of the same index changes nothing.
	if actions := step(t, n, appendResp("c", "a", 1, true, 1)); len(applieds(actions)) != 0 {
		t.Fatalf("late ack applied again: %+v", actions)
	}
	requireCommit(t, n, 1, 1)
}

// figure8Leader builds S1 of Figure 8 in a 5-node cluster: node a holds
// index 1 from term 1 and index 2 from term 2 (written while it led term
// 2), and has just won term 4 with b's and c's votes.
func figure8Leader(t *testing.T) (*Node, *recordingSM) {
	t.Helper()
	st := &memStorage{state: PersistentState{CurrentTerm: 3, Entries: entries([2]uint64{1, 1}, [2]uint64{2, 2})}}
	sm := &recordingSM{st: st}
	n := newTestNodeSM(t, testConfig("a", "b", "c", "d", "e"), st, sm)
	electionTimeout(t, n)
	step(t, n, voteResp("b", "a", 4, true))
	step(t, n, voteResp("c", "a", 4, true))
	requireStatus(t, n, Leader, 4, "a")
	return n, sm
}

// TestFigure8OldTermEntryNotCommittedByCounting is Figure 8 (c): the term-4
// Leader replicates its term-2 entry to a majority (a, b, c). Counting
// replicas is not enough for an entry of an older term — Figure 8 (d) shows
// such an entry can still be overwritten — so commitIndex must not move,
// nothing is applied and no follower is told otherwise. Not even index 1,
// which ends up on all five nodes.
func TestFigure8OldTermEntryNotCommittedByCounting(t *testing.T) {
	n, sm := figure8Leader(t)
	var actions []Action
	actions = append(actions, step(t, n, appendResp("b", "a", 4, true, 2))...)
	actions = append(actions, step(t, n, appendResp("c", "a", 4, true, 2))...)
	actions = append(actions, step(t, n, appendResp("d", "a", 4, true, 1))...)
	actions = append(actions, step(t, n, appendResp("e", "a", 4, true, 1))...)
	hb := heartbeatTimeout(t, n)
	requireCommit(t, n, 0, 0)
	if len(applieds(append(actions, hb...))) != 0 || len(sm.applied) != 0 {
		t.Fatalf("old-term entries applied by counting: actions=%+v applied=%v", actions, sm.appliedIndexes())
	}
	requireLeaderCommit(t, hb, 0)
}

// TestOldTermEntryCommittedIndirectly is Figure 8 (e): once an entry of the
// Leader's own term (index 3, term 4) is on a majority, it is committed,
// and with it every entry before it (Log Matching): indexes 1, 2 and 3 are
// applied in order with the terms they were created in.
func TestOldTermEntryCommittedIndirectly(t *testing.T) {
	n, sm := figure8Leader(t)
	step(t, n, appendResp("b", "a", 4, true, 2))
	step(t, n, appendResp("c", "a", 4, true, 2))
	if idx, _ := propose(t, n, "x"); idx != 3 {
		t.Fatalf("proposed at %d, want 3", idx)
	}
	// a and b hold index 3: 2 of 5, not a majority, however often b says so.
	step(t, n, appendResp("b", "a", 4, true, 3))
	step(t, n, appendResp("b", "a", 4, true, 3))
	requireCommit(t, n, 0, 0)

	actions := step(t, n, appendResp("c", "a", 4, true, 3))
	requireCommit(t, n, 3, 3)
	requireApplied(t, actions, [2]uint64{1, 1}, [2]uint64{2, 2}, [2]uint64{3, 4})
	if got := applieds(actions)[2].Result; string(got) != "ok:x" {
		t.Fatalf("Applied result = %q, want the state machine's result", got)
	}
	if got := sm.appliedIndexes(); !slices.Equal(got, []Index{1, 2, 3}) {
		t.Fatalf("state machine applied %v, want [1 2 3]", got)
	}
	requireLeaderCommit(t, heartbeatTimeout(t, n), 3)
}

// TestFollowerCommitIndexBoundedByLog: a follower sets commitIndex to
// min(LeaderCommit, index of the last entry the message covered), only on a
// successful AppendEntries, and never lowers it.
func TestFollowerCommitIndexBoundedByLog(t *testing.T) {
	t.Run("leader commit beyond the batch", func(t *testing.T) {
		n, _, _ := followerWithSM(t, nil)
		e := entries([2]uint64{1, 1}, [2]uint64{2, 1})
		actions := step(t, n, withCommit(appendMsg("b", "a", 1, 0, 0, e), 5))
		requireCommit(t, n, 2, 2)
		requireApplied(t, actions, [2]uint64{1, 1}, [2]uint64{2, 1})
		// A heartbeat that repeats the commit index changes nothing.
		if actions := step(t, n, withCommit(heartbeat("b", "a", 1, 2, 1), 2)); len(applieds(actions)) != 0 {
			t.Fatalf("heartbeat re-applied: %+v", actions)
		}
		requireCommit(t, n, 2, 2)
	})
	t.Run("stale suffix beyond the batch", func(t *testing.T) {
		// a's entries 2 and 3 came from the Leader of term 1. b leads term 2
		// and has committed its own index 3, but has only confirmed that a
		// matches it through index 1: a's 2 and 3 may not be b's 2 and 3, so
		// a commits 1, not 3, even though its log reaches 3.
		n, _, _ := followerWithSM(t, entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1}))
		actions := step(t, n, withCommit(heartbeat("b", "a", 2, 1, 1), 3))
		requireReply(t, actions, "b", 2, true, 1)
		requireCommit(t, n, 1, 1)
	})
	t.Run("rejected append does not commit", func(t *testing.T) {
		n, _, _ := followerWithSM(t, entries([2]uint64{1, 1}))
		actions := step(t, n, withCommit(heartbeat("b", "a", 1, 2, 1), 2))
		requireReply(t, actions, "b", 1, false, 0)
		requireCommit(t, n, 0, 0)
	})
	t.Run("stale term does not commit", func(t *testing.T) {
		n, _, _ := followerWithSM(t, entries([2]uint64{1, 1}))
		step(t, n, voteReq("c", "a", 2, 100, 100)) // a moves to term 2
		actions := step(t, n, withCommit(heartbeat("b", "a", 1, 1, 1), 1))
		requireReply(t, actions, "b", 2, false, 0)
		requireCommit(t, n, 0, 0)
	})
	t.Run("reordered shorter batch never lowers it", func(t *testing.T) {
		n, _, _ := followerWithSM(t, nil)
		e := entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1})
		step(t, n, withCommit(appendMsg("b", "a", 1, 0, 0, e), 3))
		requireCommit(t, n, 3, 3)
		// An older AppendEntries for index 1 only, delayed in the network.
		step(t, n, withCommit(appendMsg("b", "a", 1, 0, 0, e[:1]), 1))
		requireCommit(t, n, 3, 3)
	})
}

// TestApplyOrderIsStrict: commitIndex jumps from 0 to 3 in one message; the
// entries are applied 1, 2, 3 in that order, each only after it was
// persisted, and the Applied actions follow the reply.
func TestApplyOrderIsStrict(t *testing.T) {
	n, _, sm := followerWithSM(t, nil)
	e := entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1})
	actions := step(t, n, withCommit(appendMsg("b", "a", 1, 0, 0, e), 3))
	requireCommit(t, n, 3, 3)
	if got := sm.appliedIndexes(); !slices.Equal(got, []Index{1, 2, 3}) {
		t.Fatalf("state machine applied %v, want [1 2 3]", got)
	}
	if len(sm.notPersisted) != 0 {
		t.Fatalf("entries %v applied before they were persisted", sm.notPersisted)
	}
	requireApplied(t, actions, [2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1})
	reply := slices.IndexFunc(actions, func(a Action) bool { _, ok := a.(SendMessage); return ok })
	first := slices.IndexFunc(actions, func(a Action) bool { _, ok := a.(Applied); return ok })
	if reply < 0 || first < reply {
		t.Fatalf("actions = %+v, want the reply before the first Applied", actions)
	}
	for i, ap := range applieds(actions) {
		if want := "ok:" + string([]byte{byte(i + 1)}); string(ap.Result) != want {
			t.Fatalf("Applied[%d].Result = %q, want %q", i, ap.Result, want)
		}
	}
}

// TestApplyErrorStopsTheApplyLoop: when the state machine fails on an
// entry, lastApplied stays just below it, the error reaches the host with
// everything that did happen (the reply, the Applied for index 1), and the
// next input retries from the failed entry, still in order.
func TestApplyErrorStopsTheApplyLoop(t *testing.T) {
	n, _, sm := followerWithSM(t, nil)
	boom := errors.New("boom")
	sm.fail = map[Index]error{2: boom}
	e := entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1})
	actions, err := n.Step(withCommit(appendMsg("b", "a", 1, 0, 0, e), 3))
	if !errors.Is(err, boom) {
		t.Fatalf("Step error = %v, want the Apply error", err)
	}
	requireReply(t, actions, "b", 1, true, 3) // the entries are durable: still acknowledged
	requireApplied(t, actions, [2]uint64{1, 1})
	requireCommit(t, n, 3, 1)

	delete(sm.fail, 2)
	actions = step(t, n, withCommit(heartbeat("b", "a", 1, 3, 1), 3))
	requireApplied(t, actions, [2]uint64{2, 1}, [2]uint64{3, 1})
	requireCommit(t, n, 3, 3)
	if got := sm.appliedIndexes(); !slices.Equal(got, []Index{1, 2, 3}) {
		t.Fatalf("state machine applied %v, want [1 2 3]", got)
	}
}

// TestCommittedEntryIsNeverTruncated: a follower refuses an AppendEntries
// that would delete an entry it already knows is committed. A correct
// Leader never sends one (Leader Completeness); the refusal keeps
// commitIndex <= lastLogIndex and the applied entries in the log even when
// a faulty peer does.
func TestCommittedEntryIsNeverTruncated(t *testing.T) {
	n, st, _ := followerWithSM(t, nil)
	e := entries([2]uint64{1, 1}, [2]uint64{2, 1})
	step(t, n, withCommit(appendMsg("b", "a", 1, 0, 0, e), 2))
	requireCommit(t, n, 2, 2)

	actions, err := n.Step(appendMsg("c", "a", 2, 1, 1, entries([2]uint64{2, 2})))
	if err == nil || !strings.Contains(err.Error(), "committed") {
		t.Fatalf("Step error = %v, want a refusal to truncate a committed entry", err)
	}
	if msgs := sends(actions); len(msgs) != 0 {
		t.Fatalf("sent %+v, want no reply", msgs)
	}
	requireLog(t, n, st, e)
	requireCommit(t, n, 2, 2)
}

// TestProposeReturnsTerm: the term Propose returns is the Leader's current
// term, which is what a host compares with Applied.Term to detect that its
// entry was overwritten.
func TestProposeReturnsTerm(t *testing.T) {
	n, _ := figure8Leader(t)
	idx, term, _, err := n.Propose([]byte("x"))
	if err != nil || idx != 3 || term != 4 {
		t.Fatalf("Propose = (%d, %d, %v), want (3, 4, nil)", idx, term, err)
	}
}
