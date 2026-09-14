package simulator

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Leader election tests. Each test stops the clock as soon as the election
// it is interested in has completed and asserts that no core error was
// recorded on the way; longer runs with heartbeats are in heartbeat_test.go
// and multi-round election runs are issue #17.

// hasLeader reports whether c currently has a unique Leader.
func hasLeader(c *Cluster) func() bool {
	return func() bool { return c.Leader() != nil }
}

// requireNoErrors fails the test when any core returned an error.
func requireNoErrors(t *testing.T, c *Cluster) {
	t.Helper()
	if errs := c.Errors(); len(errs) != 0 {
		t.Fatalf("core errors: %v", errs)
	}
}

// preloaded returns a MemoryStorage holding term/vote and a log built from
// (index, term) pairs.
func preloaded(t *testing.T, term raft.Term, votedFor raft.NodeID, pairs ...[2]uint64) *storage.MemoryStorage {
	t.Helper()
	st := storage.NewMemoryStorage()
	if err := st.SaveTermVote(term, votedFor); err != nil {
		t.Fatal(err)
	}
	var entries []raft.LogEntry
	for _, p := range pairs {
		entries = append(entries, raft.LogEntry{Index: raft.Index(p[0]), Term: raft.Term(p[1]), Command: []byte{byte(p[0])}})
	}
	if err := st.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestSingleNodeElectsItself(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a"}})
	if !c.RunUntil(hasLeader(c), time.Second) {
		t.Fatal("no Leader within 1s")
	}
	st := c.Node("a").Status()
	if st.Role != raft.Leader || st.Term != 1 || st.VotedFor != "a" {
		t.Fatalf("status = %+v, want Leader of term 1 voted for itself", st)
	}
	if now := c.Now(); now < DefaultElectionTimeoutMin || now > DefaultElectionTimeoutMax {
		t.Fatalf("elected at %v, want on the first timeout in [%v, %v]", now, DefaultElectionTimeoutMin, DefaultElectionTimeoutMax)
	}
	if c.Node("a").ElectionTimerArmed() || !c.Node("a").HeartbeatTimerArmed() {
		t.Fatal("a Leader must have its heartbeat timer, and no election timer, armed")
	}
	requireNoErrors(t, c)
}

// electionAgreed reports whether c has a unique Leader and every node is in
// the Leader's term.
func electionAgreed(c *Cluster) func() bool {
	return func() bool {
		l := c.Leader()
		if l == nil {
			return false
		}
		for _, n := range c.Nodes() {
			if n.Status().Term != l.Status().Term {
				return false
			}
		}
		return true
	}
}

func TestThreeNodeElection(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := newTestCluster(t, Config{Seed: seed, NodeIDs: []raft.NodeID{"a", "b", "c"},
				MinLatency: 1 * time.Millisecond, MaxLatency: 20 * time.Millisecond})
			if !c.RunUntil(electionAgreed(c), 2*time.Second) {
				t.Fatalf("no agreed Leader within 2s: roles=%v", c.Roles())
			}
			leader := c.Leader()
			leaders := 0
			for _, r := range c.Roles() {
				if r == raft.Leader {
					leaders++
				}
			}
			if leaders != 1 {
				t.Fatalf("roles = %v, want exactly one Leader", c.Roles())
			}
			if st := leader.Status(); st.VotedFor != st.ID || st.Term == 0 {
				t.Fatalf("leader status = %+v", st)
			}
			requireNoErrors(t, c)
		})
	}
}

// TestSplitVoteEventuallyElects forces two simultaneous candidates in term
// 1 whose RequestVotes to the third node are dropped: each votes for itself,
// denies the other, and nobody wins term 1. The randomized timeouts then
// resolve the split in a later term.
func TestSplitVoteEventuallyElects(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	c.Network().Drop("a", "c")
	c.Network().Drop("b", "c")
	c.Node("a").ForceElectionTimeout()
	c.Node("b").ForceElectionTimeout()
	c.Run(3 * DefaultLatency) // both RequestVotes and both denials have been delivered

	for _, id := range []raft.NodeID{"a", "b"} {
		if st := c.Node(id).Status(); st.Role != raft.Candidate || st.Term != 1 || st.VotedFor != id {
			t.Fatalf("node %s after the split: %+v, want Candidate of term 1 voted for itself", id, st)
		}
	}
	if st := c.Node("c").Status(); st.Term != 0 {
		t.Fatalf("node c heard about term 1: %+v", st)
	}
	if !timelineHas(c, `event=VoteDenied node=a term=1 role=Candidate peer=b reason="already voted"`) ||
		!timelineHas(c, `event=VoteDenied node=b term=1 role=Candidate peer=a reason="already voted"`) {
		t.Fatal("timeline missing the mutual denials")
	}

	if !c.RunUntil(hasLeader(c), 2*time.Second) {
		t.Fatalf("no Leader within 2s: roles=%v", c.Roles())
	}
	if term := c.Leader().Status().Term; term < 2 {
		t.Fatalf("Leader elected in term %d, want a term after the split", term)
	}
	if timelineHas(c, "event=BecameLeader node=a term=1") || timelineHas(c, "event=BecameLeader node=b term=1") {
		t.Fatal("a Leader was elected in the split term")
	}
	requireNoErrors(t, c)
}

// TestStaleLogCandidateLoses: a candidate whose last log term is lower than
// the voters' cannot collect a majority, no matter how many terms it starts.
func TestStaleLogCandidateLoses(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
		Stores: map[raft.NodeID]*storage.MemoryStorage{
			"b": preloaded(t, 1, raft.None, [2]uint64{1, 1}),
			"c": preloaded(t, 1, raft.None, [2]uint64{1, 1}),
		}})
	c.Node("a").ForceElectionTimeout()
	c.Run(3 * DefaultLatency)
	if st := c.Node("a").Status(); st.Role != raft.Candidate || st.Term != 1 {
		t.Fatalf("stale candidate: %+v, want still a Candidate of term 1", st)
	}
	for _, id := range []raft.NodeID{"b", "c"} {
		if st := c.Node(id).Status(); st.VotedFor != raft.None {
			t.Fatalf("node %s voted for %s, want no vote for a stale log", id, st.VotedFor)
		}
	}
	if !timelineHas(c, `event=VoteDenied node=b term=1 role=Follower peer=a reason="stale log"`) {
		t.Fatal("timeline missing the stale-log denial")
	}

	if !c.RunUntil(hasLeader(c), 2*time.Second) {
		t.Fatalf("no Leader within 2s: roles=%v", c.Roles())
	}
	if l := c.Leader().ID(); l == "a" {
		t.Fatal("the stale-log node became Leader")
	}
	if timelineHas(c, "event=BecameLeader node=a") {
		t.Fatal("the stale-log node became Leader at some point")
	}
	requireNoErrors(t, c)
}

// TestDeniedVoteDoesNotResetTimer: a denied RequestVote leaves the
// receiver's election deadline where it was, while a granted one moves it.
func TestDeniedVoteDoesNotResetTimer(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
		Stores: map[raft.NodeID]*storage.MemoryStorage{"c": preloaded(t, 1, raft.None, [2]uint64{1, 1})}})
	bDeadline, _ := c.Node("b").ElectionDeadline()
	cDeadline, _ := c.Node("c").ElectionDeadline()

	c.Node("a").ForceElectionTimeout() // term 1, empty log: b grants, c denies
	c.Run(2 * DefaultLatency)
	if !timelineHas(c, `event=VoteDenied node=c term=1 role=Follower peer=a reason="stale log"`) ||
		!timelineHas(c, "event=VoteGranted node=b term=1 role=Follower peer=a") {
		t.Fatal("timeline missing c's denial or b's grant")
	}
	if at, ok := c.Node("c").ElectionDeadline(); !ok || at != cDeadline {
		t.Fatalf("c's election deadline moved from %v to %v,%v after denying a vote", cDeadline, at, ok)
	}
	if at, ok := c.Node("b").ElectionDeadline(); !ok || at == bDeadline || at < c.Now()+DefaultElectionTimeoutMin {
		t.Fatalf("b's election deadline = %v,%v after granting a vote at %v; want it reset (was %v)", at, ok, c.Now(), bDeadline)
	}
	requireNoErrors(t, c)
}

// electionRun elects a Leader in a 3-node cluster with random latency and
// returns the timeline and the Leader's id.
func electionRun(t *testing.T, seed int64) ([]string, raft.NodeID) {
	t.Helper()
	c := newTestCluster(t, Config{Seed: seed, NodeIDs: []raft.NodeID{"a", "b", "c"},
		MinLatency: 1 * time.Millisecond, MaxLatency: 30 * time.Millisecond})
	if !c.RunUntil(electionAgreed(c), 2*time.Second) {
		t.Fatalf("no agreed Leader within 2s: roles=%v", c.Roles())
	}
	requireNoErrors(t, c)
	return c.Timeline(), c.Leader().ID()
}

// TestElectionDeterministicWithSeed: the same seed replays the same
// election, byte for byte.
func TestElectionDeterministicWithSeed(t *testing.T) {
	const seed = 7
	first, leader1 := electionRun(t, seed)
	second, leader2 := electionRun(t, seed)
	if leader1 != leader2 || !slices.Equal(first, second) {
		t.Fatalf("seed=%d replayed differently: leader %s vs %s", seed, leader1, leader2)
	}
	if len(first) < 10 {
		t.Fatalf("timeline too short to be meaningful: %d lines", len(first))
	}
}
