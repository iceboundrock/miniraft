package simulator

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Heartbeat tests (issue #6): a Leader's empty AppendEntries keep followers
// from timing out, a silent Leader gets replaced, and the consistency check
// already reports a diverged follower.

// countTimeline returns how many timeline lines contain substr.
func countTimeline(c *Cluster, substr string) int {
	n := 0
	for _, line := range c.Timeline() {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// electLeader runs c until an agreed Leader exists and returns it.
func electLeader(t *testing.T, c *Cluster) *SimNode {
	t.Helper()
	if !c.RunUntil(electionAgreed(c), 2*time.Second) {
		t.Fatalf("no agreed Leader within 2s: roles=%v", c.Roles())
	}
	return c.Leader()
}

func TestRunFor(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a"}})
	c.Run(100 * time.Millisecond)
	c.RunFor(250 * time.Millisecond)
	if now := c.Now(); now != 350*time.Millisecond {
		t.Fatalf("Now = %v after Run(100ms)+RunFor(250ms), want 350ms", now)
	}
	// Events inside the window ran: a single node elects itself on its
	// first timeout, which lies in [150ms, 300ms].
	if c.Node("a").Status().Role != raft.Leader {
		t.Fatal("RunFor did not execute the election timeout inside its window")
	}
}

// TestRunForRejectsOverflow: RunFor must follow the clock's overflow policy
// (panic, never wrap). An empty cluster has no timers, so it can be advanced
// to the end of logical time without firing anything.
func TestRunForRejectsOverflow(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1})
	end := time.Duration(math.MaxInt64)
	c.RunFor(end)
	if now := c.Now(); now != end {
		t.Fatalf("Now = %v after RunFor(MaxInt64), want %v", now, end)
	}
	assertPanics(t, "RunFor(1ns) at end of time", func() { c.RunFor(time.Nanosecond) })
	if now := c.Now(); now != end {
		t.Fatalf("Now = %v after rejected RunFor, want %v (clock untouched)", now, end)
	}
}

func TestStableLeaderNoReelection(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := newTestCluster(t, Config{Seed: seed, NodeIDs: []raft.NodeID{"a", "b", "c"},
				MinLatency: 1 * time.Millisecond, MaxLatency: 20 * time.Millisecond})
			leader := electLeader(t, c)
			term := leader.Status().Term

			c.RunFor(50 * DefaultElectionTimeoutMax)

			if got := countTimeline(c, "event=BecameLeader"); got != 1 {
				t.Fatalf("timeline has %d BecameLeader events, want exactly 1", got)
			}
			if got := countTimeline(c, "event=BecameCandidate"); got == 0 {
				t.Fatal("timeline has no BecameCandidate at all; the first election is missing")
			}
			for _, n := range c.Nodes() {
				st := n.Status()
				if st.Term != term || st.LeaderID != leader.ID() {
					t.Fatalf("node %s: %+v, want term %d recognizing leader %s", n.ID(), st, term, leader.ID())
				}
			}
			if !timelineHas(c, "event=Heartbeat node="+string(leader.ID())) ||
				!timelineHas(c, "event=AppendEntriesAck node="+string(leader.ID())) {
				t.Fatal("timeline missing Heartbeat / AppendEntriesAck events")
			}
			requireNoErrors(t, c)
		})
	}
}

func TestSilentLeaderTriggersReelection(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	old := electLeader(t, c)
	oldTerm := old.Status().Term
	c.RunFor(3 * DefaultHeartbeatInterval) // a few heartbeats reach the followers

	c.StopHeartbeats(old.ID())
	newLeaderElected := func() bool {
		l := c.Leader()
		return l != nil && l.ID() != old.ID() && l.Status().Term > oldTerm
	}
	if !c.RunUntil(newLeaderElected, 5*time.Second) {
		t.Fatalf("no new Leader after silencing %s: roles=%v", old.ID(), c.Roles())
	}
	if got := countTimeline(c, "event=BecameLeader"); got != 2 {
		t.Fatalf("timeline has %d BecameLeader events, want 2", got)
	}
	if elapsed := c.Now(); elapsed > 2*time.Second {
		t.Fatalf("re-election took until %v, want well within a few election timeouts", elapsed)
	}
	requireNoErrors(t, c)
}

// TestOldLeaderStepsDownOnHigherTerm continues the silent-Leader scenario:
// the old Leader still receives messages, so the first one carrying the new
// term (a RequestVote or the new Leader's heartbeat) makes it a Follower.
func TestOldLeaderStepsDownOnHigherTerm(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 2, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	old := electLeader(t, c)
	c.StopHeartbeats(old.ID())

	steppedDown := func() bool {
		l := c.Leader()
		return l != nil && l.ID() != old.ID() && old.Status().Role == raft.Follower
	}
	if !c.RunUntil(steppedDown, 5*time.Second) {
		t.Fatalf("old Leader %s did not step down: roles=%v", old.ID(), c.Roles())
	}
	newLeader := c.Leader()
	if st := old.Status(); st.Term != newLeader.Status().Term {
		t.Fatalf("old Leader is in term %d, new Leader in term %d", st.Term, newLeader.Status().Term)
	}
	if old.HeartbeatTimerArmed() || !old.ElectionTimerArmed() {
		t.Fatal("a deposed Leader must have an election timer and no heartbeat timer")
	}
	if !timelineHas(c, "event=SteppedDown node="+string(old.ID())+" ") {
		t.Fatal("timeline missing the old Leader's SteppedDown")
	}

	// Heartbeats resumed, the old Leader follows the new one and no further
	// election happens.
	c.ResumeHeartbeats(old.ID())
	c.RunFor(10 * DefaultElectionTimeoutMax)
	if st := old.Status(); st.Role != raft.Follower || st.LeaderID != newLeader.ID() {
		t.Fatalf("old Leader: %+v, want a Follower of %s", st, newLeader.ID())
	}
	if got := countTimeline(c, "event=BecameLeader"); got != 2 {
		t.Fatalf("timeline has %d BecameLeader events, want 2", got)
	}
	requireNoErrors(t, c)
}

// TestHeartbeatConsistencyCheckFails: node a is elected with a log the
// others lack, so its first heartbeats (prevLogIndex = 2) are rejected by
// both followers with Success=false, while they still recognize a as
// Leader. The repair that follows is covered in replication_test.go.
func TestHeartbeatConsistencyCheckFails(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
		Stores: map[raft.NodeID]*storage.MemoryStorage{
			"a": preloaded(t, 1, raft.None, [2]uint64{1, 1}, [2]uint64{2, 1}),
		}})
	c.Node("a").ForceElectionTimeout()
	if !c.RunUntil(hasLeader(c), time.Second) || c.Leader().ID() != "a" {
		t.Fatalf("a was not elected: roles=%v", c.Roles())
	}
	c.RunFor(DefaultHeartbeatInterval + 3*DefaultLatency) // one heartbeat round trip

	for _, id := range []raft.NodeID{"b", "c"} {
		if !timelineHas(c, "event=AppendEntriesAck node=a term=2 role=Leader peer="+string(id)+" success=false") {
			t.Fatalf("timeline missing the failed ack from %s", id)
		}
		if st := c.Node(id).Status(); st.LeaderID != "a" || st.Role != raft.Follower {
			t.Fatalf("node %s: %+v, want a Follower of a", id, st)
		}
	}
	if got := countTimeline(c, "event=BecameLeader"); got != 1 {
		t.Fatalf("timeline has %d BecameLeader events, want exactly 1", got)
	}
	requireNoErrors(t, c)
}

// TestStaleAppendEntriesRejected: a heartbeat carrying a deposed Leader's
// term is answered with the receiver's current term and does not move its
// election deadline. The old Leader's own heartbeats are muted in the
// network, so the stale one is injected by hand.
func TestStaleAppendEntriesRejected(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 2, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	old := electLeader(t, c)
	oldTerm := old.Status().Term
	c.StopHeartbeats(old.ID())
	replaced := func() bool {
		l := c.Leader()
		return l != nil && l.ID() != old.ID()
	}
	if !c.RunUntil(replaced, 5*time.Second) {
		t.Fatalf("no new Leader: roles=%v", c.Roles())
	}
	newLeader := c.Leader()
	var follower *SimNode
	for _, n := range c.Nodes() {
		if n != old && n != newLeader {
			follower = n
		}
	}
	c.RunFor(2 * DefaultHeartbeatInterval) // the follower has heard from the new Leader
	if st := follower.Status(); st.LeaderID != newLeader.ID() {
		t.Fatalf("%s: %+v, want a Follower of %s", follower.ID(), st, newLeader.ID())
	}

	deadline, _ := follower.ElectionDeadline()
	follower.HandleMessage(raft.Message{From: old.ID(), To: follower.ID(), Type: raft.MsgAppendEntries,
		AppendEntries: &raft.AppendEntries{Term: oldTerm, LeaderID: old.ID()}})
	if at, ok := follower.ElectionDeadline(); !ok || at != deadline {
		t.Fatalf("%s's election deadline moved from %v to %v after a stale heartbeat", follower.ID(), deadline, at)
	}
	if st := follower.Status(); st.LeaderID != newLeader.ID() || st.Term != newLeader.Status().Term {
		t.Fatalf("%s after the stale heartbeat: %+v, want unchanged", follower.ID(), st)
	}
	if !timelineHas(c, fmt.Sprintf("event=RejectStaleLeader node=%s term=%d role=Follower peer=%s leaderTerm=%d",
		follower.ID(), newLeader.Status().Term, old.ID(), oldTerm)) {
		t.Fatal("timeline missing RejectStaleLeader")
	}
	if !timelineHas(c, fmt.Sprintf("event=send from=%s to=%s type=AppendEntriesResponse term=%d",
		follower.ID(), old.ID(), newLeader.Status().Term)) {
		t.Fatal("timeline missing the rejection carrying the current term")
	}
	requireNoErrors(t, c)
}

func TestStopHeartbeatsMutesOnlyAppendEntries(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	before := len(c.Timeline())
	c.StopHeartbeats(leader.ID())
	c.RunFor(2 * DefaultHeartbeatInterval)
	after := c.Timeline()[before:]
	drops, sends := 0, 0
	for _, line := range after {
		if strings.Contains(line, "event=drop from="+string(leader.ID())) && strings.Contains(line, "reason=heartbeats-stopped") {
			drops++
		}
		if strings.Contains(line, "event=send from="+string(leader.ID())) {
			sends++
		}
	}
	if drops != 4 || sends != 0 {
		t.Fatalf("after muting: %d dropped heartbeats and %d sends, want 4 (2 rounds x 2 peers) and 0", drops, sends)
	}
	c.ResumeHeartbeats(leader.ID())
	before = len(c.Timeline())
	c.RunFor(DefaultHeartbeatInterval)
	if !strings.Contains(strings.Join(c.Timeline()[before:], "\n"), "event=send from="+string(leader.ID())+" to=") {
		t.Fatal("heartbeats did not resume")
	}
}
