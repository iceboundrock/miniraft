package simulator

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// fakeCore is a scripted Core: it records every input and returns the
// actions the test configured.
type fakeCore struct {
	id                raft.NodeID
	steps             []raft.Message
	electionTimeouts  int
	heartbeatTimeouts int
	proposals         [][]byte
	onElectionTimeout func() []raft.Action
	onStep            func(raft.Message) []raft.Action
	err               error
}

func (f *fakeCore) Step(msg raft.Message) ([]raft.Action, error) {
	f.steps = append(f.steps, msg)
	if f.onStep != nil {
		return f.onStep(msg), f.err
	}
	return nil, f.err
}

func (f *fakeCore) ElectionTimeout() ([]raft.Action, error) {
	f.electionTimeouts++
	if f.onElectionTimeout != nil {
		return f.onElectionTimeout(), f.err
	}
	return nil, f.err
}

func (f *fakeCore) HeartbeatTimeout() ([]raft.Action, error) {
	f.heartbeatTimeouts++
	return nil, f.err
}

func (f *fakeCore) Propose(cmd []byte) ([]raft.Action, error) {
	f.proposals = append(f.proposals, cmd)
	return nil, f.err
}

func (f *fakeCore) Status() raft.Status { return raft.Status{ID: f.id} }

// TestClusterTranslatesActions drives SimNode.Execute with a hand-written
// action list and checks each action's effect on the clock and network.
func TestClusterTranslatesActions(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
	fa := &fakeCore{id: "a"}
	fb := &fakeCore{id: "b"}
	a := c.AddNode("a", fa)
	b := c.AddNode("b", fb)

	// ResetElectionTimer arms one timer; a second reset replaces it.
	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 100 * time.Millisecond}})
	if !a.ElectionTimerArmed() || c.Clock().Pending() != 1 {
		t.Fatalf("after ResetElectionTimer: armed=%v pending=%d", a.ElectionTimerArmed(), c.Clock().Pending())
	}
	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 200 * time.Millisecond}})
	if d, _ := c.Clock().NextDeadline(); c.Clock().Pending() != 1 || d != 200*time.Millisecond {
		t.Fatalf("second ResetElectionTimer must cancel the first: pending=%d next=%v", c.Clock().Pending(), d)
	}
	c.Run(150 * time.Millisecond)
	if fa.electionTimeouts != 0 {
		t.Fatalf("phantom timeout: ElectionTimeout called %d times before 200ms", fa.electionTimeouts)
	}

	// When the timer fires, the core's actions are executed: SendMessage
	// reaches the network and is delivered to b after the latency.
	fa.onElectionTimeout = func() []raft.Action {
		return []raft.Action{raft.SendMessage{Message: vote("a", "b", 1)}}
	}
	c.Run(200 * time.Millisecond)
	if fa.electionTimeouts != 1 || a.ElectionTimerArmed() {
		t.Fatalf("ElectionTimeout called %d times, armed=%v; want 1, false", fa.electionTimeouts, a.ElectionTimerArmed())
	}
	if len(fb.steps) != 0 {
		t.Fatal("message delivered before latency elapsed")
	}
	c.Run(205 * time.Millisecond)
	if len(fb.steps) != 1 || fb.steps[0].Term() != 1 {
		t.Fatalf("b received %+v, want one term-1 RequestVote", fb.steps)
	}
	if !timelineHas(c, "t=200 event=ElectionTimeout node=a") {
		t.Fatal("timeline missing ElectionTimeout event")
	}

	// StopElectionTimer cancels; the core is never called.
	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 10 * time.Millisecond}, raft.StopElectionTimer{}})
	c.Run(300 * time.Millisecond)
	if fa.electionTimeouts != 1 || a.ElectionTimerArmed() {
		t.Fatalf("StopElectionTimer did not cancel: calls=%d armed=%v", fa.electionTimeouts, a.ElectionTimerArmed())
	}

	// Heartbeat timer is independent of the election timer.
	b.Execute([]raft.Action{
		raft.ResetHeartbeatTimer{Interval: 50 * time.Millisecond},
		raft.ResetElectionTimer{Timeout: 70 * time.Millisecond},
	})
	if !b.HeartbeatTimerArmed() || !b.ElectionTimerArmed() || c.Clock().Pending() != 2 {
		t.Fatalf("both timers should be armed: pending=%d", c.Clock().Pending())
	}
	b.Execute([]raft.Action{raft.StopHeartbeatTimer{}})
	c.Run(400 * time.Millisecond)
	if fb.heartbeatTimeouts != 0 || fb.electionTimeouts != 1 {
		t.Fatalf("heartbeat=%d election=%d, want 0 and 1", fb.heartbeatTimeouts, fb.electionTimeouts)
	}

	// Applied is recorded on the timeline; Propose reaches the core.
	a.Execute([]raft.Action{raft.Applied{Index: 3, Term: 2, Result: []byte("ok")}})
	if !timelineHas(c, "event=Applied node=a index=3 term=2") {
		t.Fatal("timeline missing Applied event")
	}
	a.Propose([]byte("SET x 1"))
	if len(fa.proposals) != 1 || string(fa.proposals[0]) != "SET x 1" {
		t.Fatalf("proposals = %q", fa.proposals)
	}
	if errs := c.Errors(); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

// TestClusterRecordsCoreErrors: an error from the core is logged and kept,
// and any actions returned alongside it are still executed.
func TestClusterRecordsCoreErrors(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1})
	fa := &fakeCore{id: "a", err: errors.New("boom")}
	a := c.AddNode("a", fa)
	a.HandleMessage(vote("b", "a", 1))
	errs := c.Errors()
	if len(errs) != 1 || !errors.Is(errs[0], fa.err) {
		t.Fatalf("Errors() = %v, want one wrapping boom", errs)
	}
	if !timelineHas(c, `event=error node=a err=boom`) {
		t.Fatal("timeline missing error event")
	}
}

// TestClusterHostsRaftNodes: NewCluster builds real raft.Nodes over
// MemoryStorage. Their protocol entry points are stubs in this issue, so a
// fired timer reaches the core and its ErrNotImplemented is recorded.
func TestClusterHostsRaftNodes(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"c", "a", "b"}})
	nodes := c.Nodes()
	var ids []raft.NodeID
	for _, n := range nodes {
		ids = append(ids, n.ID())
		st := n.Status()
		if st.ID != n.ID() || st.Role != raft.Follower || st.Term != 0 {
			t.Fatalf("node %s status = %+v, want fresh Follower", n.ID(), st)
		}
		if n.Storage == nil {
			t.Fatalf("node %s has no MemoryStorage", n.ID())
		}
	}
	if !slices.Equal(ids, []raft.NodeID{"a", "b", "c"}) {
		t.Fatalf("Nodes() order = %v, want sorted", ids)
	}
	if c.Node("zz") != nil {
		t.Fatal("Node(unknown) must be nil")
	}

	c.Node("a").Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 100 * time.Millisecond}})
	c.Run(100 * time.Millisecond)
	errs := c.Errors()
	if len(errs) != 1 || !errors.Is(errs[0], raft.ErrNotImplemented) {
		t.Fatalf("Errors() = %v, want one ErrNotImplemented", errs)
	}
	if c.Timeline()[0] != "t=0 event=cluster seed=1 nodes=3" {
		t.Fatalf("first timeline line = %q", c.Timeline()[0])
	}
}

func TestClusterRejectsInvalidConfig(t *testing.T) {
	if _, err := NewCluster(Config{NodeIDs: []raft.NodeID{"a", "a"}}); err == nil {
		t.Fatal("duplicate NodeIDs accepted")
	}
	if _, err := NewCluster(Config{NodeIDs: []raft.NodeID{"a", ""}}); err == nil {
		t.Fatal("empty NodeID accepted")
	}
	if _, err := NewCluster(Config{NodeIDs: []raft.NodeID{"a"}, HeartbeatInterval: time.Second}); err == nil {
		t.Fatal("heartbeat >= election timeout accepted")
	}
}

func TestClusterAddNodeRejectsDuplicate(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a"}})
	assertPanics(t, "AddNode(a) twice", func() { c.AddNode("a", &fakeCore{id: "a"}) })
}

// TestClusterRunUntil stops as soon as the predicate holds, or at maxTime.
func TestClusterRunUntil(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1})
	fa := &fakeCore{id: "a"}
	a := c.AddNode("a", fa)
	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 30 * time.Millisecond}})

	if !c.RunUntil(func() bool { return fa.electionTimeouts == 1 }, time.Second) {
		t.Fatal("RunUntil returned false although the predicate became true")
	}
	if c.Now() != 30*time.Millisecond {
		t.Fatalf("RunUntil stopped at %v, want 30ms (as soon as the predicate held)", c.Now())
	}
	if c.RunUntil(func() bool { return false }, 100*time.Millisecond) {
		t.Fatal("RunUntil returned true for an always-false predicate")
	}
	if c.Now() != 100*time.Millisecond {
		t.Fatalf("RunUntil(false) stopped at %v, want maxTime 100ms", c.Now())
	}
	if !c.RunUntil(func() bool { return true }, 50*time.Millisecond) || c.Now() != 100*time.Millisecond {
		t.Fatalf("RunUntil with an already-true predicate must not move time: now=%v", c.Now())
	}
}

// TestClusterStepRunsOneEvent: Step runs exactly one event.
func TestClusterStepRunsOneEvent(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1})
	fa := &fakeCore{id: "a"}
	a := c.AddNode("a", fa)
	a.Execute([]raft.Action{
		raft.ResetElectionTimer{Timeout: 10 * time.Millisecond},
		raft.ResetHeartbeatTimer{Interval: 20 * time.Millisecond},
	})
	if !c.Step() || fa.electionTimeouts != 1 || fa.heartbeatTimeouts != 0 || c.Now() != 10*time.Millisecond {
		t.Fatalf("after one Step: election=%d heartbeat=%d now=%v", fa.electionTimeouts, fa.heartbeatTimeouts, c.Now())
	}
	if !c.Step() || fa.heartbeatTimeouts != 1 || c.Now() != 20*time.Millisecond {
		t.Fatalf("after two Steps: heartbeat=%d now=%v", fa.heartbeatTimeouts, c.Now())
	}
	if c.Step() {
		t.Fatal("Step() = true with nothing pending")
	}
}

// echo is a stub Handler that answers every message with term+1 until a cap,
// producing a message storm whose interleaving depends on latency.
type echo struct {
	id      raft.NodeID
	net     *Network
	maxTerm raft.Term
}

func (e *echo) HandleMessage(msg raft.Message) {
	if msg.Term() >= e.maxTerm {
		return
	}
	e.net.Send(vote(e.id, msg.From, msg.Term()+1))
}

// echoScenario runs three echo handlers with random latency and returns the
// timeline.
func echoScenario(t *testing.T, seed int64) []string {
	t.Helper()
	c := newTestCluster(t, Config{Seed: seed, MinLatency: 1 * time.Millisecond, MaxLatency: 20 * time.Millisecond})
	ids := []raft.NodeID{"a", "b", "c"}
	for _, id := range ids {
		c.Network().Register(id, &echo{id: id, net: c.Network(), maxTerm: 5})
	}
	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				c.Network().Send(vote(from, to, 1))
			}
		}
	}
	c.Run(time.Second)
	return c.Timeline()
}

// TestDeterministicReplay: the same seed yields a byte-identical timeline.
// A different seed yields a different one — that is not a guarantee in
// general (two seeds could draw the same latencies), but it holds for the
// seeds used here and documents that the seed is what varies a run.
func TestDeterministicReplay(t *testing.T) {
	first := echoScenario(t, 1)
	second := echoScenario(t, 1)
	if !slices.Equal(first, second) {
		t.Fatalf("same seed produced different timelines:\n%s\n---\n%s", first, second)
	}
	if len(first) < 20 {
		t.Fatalf("scenario too short to be meaningful: %d lines", len(first))
	}
	other := echoScenario(t, 2)
	if slices.Equal(first, other) {
		t.Fatal("seeds 1 and 2 produced identical timelines")
	}
}
