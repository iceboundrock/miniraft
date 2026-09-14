package simulator

import (
	"errors"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// fakeCore is a scripted Core: it records every input and returns the
// actions the test configured.
type fakeCore struct {
	id                raft.NodeID
	status            raft.Status // returned by Status (ID is filled in)
	starts            int
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

func (f *fakeCore) Propose(cmd []byte) (raft.Index, []raft.Action, error) {
	f.proposals = append(f.proposals, cmd)
	return raft.Index(len(f.proposals)), nil, f.err
}

func (f *fakeCore) Start() []raft.Action {
	f.starts++
	return nil
}

func (f *fakeCore) Status() raft.Status {
	st := f.status
	st.ID = f.id
	return st
}

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
	if idx, err := a.Propose([]byte("SET x 1")); idx != 1 || err != nil {
		t.Fatalf("Propose = (%d, %v), want (1, nil)", idx, err)
	}
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
// MemoryStorage and arms each node's first election timer via Start().
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
		at, ok := n.ElectionDeadline()
		if !n.ElectionTimerArmed() || !ok || at < DefaultElectionTimeoutMin || at > DefaultElectionTimeoutMax {
			t.Fatalf("node %s: election timer armed=%v deadline=%v,%v; want a deadline in [%v, %v]",
				n.ID(), n.ElectionTimerArmed(), at, ok, DefaultElectionTimeoutMin, DefaultElectionTimeoutMax)
		}
	}
	if !slices.Equal(ids, []raft.NodeID{"a", "b", "c"}) {
		t.Fatalf("Nodes() order = %v, want sorted", ids)
	}
	if c.Node("zz") != nil {
		t.Fatal("Node(unknown) must be nil")
	}
	if c.Clock().Pending() != 3 {
		t.Fatalf("Pending() = %d, want one election timer per node", c.Clock().Pending())
	}
	if c.Timeline()[0] != "t=0 event=cluster seed=1 nodes=3" {
		t.Fatalf("first timeline line = %q", c.Timeline()[0])
	}
}

// TestClusterUsesProvidedStores: a node listed in Config.Stores is built
// over that store, so tests can pre-load a term, a vote or a log.
func TestClusterUsesProvidedStores(t *testing.T) {
	st := storage.NewMemoryStorage()
	if err := st.SaveTermVote(3, "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries([]raft.LogEntry{{Index: 1, Term: 2, Command: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b"}, Stores: map[raft.NodeID]*storage.MemoryStorage{"a": st}})
	got := c.Node("a").Status()
	if got.Term != 3 || got.VotedFor != "b" || got.LastLogIndex != 1 || c.Node("a").Storage != st {
		t.Fatalf("node a status = %+v, want the pre-loaded state", got)
	}
	if c.Node("b").Status().Term != 0 || c.Node("b").Storage == nil {
		t.Fatal("node b must get a fresh store")
	}
}

func TestClusterRejectsStoreForUnknownNode(t *testing.T) {
	_, err := NewCluster(Config{NodeIDs: []raft.NodeID{"a"}, Stores: map[raft.NodeID]*storage.MemoryStorage{"zz": storage.NewMemoryStorage()}})
	if err == nil {
		t.Fatal("Stores for a node that is not in NodeIDs accepted")
	}
}

// TestForceElectionTimeout: the hook cancels the pending timer and drives
// the core's ElectionTimeout immediately, at the current time.
func TestForceElectionTimeout(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1})
	fa := &fakeCore{id: "a"}
	a := c.AddNode("a", fa)
	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 100 * time.Millisecond}})
	c.Run(10 * time.Millisecond)

	a.ForceElectionTimeout()
	if fa.electionTimeouts != 1 || a.ElectionTimerArmed() || c.Clock().Pending() != 0 || c.Now() != 10*time.Millisecond {
		t.Fatalf("after ForceElectionTimeout: timeouts=%d armed=%v pending=%d now=%v",
			fa.electionTimeouts, a.ElectionTimerArmed(), c.Clock().Pending(), c.Now())
	}
	c.Run(200 * time.Millisecond)
	if fa.electionTimeouts != 1 {
		t.Fatalf("the cancelled timer still fired: timeouts=%d", fa.electionTimeouts)
	}
	// Without a pending timer the hook still drives the core.
	a.ForceElectionTimeout()
	if fa.electionTimeouts != 2 {
		t.Fatalf("ForceElectionTimeout with no timer armed: timeouts=%d, want 2", fa.electionTimeouts)
	}
	if !timelineHas(c, "event=ForceElectionTimeout node=a") {
		t.Fatal("timeline missing ForceElectionTimeout")
	}
}

// TestClusterLeaderAndRoles: Leader() is the unique Leader of the highest
// term; nil with no Leader or with two Leaders claiming the highest term.
// Built with NewCluster directly: the scripted double Leader would trip
// newTestCluster's invariant assertion.
func TestClusterLeaderAndRoles(t *testing.T) {
	c, err := NewCluster(Config{Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	fa, fb, fc := &fakeCore{id: "a"}, &fakeCore{id: "b"}, &fakeCore{id: "c"}
	c.AddNode("a", fa)
	c.AddNode("b", fb)
	c.AddNode("c", fc)
	if c.Leader() != nil {
		t.Fatal("Leader() with no leader must be nil")
	}
	fa.status = raft.Status{Role: raft.Leader, Term: 1}
	fb.status = raft.Status{Role: raft.Leader, Term: 2}
	fc.status = raft.Status{Role: raft.Candidate, Term: 2}
	if l := c.Leader(); l == nil || l.ID() != "b" {
		t.Fatalf("Leader() = %v, want b (Leader of the highest term)", l)
	}
	want := map[raft.NodeID]raft.Role{"a": raft.Leader, "b": raft.Leader, "c": raft.Candidate}
	if got := c.Roles(); !maps.Equal(got, want) {
		t.Fatalf("Roles() = %v, want %v", got, want)
	}
	fa.status = raft.Status{Role: raft.Leader, Term: 2}
	if c.Leader() != nil {
		t.Fatal("Leader() with two Leaders in the highest term must be nil")
	}
}

// TestInvariantChecker: violations are detected after every core input,
// logged to the timeline and reported by AssertInvariants. Clusters are
// built with NewCluster directly because newTestCluster fails the test on
// any violation.
func TestInvariantChecker(t *testing.T) {
	newScripted := func(t *testing.T) (*Cluster, *fakeCore, *fakeCore) {
		t.Helper()
		c, err := NewCluster(Config{Seed: 1})
		if err != nil {
			t.Fatal(err)
		}
		fa, fb := &fakeCore{id: "a"}, &fakeCore{id: "b"}
		c.AddNode("a", fa)
		c.AddNode("b", fb)
		return c, fa, fb
	}
	poke := func(c *Cluster) { c.Node("a").HandleMessage(vote("b", "a", 1)) }

	t.Run("clean", func(t *testing.T) {
		c, fa, fb := newScripted(t)
		fa.status = raft.Status{Role: raft.Leader, Term: 1, VotedFor: "a"}
		fb.status = raft.Status{Role: raft.Follower, Term: 1, VotedFor: "a"}
		poke(c)
		fa.status = raft.Status{Role: raft.Follower, Term: 2, VotedFor: "b"}
		fb.status = raft.Status{Role: raft.Leader, Term: 2, VotedFor: "b"}
		poke(c)
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("AssertInvariants() = %v, want nil", err)
		}
	})
	t.Run("election safety", func(t *testing.T) {
		c, fa, fb := newScripted(t)
		fa.status = raft.Status{Role: raft.Leader, Term: 1}
		poke(c)
		fa.status = raft.Status{Role: raft.Follower, Term: 1}
		fb.status = raft.Status{Role: raft.Leader, Term: 1} // a second Leader in term 1, later in time
		poke(c)
		err := c.AssertInvariants()
		if err == nil || !strings.Contains(err.Error(), "Election Safety") {
			t.Fatalf("AssertInvariants() = %v, want an Election Safety violation", err)
		}
		if !timelineHas(c, "event=invariant-violation") {
			t.Fatal("violation not on the timeline")
		}
	})
	t.Run("term monotonicity", func(t *testing.T) {
		c, fa, _ := newScripted(t)
		fa.status = raft.Status{Term: 5}
		poke(c)
		fa.status = raft.Status{Term: 4}
		poke(c)
		if err := c.AssertInvariants(); err == nil || !strings.Contains(err.Error(), "Term Monotonicity") {
			t.Fatalf("AssertInvariants() = %v, want a Term Monotonicity violation", err)
		}
	})
	t.Run("vote safety", func(t *testing.T) {
		c, fa, _ := newScripted(t)
		fa.status = raft.Status{Term: 1, VotedFor: "b"}
		poke(c)
		fa.status = raft.Status{Term: 1, VotedFor: "a"}
		poke(c)
		if err := c.AssertInvariants(); err == nil || !strings.Contains(err.Error(), "Vote Safety") {
			t.Fatalf("AssertInvariants() = %v, want a Vote Safety violation", err)
		}
	})
	t.Run("vote safety across a cleared vote", func(t *testing.T) {
		// The vote is compared against the first vote observed in the term,
		// not the previous observation: a faulty core that clears votedFor
		// without changing the term and then votes for someone else must
		// still be caught.
		c, fa, _ := newScripted(t)
		fa.status = raft.Status{Term: 1, VotedFor: "b"}
		poke(c)
		fa.status = raft.Status{Term: 1, VotedFor: raft.None}
		poke(c)
		fa.status = raft.Status{Term: 1, VotedFor: "a"}
		poke(c)
		if err := c.AssertInvariants(); err == nil || !strings.Contains(err.Error(), "Vote Safety") {
			t.Fatalf("AssertInvariants() = %v, want a Vote Safety violation", err)
		}
	})
	t.Run("vote safety allows a new vote in a new term", func(t *testing.T) {
		c, fa, _ := newScripted(t)
		fa.status = raft.Status{Term: 1, VotedFor: "b"}
		poke(c)
		fa.status = raft.Status{Term: 2, VotedFor: raft.None}
		poke(c)
		fa.status = raft.Status{Term: 2, VotedFor: "a"}
		poke(c)
		fa.status = raft.Status{Term: 2, VotedFor: "a"} // the same vote again is fine
		poke(c)
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("AssertInvariants() = %v, want nil", err)
		}
	})
	t.Run("assert checks current state", func(t *testing.T) {
		c, fa, fb := newScripted(t)
		fa.status = raft.Status{Role: raft.Leader, Term: 1}
		fb.status = raft.Status{Role: raft.Leader, Term: 1}
		// No core input has run, so only AssertInvariants itself can see it.
		if err := c.AssertInvariants(); err == nil {
			t.Fatal("AssertInvariants() must check the current state, not only past observations")
		}
	})
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
	// Latency is validated here, as an error, rather than left to NewNetwork's
	// panic: every invalid Config must come back through the error path.
	if _, err := NewCluster(Config{NodeIDs: []raft.NodeID{"a"}, MinLatency: -time.Millisecond}); err == nil {
		t.Fatal("negative MinLatency accepted")
	}
	if _, err := NewCluster(Config{NodeIDs: []raft.NodeID{"a"}, MinLatency: 5 * time.Millisecond, MaxLatency: time.Millisecond}); err == nil {
		t.Fatal("MaxLatency < MinLatency accepted")
	}
}

func TestClusterAddNodeRejectsDuplicateNilAndNone(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a"}})
	assertPanics(t, "AddNode(a) twice", func() { c.AddNode("a", &fakeCore{id: "a"}) })
	assertPanics(t, "AddNode(b, nil)", func() { c.AddNode("b", nil) })
	assertPanics(t, "AddNode(None)", func() { c.AddNode(raft.None, &fakeCore{id: raft.None}) })
	if c.Node("b") != nil || c.Node(raft.None) != nil || len(c.Nodes()) != 1 {
		t.Fatal("a rejected AddNode must not register the node")
	}
	// The network must not have gained a handler either: a message to the
	// empty address is still dropped as no-handler.
	c.Network().Send(vote("a", raft.None, 1))
	c.Run(time.Second)
	if !timelineHas(c, "reason=no-handler") {
		t.Fatal("a message to raft.None must be dropped as no-handler")
	}
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

// TestClusterRunBoundaries: Run(until) is inclusive, so Run(Now()) fires
// events due right now (After(0) timers, zero-latency deliveries); a past
// until is a no-op even when until-Now() would wrap positive.
func TestClusterRunBoundaries(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1})
	fa := &fakeCore{id: "a"}
	a := c.AddNode("a", fa)

	c.Run(time.Millisecond)
	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 0}})
	c.Run(c.Now())
	if fa.electionTimeouts != 1 {
		t.Fatalf("Run(Now()) fired %d zero-delay timers, want 1", fa.electionTimeouts)
	}

	a.Execute([]raft.Action{raft.ResetElectionTimer{Timeout: 10 * time.Millisecond}})
	for _, past := range []time.Duration{0, time.Millisecond - 1, math.MinInt64} {
		c.Run(past)
		if c.Now() != time.Millisecond || fa.electionTimeouts != 1 {
			t.Fatalf("Run(%v): now=%v timeouts=%d; want a no-op at 1ms", past, c.Now(), fa.electionTimeouts)
		}
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

// deliveryOrder returns the deliver events of a timeline with the timestamp
// removed, so two runs compare by the order messages arrived, not by when.
func deliveryOrder(timeline []string) []string {
	var out []string
	for _, line := range timeline {
		if strings.Contains(line, " event=deliver ") {
			_, rest, _ := strings.Cut(line, " ")
			out = append(out, rest)
		}
	}
	return out
}

// TestDeterministicReplay: the same seed yields a byte-identical timeline.
// A different seed yields a different delivery order — that is not a
// guarantee in general (two seeds could draw the same latencies), but it
// holds for the seeds used here and documents that the seed is what varies
// a run. The order check is separate from the timeline check because
// latencies alone can make timelines differ while messages still arrive in
// the same order.
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
	firstOrder, otherOrder := deliveryOrder(first), deliveryOrder(other)
	if len(firstOrder) < 10 {
		t.Fatalf("too few deliveries to compare order: %d", len(firstOrder))
	}
	if slices.Equal(firstOrder, otherOrder) {
		t.Fatalf("seeds 1 and 2 delivered messages in the same order:\n%s", strings.Join(firstOrder, "\n"))
	}
}
