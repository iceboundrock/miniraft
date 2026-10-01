package simulator

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Commit and apply tests (issue #8): a SET succeeds only once the Leader
// has committed and applied it, every replica then applies the same
// entries and ends in the same state, a Leader without a majority commits
// nothing, and a proposal replaced by a later Leader's entry is reported as
// lost. The commit rules themselves (Figure 8, the follower's bound) are
// unit-tested in the raft package.

// allApplied reports whether every node has applied at least index.
func allApplied(c *Cluster, index raft.Index) func() bool {
	return func() bool {
		for _, n := range c.Nodes() {
			if n.Status().LastApplied < index {
				return false
			}
		}
		return true
	}
}

// requireStateMachines runs c until every node has applied index, then
// fails unless every KV encodes exactly want and every node has the same
// apply history.
func requireStateMachines(t *testing.T, c *Cluster, index raft.Index, want string) {
	t.Helper()
	if !c.RunUntil(allApplied(c, index), c.Now()+time.Second) {
		var st []string
		for _, n := range c.Nodes() {
			st = append(st, fmt.Sprintf("%s: %+v", n.ID(), n.Status()))
		}
		t.Fatalf("not every node applied %d within 1s:\n%s", index, strings.Join(st, "\n"))
	}
	first := c.Nodes()[0]
	for id, sm := range c.StateMachines() {
		if got := string(sm.Bytes()); got != want {
			t.Fatalf("%s state = %s, want %s", id, got, want)
		}
		if !slices.EqualFunc(c.Node(id).Applied(), first.Applied(), sameApplied) {
			t.Fatalf("%s applied %v, %s applied %v", id, c.Node(id).Applied(), first.ID(), first.Applied())
		}
	}
	requireNoErrors(t, c)
}

func sameApplied(a, b raft.Applied) bool {
	return a.Index == b.Index && a.Term == b.Term && string(a.Result) == string(b.Result)
}

// TestSetCommittedAndAppliedOnAllNodes: Submit returns the SET's result
// only after the Leader has committed and applied it, and every node then
// applies the same entries, ending in byte-identical states.
func TestSetCommittedAndAppliedOnAllNodes(t *testing.T) {
	for _, ids := range [][]raft.NodeID{{"a", "b", "c"}, {"a", "b", "c", "d", "e"}} {
		t.Run(fmt.Sprintf("%d nodes", len(ids)), func(t *testing.T) {
			c := newTestCluster(t, Config{Seed: 1, NodeIDs: ids})
			leader := electLeader(t, c)
			res, err := c.Submit(setCmd("SET x 1"))
			if err != nil || string(res) != "1" {
				t.Fatalf("Submit(SET x 1) = (%q, %v), want (1, nil)", res, err)
			}
			// Success means committed and applied on the Leader, now.
			if st := leader.Status(); st.CommitIndex < 1 || st.LastApplied < 1 {
				t.Fatalf("Submit succeeded with Leader at commitIndex %d, lastApplied %d", st.CommitIndex, st.LastApplied)
			}
			if v, ok := leader.StateMachine.Get("x"); !ok || v != "1" {
				t.Fatalf("Leader's Get(x) = (%q, %v) after a successful Submit", v, ok)
			}
			if !timelineHas(c, fmt.Sprintf("event=CommitAdvance node=%s term=%d role=Leader commitIndex=1", leader.ID(), leader.Status().Term)) ||
				!timelineHas(c, fmt.Sprintf("event=Apply node=%s term=%d role=Leader index=1 lastApplied=1", leader.ID(), leader.Status().Term)) {
				t.Fatal("timeline missing the Leader's CommitAdvance / Apply events")
			}

			for _, cmd := range []string{"SET y 2", "SET x 3"} {
				if _, err := c.Submit(setCmd(cmd)); err != nil {
					t.Fatalf("Submit(%s): %v", cmd, err)
				}
			}
			requireStateMachines(t, c, 3, `{"x":"3","y":"2"}`)
		})
	}
}

// TestSubmitOnSingleNode: a single node is its own majority, so Submit's
// proposal is committed and applied inside Propose and Submit returns
// without running the simulation.
func TestSubmitOnSingleNode(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a"}})
	if _, err := c.Submit(setCmd("SET x 1")); err == nil {
		t.Fatal("Submit succeeded with no Leader")
	}
	electLeader(t, c)
	now := c.Now()
	res, err := c.Submit(setCmd("SET x 1"))
	if err != nil || string(res) != "1" || c.Now() != now {
		t.Fatalf("Submit = (%q, %v) after %v, want (1, nil) at once", res, err, c.Now()-now)
	}
	requireStateMachines(t, c, 1, `{"x":"1"}`)
}

// TestMinorityLeaderCannotCommit: a Leader cut off from both peers still
// appends a proposal to its own log, but its commitIndex never covers it,
// nothing applies it, and the client gets ErrSubmitTimeout, not success.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	if _, err := c.Submit(setCmd("SET x 1")); err != nil {
		t.Fatalf("Submit(SET x 1) on a healthy cluster: %v", err)
	}
	requireStateMachines(t, c, 1, `{"x":"1"}`)

	c.Network().Isolate(leader.ID())
	res, err := c.Submit(setCmd("SET y 2"))
	if !errors.Is(err, ErrSubmitTimeout) {
		t.Fatalf("Submit on an isolated Leader = (%q, %v), want ErrSubmitTimeout", res, err)
	}
	requireLogEquals(t, leader, "SET x 1", "SET y 2") // appended locally
	if st := leader.Status(); st.CommitIndex != 1 || st.LastApplied != 1 {
		t.Fatalf("isolated Leader commitIndex %d, lastApplied %d, want 1 and 1", st.CommitIndex, st.LastApplied)
	}
	for _, n := range c.Nodes() {
		if _, ok := n.StateMachine.Get("y"); ok {
			t.Fatalf("%s applied the uncommitted SET y", n.ID())
		}
	}
	requireNoErrors(t, c)
}

// TestProposalLostOnLeaderChange: a Leader appends a proposal and is cut
// off before replicating it; the majority elects a new Leader, which
// commits a different entry at the same index. When the old Leader
// rejoins, it replaces its uncommitted entry and applies the new one, and
// waiting on the original proposal yields ErrProposalLost, not success.
func TestProposalLostOnLeaderChange(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	old := electLeader(t, c)
	if _, err := c.Submit(setCmd("SET x 1")); err != nil {
		t.Fatalf("Submit(SET x 1): %v", err)
	}

	c.Network().Isolate(old.ID())
	idx, term, err := old.Propose(setCmd("SET y lost"))
	if err != nil || idx != 2 {
		t.Fatalf("Propose on the old Leader = (%d, %d, %v), want index 2", idx, term, err)
	}
	newLeader := func() bool { l := c.Leader(); return l != nil && l != old }
	if !c.RunUntil(newLeader, c.Now()+2*time.Second) {
		t.Fatalf("the majority elected no new Leader: roles=%v", c.Roles())
	}
	if res, err := c.Submit(setCmd("SET y won")); err != nil || string(res) != "won" {
		t.Fatalf("Submit on the new Leader = (%q, %v), want (won, nil)", res, err)
	}

	c.Network().Heal()
	res, err := c.Await(old, idx, term)
	if !errors.Is(err, raft.ErrProposalLost) {
		t.Fatalf("Await(old proposal) = (%q, %v), want ErrProposalLost", res, err)
	}
	requireStateMachines(t, c, 2, `{"x":"1","y":"won"}`)
}

// TestCommitInvariantChecker: the commit and apply invariants catch the
// violations they are meant to, using scripted cores whose Status, Storage
// and Applied actions are set by hand. Clusters are built with NewCluster
// directly because newTestCluster fails the test on any violation.
func TestCommitInvariantChecker(t *testing.T) {
	newScripted := func(t *testing.T, withStorage bool) (*Cluster, *fakeCore, *fakeCore) {
		t.Helper()
		c, err := NewCluster(Config{Seed: 1})
		if err != nil {
			t.Fatal(err)
		}
		fa, fb := &fakeCore{id: "a"}, &fakeCore{id: "b"}
		a, b := c.AddNode("a", fa), c.AddNode("b", fb)
		if withStorage {
			a.Storage, b.Storage = storage.NewMemoryStorage(), storage.NewMemoryStorage()
		}
		return c, fa, fb
	}
	poke := func(c *Cluster) { c.Node("a").HandleMessage(vote("b", "a", 1)) }
	entry := func(i, term uint64, cmd string) raft.LogEntry {
		return raft.LogEntry{Index: raft.Index(i), Term: raft.Term(term), Command: []byte(cmd)}
	}
	// node sets n's log and returns a Status consistent with it.
	node := func(t *testing.T, c *Cluster, id raft.NodeID, role raft.Role, term raft.Term, commit raft.Index, log ...raft.LogEntry) raft.Status {
		t.Helper()
		st := c.Node(id).Storage
		if err := st.TruncateSuffix(1); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendEntries(log); err != nil {
			t.Fatal(err)
		}
		s := raft.Status{Role: role, Term: term, CommitIndex: commit}
		if len(log) != 0 {
			s.LastLogIndex, s.LastLogTerm = log[len(log)-1].Index, log[len(log)-1].Term
		}
		return s
	}
	applied := func(c *Cluster, id raft.NodeID, index raft.Index, term raft.Term, result string) {
		c.Node(id).Execute([]raft.Action{raft.Applied{Index: index, Term: term, Result: []byte(result)}})
	}
	requireViolation := func(t *testing.T, c *Cluster, invariant string) {
		t.Helper()
		if err := c.AssertInvariants(); err == nil || !strings.Contains(err.Error(), invariant) {
			t.Fatalf("AssertInvariants() = %v, want a %s violation", err, invariant)
		}
	}

	t.Run("clean", func(t *testing.T) {
		c, fa, fb := newScripted(t, true)
		x, y := entry(1, 1, "x"), entry(2, 2, "y")
		// b leads term 2 and has committed y; a is a deposed Leader of term 1
		// that has not heard of term 2 and lacks y, which is legal.
		fa.status = node(t, c, "a", raft.Leader, 1, 1, x)
		fb.status = node(t, c, "b", raft.Leader, 2, 2, x, y)
		fa.status.LastApplied, fb.status.LastApplied = 1, 2
		applied(c, "a", 1, 1, "x")
		applied(c, "b", 1, 1, "x")
		applied(c, "b", 2, 2, "y")
		poke(c)
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("AssertInvariants() = %v, want nil", err)
		}
	})
	t.Run("monotonic progress: commitIndex", func(t *testing.T) {
		c, fa, _ := newScripted(t, false)
		fa.status = raft.Status{Term: 1, CommitIndex: 2, LastLogIndex: 2}
		poke(c)
		fa.status = raft.Status{Term: 1, CommitIndex: 1, LastLogIndex: 2}
		poke(c)
		requireViolation(t, c, "Monotonic Progress")
	})
	t.Run("monotonic progress: lastApplied", func(t *testing.T) {
		c, fa, _ := newScripted(t, false)
		fa.status = raft.Status{Term: 1, CommitIndex: 2, LastApplied: 2, LastLogIndex: 2}
		poke(c)
		fa.status = raft.Status{Term: 1, CommitIndex: 2, LastApplied: 1, LastLogIndex: 2}
		poke(c)
		requireViolation(t, c, "Monotonic Progress")
	})
	t.Run("bounds: lastApplied beyond commitIndex", func(t *testing.T) {
		c, fa, _ := newScripted(t, false)
		fa.status = raft.Status{Term: 1, CommitIndex: 1, LastApplied: 2, LastLogIndex: 2}
		poke(c)
		requireViolation(t, c, "Apply/Commit Bounds")
	})
	t.Run("bounds: commitIndex beyond the log", func(t *testing.T) {
		c, fa, _ := newScripted(t, false)
		fa.status = raft.Status{Term: 1, CommitIndex: 3, LastLogIndex: 2}
		poke(c)
		requireViolation(t, c, "Apply/Commit Bounds")
	})
	t.Run("apply order", func(t *testing.T) {
		c, _, _ := newScripted(t, false)
		applied(c, "a", 1, 1, "x")
		applied(c, "a", 3, 1, "z") // skips 2
		poke(c)
		requireViolation(t, c, "Apply Order")
	})
	t.Run("state machine safety: different entries applied at one index", func(t *testing.T) {
		c, _, _ := newScripted(t, false)
		applied(c, "a", 1, 1, "x")
		applied(c, "b", 1, 2, "x")
		poke(c)
		requireViolation(t, c, "State Machine Safety")
	})
	t.Run("state machine safety: different results at one index", func(t *testing.T) {
		c, _, _ := newScripted(t, false)
		applied(c, "a", 1, 1, "x")
		poke(c)
		applied(c, "b", 1, 1, "y") // observed in a later round
		poke(c)
		requireViolation(t, c, "State Machine Safety")
	})
	t.Run("state machine safety: different entries committed at one index", func(t *testing.T) {
		c, fa, fb := newScripted(t, true)
		// Different terms at index 1, so Log Matching has nothing to say.
		fa.status = node(t, c, "a", raft.Follower, 2, 1, entry(1, 1, "x"))
		fb.status = node(t, c, "b", raft.Follower, 2, 1, entry(1, 2, "y"))
		poke(c)
		requireViolation(t, c, "State Machine Safety")
	})
	t.Run("leader completeness", func(t *testing.T) {
		c, fa, fb := newScripted(t, true)
		fb.status = node(t, c, "b", raft.Follower, 1, 1, entry(1, 1, "x"))
		poke(c)
		// A Leader of a later term without the entry committed in term 1.
		fa.status = node(t, c, "a", raft.Leader, 2, 0)
		poke(c)
		requireViolation(t, c, "Leader Completeness")
	})
}
