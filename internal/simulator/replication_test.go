package simulator

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Log replication tests (issue #7): proposals reach every follower, a new
// Leader walks nextIndex back to the match point and repairs divergent
// suffixes, duplicates are harmless, and the Log Matching / Leader
// Append-Only invariants hold throughout.

// requireConverged runs c until every node connected to the Leader holds
// the Leader's log (see Cluster.LogsConverged) and returns the Leader. Every
// caller runs it on a healed network, where that means every node.
func requireConverged(t *testing.T, c *Cluster, within time.Duration) *SimNode {
	t.Helper()
	if !c.RunUntil(c.LogsConverged, within) {
		var logs []string
		for _, n := range c.Nodes() {
			logs = append(logs, fmt.Sprintf("%s: %v", n.ID(), terms(n.Log())))
		}
		t.Fatalf("logs did not converge within %v:\n%s", within, strings.Join(logs, "\n"))
	}
	requireNoErrors(t, c)
	return c.Leader()
}

// terms returns the term of each entry, the compact notation of Figure 7.
func terms(log []raft.LogEntry) []raft.Term {
	out := make([]raft.Term, len(log))
	for i, e := range log {
		out[i] = e.Term
	}
	return out
}

// requireLogEquals fails unless n's log has exactly the given commands, in
// order.
func requireLogEquals(t *testing.T, n *SimNode, want ...string) {
	t.Helper()
	log := n.Log()
	if len(log) != len(want) {
		t.Fatalf("%s log has %d entries, want %d", n.ID(), len(log), len(want))
	}
	for i, e := range log {
		if e.Index != raft.Index(i+1) || string(e.Command) != want[i] {
			t.Fatalf("%s log[%d] = %+v, want index %d command %q", n.ID(), i, e, i+1, want[i])
		}
	}
}

// proposeAll proposes each command through the cluster, failing on error.
func proposeAll(t *testing.T, c *Cluster, cmds ...string) {
	t.Helper()
	for _, cmd := range cmds {
		if _, err := c.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
	}
}

func TestProposeReplicatesToFollowers(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	idx, err := c.Propose([]byte("SET x 1"))
	if err != nil || idx != 1 {
		t.Fatalf("Propose = (%d, %v), want index 1", idx, err)
	}
	// The proposal is sent at once, not on the next heartbeat: one latency
	// to the follower is enough for it to hold the entry.
	c.RunFor(DefaultLatency)
	for _, n := range c.Nodes() {
		requireLogEquals(t, n, "SET x 1")
	}
	// One round trip and the Leader knows.
	c.RunFor(DefaultLatency)
	for _, id := range []raft.NodeID{"a", "b", "c"} {
		if id == leader.ID() {
			continue
		}
		if !timelineHas(c, fmt.Sprintf("event=MatchIndexAdvance node=%s term=%d role=Leader peer=%s matchIndex=1",
			leader.ID(), leader.Status().Term, id)) {
			t.Fatalf("timeline missing MatchIndexAdvance for %s", id)
		}
	}
	proposeAll(t, c, "SET y 2", "SET z 3")
	requireConverged(t, c, time.Second)
	for _, n := range c.Nodes() {
		requireLogEquals(t, n, "SET x 1", "SET y 2", "SET z 3")
	}
	if !timelineHas(c, "event=Propose node="+string(leader.ID())) {
		t.Fatal("timeline missing Propose")
	}
}

// TestFollowerCatchUp: a follower cut off while the Leader accepts
// proposals gets everything it missed with the first AppendEntries after
// the link heals, because the Leader's nextIndex for it never moved past
// what it acknowledged.
func TestFollowerCatchUp(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	proposeAll(t, c, "SET a 1")
	requireConverged(t, c, time.Second)

	var follower *SimNode
	for _, n := range c.Nodes() {
		if n != leader {
			follower = n
			break
		}
	}
	c.Network().Isolate(follower.ID())
	proposeAll(t, c, "SET b 2", "SET c 3", "SET d 4")
	c.RunFor(DefaultHeartbeatInterval) // well inside the follower's election timeout
	requireLogEquals(t, follower, "SET a 1")
	c.Network().Heal()

	requireConverged(t, c, time.Second)
	requireLogEquals(t, follower, "SET a 1", "SET b 2", "SET c 3", "SET d 4")
	if countTimeline(c, "event=BecameLeader") != 1 {
		t.Fatal("catch-up must not involve a new election")
	}
	if countTimeline(c, "event=TruncateSuffix") != 0 {
		t.Fatal("catch-up must not truncate anything")
	}
}

// sendsTo extracts, in timeline order, the prevIndex of every
// AppendEntriesSend from leader to peer up to the first successful ack
// from that peer.
func sendsTo(c *Cluster, leader, peer raft.NodeID) (prevIndexes []int, acked bool) {
	send := regexp.MustCompile(fmt.Sprintf(`event=AppendEntriesSend node=%s .*peer=%s prevIndex=(\d+)`, leader, peer))
	ack := fmt.Sprintf("event=AppendEntriesAck node=%s ", leader)
	for _, line := range c.Timeline() {
		if m := send.FindStringSubmatch(line); m != nil {
			v, _ := strconv.Atoi(m[1])
			prevIndexes = append(prevIndexes, v)
		}
		if strings.Contains(line, ack) && strings.Contains(line, "peer="+string(peer)+" success=true") {
			return prevIndexes, true
		}
	}
	return prevIndexes, false
}

// TestNextIndexRollback: a new Leader optimistically assumes every follower
// has its whole log (nextIndex = lastIndex+1). Followers that are k entries
// behind reject k probes; each rejection walks prevIndex back by exactly
// one until the match point, after which one AppendEntries carries the
// whole missing suffix.
func TestNextIndexRollback(t *testing.T) {
	const k = 5
	var pairs [][2]uint64
	for i := uint64(1); i <= k; i++ {
		pairs = append(pairs, [2]uint64{i, 1})
	}
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
		// Fixed 1ms latency so that the whole walk-back fits between two
		// heartbeats and no heartbeat interleaves with the probes.
		MinLatency: time.Millisecond, MaxLatency: time.Millisecond,
		Stores: map[raft.NodeID]*storage.MemoryStorage{"a": preloaded(t, 1, raft.None, pairs...)}})
	c.Node("a").ForceElectionTimeout()
	leader := requireConverged(t, c, time.Second)
	if leader.ID() != "a" {
		t.Fatalf("Leader is %s, want a", leader.ID())
	}
	c.RunFor(2 * time.Millisecond) // the final acks reach the Leader
	for _, peer := range []raft.NodeID{"b", "c"} {
		got, acked := sendsTo(c, "a", peer)
		want := []int{k, k - 1, k - 2, k - 3, k - 4, 0}
		if !acked || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("prevIndex sequence to %s = %v (acked=%v), want strictly decreasing %v then success", peer, got, acked, want)
		}
		if countTimeline(c, "event=AppendEntriesReject node="+string(peer)) != k {
			t.Fatalf("%s rejected %d probes, want %d", peer, countTimeline(c, "event=AppendEntriesReject node="+string(peer)), k)
		}
		if got := terms(c.Node(peer).Log()); len(got) != k {
			t.Fatalf("%s log terms = %v, want %d entries", peer, got, k)
		}
	}
}

// TestDivergentSuffixRepair: a follower holding uncommitted entries from an
// old term at indexes >= m has that suffix deleted and replaced by the
// Leader's, and continues to replicate normally afterwards.
func TestDivergentSuffixRepair(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
		Stores: map[raft.NodeID]*storage.MemoryStorage{
			"a": preloaded(t, 2, raft.None, [2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2}, [2]uint64{4, 2}),
			"b": preloaded(t, 2, raft.None, [2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1}, [2]uint64{4, 1}, [2]uint64{5, 1}),
		}})
	c.Node("a").ForceElectionTimeout()
	leader := requireConverged(t, c, time.Second)
	if leader.ID() != "a" {
		t.Fatalf("Leader is %s, want a", leader.ID())
	}
	want := []raft.Term{1, 1, 2, 2}
	for _, n := range c.Nodes() {
		if got := terms(n.Log()); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s log terms = %v, want %v", n.ID(), got, want)
		}
	}
	if !timelineHas(c, "event=TruncateSuffix node=b term=3 role=Follower index=3 lastIndex=5") {
		t.Fatal("timeline missing b's TruncateSuffix at index 3")
	}
	if countTimeline(c, "event=TruncateSuffix") != 1 {
		t.Fatalf("timeline has %d TruncateSuffix events, want exactly 1", countTimeline(c, "event=TruncateSuffix"))
	}

	proposeAll(t, c, "SET x 1")
	requireConverged(t, c, time.Second)
	for _, n := range c.Nodes() {
		if got := terms(n.Log()); fmt.Sprint(got) != fmt.Sprint([]raft.Term{1, 1, 2, 2, 3}) {
			t.Fatalf("%s log terms after Propose = %v", n.ID(), got)
		}
	}
}

// TestDuplicateAppendEntriesIsIdempotent: with every message between the
// Leader and a follower delivered twice, logs still converge, nothing is
// ever truncated (a duplicate of an older batch must not delete what a
// newer one appended) and matchIndex never regresses.
func TestDuplicateAppendEntriesIsIdempotent(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 3, NodeIDs: []raft.NodeID{"a", "b", "c"},
		MinLatency: time.Millisecond, MaxLatency: 20 * time.Millisecond})
	leader := electLeader(t, c)
	for _, n := range c.Nodes() {
		if n != leader {
			c.Network().SetDuplicate(leader.ID(), n.ID(), true)
			c.Network().SetDuplicate(n.ID(), leader.ID(), true)
		}
	}
	proposeAll(t, c, "SET a 1", "SET b 2", "SET c 3")
	c.RunFor(2 * DefaultHeartbeatInterval)
	proposeAll(t, c, "SET d 4", "SET e 5")
	requireConverged(t, c, time.Second)
	for _, n := range c.Nodes() {
		requireLogEquals(t, n, "SET a 1", "SET b 2", "SET c 3", "SET d 4", "SET e 5")
	}
	if countTimeline(c, "duplicate=true") == 0 {
		t.Fatal("no message was duplicated; the test exercised nothing")
	}
	if countTimeline(c, "event=TruncateSuffix") != 0 {
		t.Fatal("duplicates caused a truncation")
	}
	if countTimeline(c, "event=BecameLeader") != 1 {
		t.Fatal("duplicates caused a re-election")
	}
}

func TestProposeOnFollowerReturnsErrNotLeader(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	if _, err := c.Propose([]byte("SET x 1")); err == nil {
		t.Fatal("Cluster.Propose succeeded with no Leader")
	}
	leader := electLeader(t, c)
	c.RunFor(2 * DefaultHeartbeatInterval) // followers know who leads
	for _, n := range c.Nodes() {
		if n == leader {
			continue
		}
		_, err := n.Propose([]byte("SET x 1"))
		var nl *raft.NotLeaderError
		if !errors.Is(err, raft.ErrNotLeader) || !errors.As(err, &nl) || nl.LeaderID != leader.ID() {
			t.Fatalf("%s.Propose = %v, want NotLeaderError naming %s", n.ID(), err, leader.ID())
		}
		requireLogEquals(t, n)
	}
	// A rejected proposal is a client-visible outcome, not a core error.
	requireNoErrors(t, c)
}

// TestFigure7Scenarios replays Figure 7 of the paper: a Leader with the
// reference log in term 8 faces each of the follower logs (a)-(f) in turn
// and repairs it. Each case is a 3-node cluster of the Leader, the
// Figure 7 follower and a follower with an empty log (which grants the vote
// that (c) and (d) deny, since their last terms are ahead of the Leader's).
//
// The Leader proposes one entry in term 8 first, as the paper's Leader
// does: with the plain Figure 2 receiver rule an AppendEntries that carries
// no conflicting entry never deletes anything, so the extra entries of (c)
// and (d) survive until the Leader's next entry at index 11 conflicts with
// them.
func TestFigure7Scenarios(t *testing.T) {
	leaderLog := []raft.Term{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	want := append(slices.Clone(leaderLog), 8)
	cases := []struct {
		name     string
		follower []raft.Term
		truncate int // TruncateSuffix events expected on the follower
	}{
		{"a: missing one", []raft.Term{1, 1, 1, 4, 4, 5, 5, 6, 6}, 0},
		{"b: missing many", []raft.Term{1, 1, 1, 4}, 0},
		{"c: extra entry, same term", []raft.Term{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}, 1},
		{"d: extra entries, later term", []raft.Term{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}, 1},
		{"e: divergent old term", []raft.Term{1, 1, 1, 4, 4, 4, 4}, 1},
		{"f: long divergent suffix", []raft.Term{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}, 1},
	}
	pairs := func(terms []raft.Term) [][2]uint64 {
		var out [][2]uint64
		for i, term := range terms {
			out = append(out, [2]uint64{uint64(i + 1), uint64(term)})
		}
		return out
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
				Stores: map[raft.NodeID]*storage.MemoryStorage{
					"a": preloaded(t, 7, raft.None, pairs(leaderLog)...),
					"b": preloaded(t, 7, raft.None, pairs(tc.follower)...),
					"c": preloaded(t, 7, raft.None),
				}})
			c.Node("a").ForceElectionTimeout()
			if !c.RunUntil(hasLeader(c), time.Second) {
				t.Fatal("no Leader elected")
			}
			leader := c.Leader()
			if leader.ID() != "a" || leader.Status().Term != 8 {
				t.Fatalf("Leader = %s in term %d, want a in term 8", leader.ID(), leader.Status().Term)
			}
			if idx, err := c.Propose([]byte("SET x 1")); err != nil || idx != 11 {
				t.Fatalf("Propose = (%d, %v), want index 11", idx, err)
			}
			requireConverged(t, c, 2*time.Second)
			for _, n := range c.Nodes() {
				if got := terms(n.Log()); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("%s log terms = %v, want %v", n.ID(), got, want)
				}
			}
			if got := countTimeline(c, "event=TruncateSuffix node=b "); got != tc.truncate {
				t.Fatalf("b truncated %d times, want %d", got, tc.truncate)
			}
			// The Leader's own log only ever grew.
			if countTimeline(c, "event=TruncateSuffix node=a ") != 0 {
				t.Fatal("the Leader truncated its log")
			}
		})
	}
}

// TestLogsConverged: false without a Leader, false while a connected
// follower's log differs, and it ignores a node the Leader cannot reach:
// once every connected node agrees it is true even though the isolated
// node is behind, and it drops back to false the moment that node is
// reconnected and counted again.
func TestLogsConverged(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	if c.LogsConverged() {
		t.Fatal("LogsConverged with no Leader")
	}
	leader := electLeader(t, c)
	if !c.LogsConverged() {
		t.Fatal("LogsConverged is false with all logs empty")
	}
	var other *SimNode
	for _, n := range c.Nodes() {
		if n != leader {
			other = n
		}
	}
	c.Network().Isolate(other.ID())
	proposeAll(t, c, "SET x 1")
	if c.LogsConverged() {
		t.Fatal("LogsConverged right after Propose, before any delivery")
	}
	c.RunFor(2 * DefaultLatency)
	if !c.LogsConverged() {
		t.Fatal("LogsConverged is false although every connected node holds the entry")
	}
	requireLogEquals(t, other)
	c.Network().Heal()
	if c.LogsConverged() {
		t.Fatal("LogsConverged right after Heal, while the reconnected node is still behind")
	}
	requireConverged(t, c, time.Second)
	requireLogEquals(t, other, "SET x 1")
}

// TestLogsConvergedIgnoresIsolatedDivergentNode: the partition-side use of
// the predicate from issue #7. Every node on the Leader's side of the
// partition agrees with it, so RunUntil(LogsConverged) returns while the
// isolated node still holds its divergent suffix (Figure 7 (f)); the node
// is repaired only after the network heals.
func TestLogsConvergedIgnoresIsolatedDivergentNode(t *testing.T) {
	reference := []raft.Term{1, 1, 1, 4, 4, 5, 5, 6, 6}
	divergent := []raft.Term{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}
	pairs := func(terms []raft.Term) [][2]uint64 {
		var out [][2]uint64
		for i, term := range terms {
			out = append(out, [2]uint64{uint64(i + 1), uint64(term)})
		}
		return out
	}
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"},
		Stores: map[raft.NodeID]*storage.MemoryStorage{
			"a": preloaded(t, 7, raft.None, pairs(reference)...),
			"b": preloaded(t, 7, raft.None, pairs(divergent)...),
			"c": preloaded(t, 7, raft.None, pairs(reference)...),
		}})
	c.Network().Isolate("b")
	c.Node("a").ForceElectionTimeout()
	if !c.RunUntil(hasLeader(c), time.Second) {
		t.Fatal("no Leader elected")
	}
	if leader := c.Leader(); leader.ID() != "a" {
		t.Fatalf("Leader = %s, want a", leader.ID())
	}
	proposeAll(t, c, "SET x 1")
	if !c.RunUntil(c.LogsConverged, time.Second) {
		t.Fatal("the Leader's side of the partition did not converge")
	}
	if got := terms(c.Node("b").Log()); fmt.Sprint(got) != fmt.Sprint(divergent) {
		t.Fatalf("isolated b log terms = %v, want its divergent log %v untouched", got, divergent)
	}
	want := append(slices.Clone(reference), 8)
	for _, id := range []raft.NodeID{"a", "c"} {
		if got := terms(c.Node(id).Log()); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s log terms = %v, want %v", id, got, want)
		}
	}
	c.Network().Heal()
	if c.LogsConverged() {
		t.Fatal("LogsConverged right after Heal, with b still divergent")
	}
	requireConverged(t, c, 2*time.Second)
	if got := terms(c.Node("b").Log()); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("b log terms after heal = %v, want %v", got, want)
	}
	if got := countTimeline(c, "event=TruncateSuffix node=b "); got != 1 {
		t.Fatalf("b truncated %d times, want 1", got)
	}
}

// TestLogsConvergedIsolatedLeader: a Leader cut off from every follower is
// not "converged", so RunUntil(LogsConverged) keeps running instead of
// returning before anything was replicated. Ignoring unreachable nodes
// alone would make this vacuously true (nothing compared, nothing
// differs); the predicate needs at least one follower it can observe.
func TestLogsConvergedIsolatedLeader(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	c.Network().Isolate(leader.ID())
	proposeAll(t, c, "SET x 1")
	if c.LogsConverged() {
		t.Fatal("LogsConverged with the Leader isolated from every follower")
	}
	// Well inside the followers' election timeout, so the isolated node is
	// still the only Leader and its entry has reached nobody. RunUntil takes
	// an absolute deadline.
	if c.RunUntil(c.LogsConverged, c.Now()+DefaultHeartbeatInterval) {
		t.Fatal("RunUntil(LogsConverged) returned while the Leader was isolated")
	}
	for _, n := range c.Nodes() {
		if n != leader {
			requireLogEquals(t, n)
		}
	}
	c.Network().Heal()
	requireConverged(t, c, time.Second)
	requireLogEquals(t, leader, "SET x 1")
}

// TestLogsConvergedMinorityComponent: the predicate is about replication,
// not commit, so a Leader's side of a partition need not be a quorum. Five
// nodes, the Leader and one follower cut off from the other three: once
// that follower holds the Leader's log, every connected node agrees and
// LogsConverged is true even though the pair can never commit. The three
// others are still empty, and reconnecting them makes it false again until
// they catch up.
func TestLogsConvergedMinorityComponent(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c", "d", "e"}})
	leader := electLeader(t, c)
	var minority, majority []raft.NodeID
	for _, n := range c.Nodes() {
		if n == leader || (len(minority) == 0 && n != leader) {
			minority = append(minority, n.ID())
		} else {
			majority = append(majority, n.ID())
		}
	}
	c.Network().Partition([][]raft.NodeID{minority, majority})
	proposeAll(t, c, "SET x 1")
	if c.LogsConverged() {
		t.Fatal("LogsConverged right after Propose, before any delivery")
	}
	// Inside the majority side's election timeout: the partitioned Leader
	// is still the only Leader, and its one reachable follower is enough.
	// RunUntil takes an absolute deadline.
	if !c.RunUntil(c.LogsConverged, c.Now()+DefaultHeartbeatInterval) {
		t.Fatal("RunUntil(LogsConverged) did not return although every connected node held the entry")
	}
	for _, id := range minority {
		requireLogEquals(t, c.Node(id), "SET x 1")
	}
	for _, id := range majority {
		requireLogEquals(t, c.Node(id))
	}
	c.Network().Heal()
	if c.LogsConverged() {
		t.Fatal("LogsConverged right after Heal, while the reconnected nodes are still behind")
	}
	requireConverged(t, c, time.Second)
	for _, n := range c.Nodes() {
		requireLogEquals(t, n, "SET x 1")
	}
}

// TestLogsConvergedSingleNode: a one-node cluster has no follower to
// observe, and that is not the isolated-Leader case: with no peers at all
// the Leader's log is the whole cluster's log, so the predicate is true as
// soon as there is a Leader and stays true across a proposal.
func TestLogsConvergedSingleNode(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a"}})
	if c.LogsConverged() {
		t.Fatal("LogsConverged with no Leader")
	}
	if !c.RunUntil(c.LogsConverged, time.Second) {
		t.Fatal("a single node did not converge within 1s")
	}
	proposeAll(t, c, "SET x 1")
	if !c.LogsConverged() {
		t.Fatal("LogsConverged is false on a single node after Propose")
	}
	requireLogEquals(t, c.Node("a"), "SET x 1")
	requireNoErrors(t, c)
}

// TestProposeOnClosedStorage: a proposal the Leader cannot persist is
// answered with the storage error. It is a client-facing answer, so it is
// not in Errors(), and the invariant checker that runs after every input
// must cope with the unreadable store rather than panic.
func TestProposeOnClosedStorage(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	if err := leader.Storage.Close(); err != nil {
		t.Fatal(err)
	}
	idx, err := leader.Propose([]byte("SET x 1"))
	if !errors.Is(err, storage.ErrClosed) || idx != 0 {
		t.Fatalf("Propose = (%d, %v), want (0, %v)", idx, err, storage.ErrClosed)
	}
	requireNoErrors(t, c)
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
	if c.LogsConverged() {
		t.Fatal("LogsConverged with the Leader's store closed")
	}
}

// TestClosedFollowerStorageIsRecorded: a follower whose store is closed
// cannot append what the Leader sends; that is the core failing an input,
// so it lands in Errors(), while the rest of the cluster keeps going and
// the invariant checker skips the unreadable store.
func TestClosedFollowerStorageIsRecorded(t *testing.T) {
	c := newTestCluster(t, Config{Seed: 1, NodeIDs: []raft.NodeID{"a", "b", "c"}})
	leader := electLeader(t, c)
	var closed, open *SimNode
	for _, n := range c.Nodes() {
		if n == leader {
			continue
		}
		if closed == nil {
			closed = n
		} else {
			open = n
		}
	}
	if err := closed.Storage.Close(); err != nil {
		t.Fatal(err)
	}
	proposeAll(t, c, "SET x 1")
	c.RunFor(2 * DefaultLatency)
	requireLogEquals(t, open, "SET x 1")
	errs := c.Errors()
	if len(errs) == 0 || !errors.Is(errs[0], storage.ErrClosed) {
		t.Fatalf("Errors() = %v, want the closed follower's %v", errs, storage.ErrClosed)
	}
	for _, err := range errs {
		if !strings.HasPrefix(err.Error(), "node "+string(closed.ID())+":") {
			t.Fatalf("error from a node other than %s: %v", closed.ID(), err)
		}
	}
	if c.LogsConverged() {
		t.Fatalf("LogsConverged with %s's store closed", closed.ID())
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

// TestLogInvariantChecker: Log Matching and Leader Append-Only are checked
// over the nodes' storage after every core input. Scripted cores with
// hand-filled stores make the violations reproducible.
func TestLogInvariantChecker(t *testing.T) {
	newScripted := func(t *testing.T) (*Cluster, *fakeCore, *fakeCore) {
		t.Helper()
		c, err := NewCluster(Config{Seed: 1})
		if err != nil {
			t.Fatal(err)
		}
		fa, fb := &fakeCore{id: "a"}, &fakeCore{id: "b"}
		c.AddNode("a", fa).Storage = storage.NewMemoryStorage()
		c.AddNode("b", fb).Storage = storage.NewMemoryStorage()
		return c, fa, fb
	}
	poke := func(c *Cluster) { c.Node("a").HandleMessage(vote("b", "a", 1)) }
	entry := func(i uint64, term uint64, cmd string) raft.LogEntry {
		return raft.LogEntry{Index: raft.Index(i), Term: raft.Term(term), Command: []byte(cmd)}
	}
	set := func(t *testing.T, st *storage.MemoryStorage, entries ...raft.LogEntry) {
		t.Helper()
		if err := st.TruncateSuffix(1); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendEntries(entries); err != nil {
			t.Fatal(err)
		}
	}
	status := func(role raft.Role, term raft.Term, log ...raft.LogEntry) raft.Status {
		st := raft.Status{Role: role, Term: term}
		if len(log) != 0 {
			st.LastLogIndex, st.LastLogTerm = log[len(log)-1].Index, log[len(log)-1].Term
		}
		return st
	}

	t.Run("clean", func(t *testing.T) {
		c, fa, fb := newScripted(t)
		la := []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "y")}
		lb := []raft.LogEntry{entry(1, 1, "x"), entry(2, 2, "z")} // same index, different term: allowed
		set(t, c.Node("a").Storage, la...)
		set(t, c.Node("b").Storage, lb...)
		fa.status, fb.status = status(raft.Leader, 1, la...), status(raft.Follower, 1, lb...)
		poke(c)
		la = append(la, entry(3, 1, "w")) // the Leader extends its log
		set(t, c.Node("a").Storage, la...)
		fa.status = status(raft.Leader, 1, la...)
		poke(c)
		if err := c.AssertInvariants(); err != nil {
			t.Fatalf("AssertInvariants() = %v, want nil", err)
		}
	})
	t.Run("log matching: same index and term, different command", func(t *testing.T) {
		c, fa, fb := newScripted(t)
		la := []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "y")}
		lb := []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "DIFFERENT")}
		set(t, c.Node("a").Storage, la...)
		set(t, c.Node("b").Storage, lb...)
		fa.status, fb.status = status(raft.Leader, 1, la...), status(raft.Follower, 1, lb...)
		poke(c)
		err := c.AssertInvariants()
		if err == nil || !strings.Contains(err.Error(), "Log Matching") {
			t.Fatalf("AssertInvariants() = %v, want a Log Matching violation", err)
		}
	})
	t.Run("log matching: equal entry with differing prefix", func(t *testing.T) {
		c, fa, fb := newScripted(t)
		la := []raft.LogEntry{entry(1, 1, "x"), entry(2, 3, "y")}
		lb := []raft.LogEntry{entry(1, 2, "x"), entry(2, 3, "y")}
		set(t, c.Node("a").Storage, la...)
		set(t, c.Node("b").Storage, lb...)
		fa.status, fb.status = status(raft.Leader, 3, la...), status(raft.Follower, 3, lb...)
		poke(c)
		err := c.AssertInvariants()
		if err == nil || !strings.Contains(err.Error(), "Log Matching") {
			t.Fatalf("AssertInvariants() = %v, want a Log Matching violation", err)
		}
	})
	t.Run("leader append-only", func(t *testing.T) {
		c, fa, _ := newScripted(t)
		la := []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "y")}
		set(t, c.Node("a").Storage, la...)
		fa.status = status(raft.Leader, 1, la...)
		poke(c)
		la = []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "REPLACED")} // same shape, different content
		set(t, c.Node("a").Storage, la...)
		fa.status = status(raft.Leader, 1, la...)
		poke(c)
		err := c.AssertInvariants()
		if err == nil || !strings.Contains(err.Error(), "Leader Append-Only") {
			t.Fatalf("AssertInvariants() = %v, want a Leader Append-Only violation", err)
		}
	})
	t.Run("leader append-only: shrinking", func(t *testing.T) {
		c, fa, _ := newScripted(t)
		la := []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "y")}
		set(t, c.Node("a").Storage, la...)
		fa.status = status(raft.Leader, 1, la...)
		poke(c)
		set(t, c.Node("a").Storage, la[:1]...)
		fa.status = status(raft.Leader, 1, la[:1]...)
		poke(c)
		err := c.AssertInvariants()
		if err == nil || !strings.Contains(err.Error(), "Leader Append-Only") {
			t.Fatalf("AssertInvariants() = %v, want a Leader Append-Only violation", err)
		}
	})
	t.Run("storage consistency", func(t *testing.T) {
		c, fa, _ := newScripted(t)
		la := []raft.LogEntry{entry(1, 1, "x"), entry(2, 1, "y")}
		set(t, c.Node("a").Storage, la...)
		fa.status = status(raft.Follower, 1, la[:1]...) // claims one entry, storage holds two
		poke(c)
		err := c.AssertInvariants()
		if err == nil || !strings.Contains(err.Error(), "Storage Consistency") {
			t.Fatalf("AssertInvariants() = %v, want a Storage Consistency violation", err)
		}
	})
}
