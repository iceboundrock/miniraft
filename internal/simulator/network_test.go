package simulator

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// received is one delivery observed by a recorder.
type received struct {
	at  time.Duration
	msg raft.Message
}

// recorder is a Handler that records every delivery with the fake time.
type recorder struct {
	clock *Clock
	got   []received
}

func (r *recorder) HandleMessage(msg raft.Message) {
	r.got = append(r.got, received{at: r.clock.Now(), msg: msg})
}

// terms returns the terms of the recorded messages, in delivery order.
func (r *recorder) terms() []raft.Term {
	out := make([]raft.Term, 0, len(r.got))
	for _, d := range r.got {
		out = append(out, d.msg.Term())
	}
	return out
}

// vote builds a RequestVote envelope; the term is the payload used by tests
// to tell messages apart.
func vote(from, to raft.NodeID, term raft.Term) raft.Message {
	return raft.Message{
		From: from, To: to, Type: raft.MsgRequestVote,
		RequestVote: &raft.RequestVote{Term: term, CandidateID: from},
	}
}

type testNet struct {
	clock *Clock
	net   *Network
	log   *bytes.Buffer
	nodes map[raft.NodeID]*recorder
}

func newTestNetwork(t *testing.T, seed int64, cfg NetworkConfig, ids ...raft.NodeID) *testNet {
	t.Helper()
	t.Logf("seed=%d", seed)
	tn := &testNet{clock: NewClock(), log: &bytes.Buffer{}, nodes: map[raft.NodeID]*recorder{}}
	tn.net = NewNetwork(tn.clock, rand.New(rand.NewSource(seed)), newTimelineLogger(tn.clock, tn.log), cfg)
	for _, id := range ids {
		r := &recorder{clock: tn.clock}
		tn.nodes[id] = r
		tn.net.Register(id, r)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("timeline:\n%s", tn.log.String())
		}
	})
	return tn
}

func fixed(d time.Duration) NetworkConfig { return NetworkConfig{MinLatency: d, MaxLatency: d} }

func assertTerms(t *testing.T, r *recorder, want ...raft.Term) {
	t.Helper()
	got := r.terms()
	if len(got) != len(want) {
		t.Fatalf("delivered terms %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered terms %v, want %v", got, want)
		}
	}
}

// TestNetworkDeliversWithLatency: fixed latency delivers exactly latency
// later, and messages sent on one link at the same instant stay FIFO.
func TestNetworkDeliversWithLatency(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(10*time.Millisecond), "a", "b")
	tn.net.Send(vote("a", "b", 1))
	tn.net.Send(vote("a", "b", 2))
	tn.net.Send(vote("a", "b", 3))

	tn.clock.Advance(9 * time.Millisecond)
	assertTerms(t, tn.nodes["b"])
	tn.clock.Advance(1 * time.Millisecond)
	assertTerms(t, tn.nodes["b"], 1, 2, 3)
	for _, d := range tn.nodes["b"].got {
		if d.at != 10*time.Millisecond {
			t.Fatalf("delivered at %v, want 10ms", d.at)
		}
	}

	tn.net.Send(vote("b", "a", 4))
	tn.clock.Advance(10 * time.Millisecond)
	assertTerms(t, tn.nodes["a"], 4)
	if at := tn.nodes["a"].got[0].at; at != 20*time.Millisecond {
		t.Fatalf("second send delivered at %v, want 20ms", at)
	}

	if !strings.Contains(tn.log.String(), "t=0 event=send from=a to=b type=RequestVote term=1 latency=10ms\n") {
		t.Fatalf("timeline missing send line:\n%s", tn.log.String())
	}
	if !strings.Contains(tn.log.String(), "t=10 event=deliver from=a to=b type=RequestVote term=1\n") {
		t.Fatalf("timeline missing deliver line:\n%s", tn.log.String())
	}
}

// TestNetworkRandomLatencyStaysInRange: seeded latency is drawn from
// [Min, Max] inclusive, and the same seed draws the same sequence.
func TestNetworkRandomLatencyStaysInRange(t *testing.T) {
	cfg := NetworkConfig{MinLatency: 1 * time.Millisecond, MaxLatency: 20 * time.Millisecond}
	run := func(seed int64) []time.Duration {
		tn := newTestNetwork(t, seed, cfg, "a", "b")
		for i := 0; i < 50; i++ {
			tn.net.Send(vote("a", "b", raft.Term(i+1)))
		}
		tn.clock.Advance(time.Second)
		var ats []time.Duration
		for _, d := range tn.nodes["b"].got {
			if d.at < cfg.MinLatency || d.at > cfg.MaxLatency {
				t.Fatalf("delivered at %v, outside [%v, %v]", d.at, cfg.MinLatency, cfg.MaxLatency)
			}
			ats = append(ats, d.at)
		}
		if len(ats) != 50 {
			t.Fatalf("delivered %d messages, want 50", len(ats))
		}
		return ats
	}
	a, b := run(7), run(7)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed, different delivery times: %v vs %v", a, b)
		}
	}
}

// TestNetworkDisconnectIsDirectional: Disconnect(a,b) blocks a->b only;
// Reconnect restores it.
func TestNetworkDisconnectIsDirectional(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a", "b")
	tn.net.Disconnect("a", "b")
	if tn.net.Connected("a", "b") || !tn.net.Connected("b", "a") {
		t.Fatal("Disconnect(a,b) must block a->b only")
	}
	tn.net.Send(vote("a", "b", 1))
	tn.net.Send(vote("b", "a", 2))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"])
	assertTerms(t, tn.nodes["a"], 2)

	tn.net.Reconnect("a", "b")
	tn.net.Send(vote("a", "b", 3))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"], 3)

	if !strings.Contains(tn.log.String(), "event=drop from=a to=b type=RequestVote term=1 reason=unreachable\n") {
		t.Fatalf("timeline missing drop line:\n%s", tn.log.String())
	}
}

// TestNetworkPartitionAndHeal: cross-group traffic is blocked both ways,
// intra-group traffic flows, Heal restores everything.
func TestNetworkPartitionAndHeal(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a", "b", "c")
	tn.net.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b", "c"})

	tn.net.Send(vote("a", "b", 1))
	tn.net.Send(vote("b", "a", 2))
	tn.net.Send(vote("b", "c", 3))
	tn.net.Send(vote("c", "b", 4))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["a"])
	assertTerms(t, tn.nodes["b"], 4)
	assertTerms(t, tn.nodes["c"], 3)

	tn.net.Heal()
	tn.net.Send(vote("a", "b", 5))
	tn.net.Send(vote("b", "a", 6))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["a"], 6)
	assertTerms(t, tn.nodes["b"], 4, 5)
}

// TestNetworkPartitionLosesInFlightMessages: a message sent before the
// partition but not yet delivered is lost (policy is re-checked on delivery).
func TestNetworkPartitionLosesInFlightMessages(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(10*time.Millisecond), "a", "b")
	tn.net.Send(vote("a", "b", 1))
	tn.clock.Advance(5 * time.Millisecond)
	tn.net.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b"})
	tn.clock.Advance(10 * time.Millisecond)
	assertTerms(t, tn.nodes["b"])
	if !strings.Contains(tn.log.String(), "reason=unreachable-in-flight") {
		t.Fatalf("timeline missing in-flight drop:\n%s", tn.log.String())
	}
}

// TestNetworkHealClearsDisconnects: Heal restores directed disconnects too.
func TestNetworkHealClearsDisconnects(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a", "b")
	tn.net.Disconnect("a", "b")
	tn.net.Heal()
	if !tn.net.Connected("a", "b") {
		t.Fatal("Heal did not clear Disconnect(a,b)")
	}
}

// TestNetworkIsolate cuts every link to and from the node.
func TestNetworkIsolate(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a", "b", "c")
	tn.net.Isolate("a")
	tn.net.Send(vote("a", "b", 1))
	tn.net.Send(vote("b", "a", 2))
	tn.net.Send(vote("b", "c", 3))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["a"])
	assertTerms(t, tn.nodes["b"])
	assertTerms(t, tn.nodes["c"], 3)
	tn.net.Reconnect("a", "b")
	tn.net.Send(vote("a", "b", 4))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"], 4)
}

// TestNetworkDrop drops only the next message on the link; calls accumulate.
func TestNetworkDrop(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a", "b")
	tn.net.Drop("a", "b")
	tn.net.Drop("a", "b")
	tn.net.Send(vote("a", "b", 1)) // dropped
	tn.net.Send(vote("a", "b", 2)) // dropped
	tn.net.Send(vote("a", "b", 3)) // delivered
	tn.net.Send(vote("b", "a", 4)) // other direction unaffected
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"], 3)
	assertTerms(t, tn.nodes["a"], 4)
	if !strings.Contains(tn.log.String(), "term=1 reason=requested") {
		t.Fatalf("timeline missing requested drop:\n%s", tn.log.String())
	}
}

// TestNetworkDuplicateHook: off by default; when on, each send is delivered
// twice (each copy draws its own latency).
func TestNetworkDuplicateHook(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a", "b")
	tn.net.Send(vote("a", "b", 1))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"], 1)

	tn.net.SetDuplicate("a", "b", true)
	tn.net.Send(vote("a", "b", 2))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"], 1, 2, 2)

	tn.net.SetDuplicate("a", "b", false)
	tn.net.Send(vote("a", "b", 3))
	tn.clock.Advance(time.Millisecond)
	assertTerms(t, tn.nodes["b"], 1, 2, 2, 3)
}

// TestNetworkUnknownRecipientIsDropped: no handler registered => dropped and
// logged, not a panic (a later issue uses this for crashed nodes).
func TestNetworkUnknownRecipientIsDropped(t *testing.T) {
	tn := newTestNetwork(t, 1, fixed(time.Millisecond), "a")
	tn.net.Send(vote("a", "zz", 1))
	tn.clock.Advance(time.Millisecond)
	if !strings.Contains(tn.log.String(), "reason=no-handler") {
		t.Fatalf("timeline missing no-handler drop:\n%s", tn.log.String())
	}
}

func TestNetworkRejectsInvalidLatency(t *testing.T) {
	clock := NewClock()
	logger := newTimelineLogger(clock, &bytes.Buffer{})
	rng := rand.New(rand.NewSource(1))
	assertPanics(t, "negative", func() {
		NewNetwork(clock, rng, logger, NetworkConfig{MinLatency: -1})
	})
	assertPanics(t, "max<min", func() {
		NewNetwork(clock, rng, logger, NetworkConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 1 * time.Millisecond})
	})
}
