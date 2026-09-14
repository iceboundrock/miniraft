package simulator

import (
	"bytes"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Default timing used when Config leaves the corresponding fields zero.
// They match the raft package tests: heartbeat well below the election
// timeout, and a latency small enough that a round trip fits inside it.
const (
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
	DefaultLatency            = 5 * time.Millisecond
)

// Config describes a simulated cluster.
type Config struct {
	// Seed initializes the cluster's single random source, which drives
	// election timeouts and message latency. Same seed => same run.
	Seed int64
	// NodeIDs are the Raft nodes to create (any order; nodes are created and
	// listed sorted). Empty is allowed: tests then add stub handlers.
	NodeIDs []raft.NodeID

	// Election timeout range and heartbeat interval passed to every node.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// Per-message latency range; equal values give fixed latency.
	MinLatency time.Duration
	MaxLatency time.Duration

	// Stores optionally supplies the storage a node is built over, so a
	// test can pre-load a term, a vote or a log. Nodes not listed get a
	// fresh MemoryStorage. Every key must also appear in NodeIDs.
	Stores map[raft.NodeID]*storage.MemoryStorage
}

func (cfg Config) withDefaults() Config {
	if cfg.ElectionTimeoutMin == 0 && cfg.ElectionTimeoutMax == 0 {
		cfg.ElectionTimeoutMin = DefaultElectionTimeoutMin
		cfg.ElectionTimeoutMax = DefaultElectionTimeoutMax
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.MinLatency == 0 && cfg.MaxLatency == 0 {
		cfg.MinLatency = DefaultLatency
		cfg.MaxLatency = DefaultLatency
	}
	return cfg
}

// Cluster is the deterministic test harness: one clock, one network, one
// random source, one timeline, and the nodes. Everything runs on the caller's
// goroutine when it calls Run, Step or RunUntil; nothing runs otherwise.
type Cluster struct {
	cfg      Config
	clock    *Clock
	rng      *rand.Rand
	timeline bytes.Buffer
	logger   *slog.Logger
	network  *Network
	nodes    map[raft.NodeID]*SimNode
	ids      []raft.NodeID // sorted; the only iteration order used
	errs     []error
	inv      invariants
}

// nopStateMachine stands in for the KV state machine of a later issue.
type nopStateMachine struct{}

func (nopStateMachine) Apply(raft.LogEntry) ([]byte, error) { return nil, nil }

// NewCluster builds the harness and one raft.Node per NodeID, each over a
// fresh storage.MemoryStorage (or the one given in Config.Stores) and a
// no-op state machine, and executes every node's Start actions, so a fresh
// cluster has one election timer per node pending at time 0.
func NewCluster(cfg Config) (*Cluster, error) {
	cfg = cfg.withDefaults()
	// NewNetwork panics on a bad range; a Config error belongs on this
	// constructor's error path like every other invalid setting.
	if cfg.MinLatency < 0 || cfg.MaxLatency < cfg.MinLatency {
		return nil, fmt.Errorf("simulator: invalid latency range [%v, %v]", cfg.MinLatency, cfg.MaxLatency)
	}
	c := &Cluster{
		cfg:   cfg,
		clock: NewClock(),
		rng:   rand.New(rand.NewSource(cfg.Seed)),
		nodes: make(map[raft.NodeID]*SimNode),
	}
	c.logger = newTimelineLogger(c.clock, &c.timeline)
	c.network = NewNetwork(c.clock, c.rng, c.logger, NetworkConfig{MinLatency: cfg.MinLatency, MaxLatency: cfg.MaxLatency})
	c.logger.Info("cluster", "seed", cfg.Seed, "nodes", len(cfg.NodeIDs))

	ids := slices.Clone(cfg.NodeIDs)
	slices.Sort(ids)
	for id := range cfg.Stores {
		if _, ok := slices.BinarySearch(ids, id); !ok {
			return nil, fmt.Errorf("simulator: Stores lists node %q, which is not in NodeIDs", id)
		}
	}
	for i, id := range ids {
		// The peer filter below (p != id) would silently hide a duplicate
		// id from raft.Config validation, so reject it here.
		if i > 0 && ids[i-1] == id {
			return nil, fmt.Errorf("simulator: duplicate node id %q", id)
		}
		var peers []raft.NodeID
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		st := cfg.Stores[id]
		if st == nil {
			st = storage.NewMemoryStorage()
		}
		node, err := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			HeartbeatInterval:  cfg.HeartbeatInterval,
			Rand:               c.rng,
			Logger:             c.logger,
		}, st, nopStateMachine{})
		if err != nil {
			return nil, fmt.Errorf("simulator: node %q: %w", id, err)
		}
		c.AddNode(id, node).Storage = st
	}
	// Executed only once every node is registered, so that no action can
	// address a node that does not exist yet.
	for _, id := range c.ids {
		n := c.nodes[id]
		n.Execute(n.core.Start())
	}
	c.checkInvariants()
	return c, nil
}

// AddNode hosts core as node id: it becomes the network handler for id and
// its actions are translated by the returned SimNode. NewCluster uses it for
// raft.Nodes; tests use it for scripted cores. It panics on raft.None, a
// duplicate id or a nil core, leaving the cluster unchanged.
func (c *Cluster) AddNode(id raft.NodeID, core Core) *SimNode {
	// raft.None is "no node"; NewCluster rejects it via raft.NewNode, and a
	// scripted core must not get to send or receive with an empty address.
	if id == raft.None {
		panic("simulator: AddNode with empty node id")
	}
	if _, dup := c.nodes[id]; dup {
		panic(fmt.Sprintf("simulator: node %q already exists", id))
	}
	if core == nil {
		panic(fmt.Sprintf("simulator: AddNode(%q) with nil Core", id))
	}
	n := &SimNode{id: id, core: core, cluster: c, logger: c.logger.With("node", id)}
	c.nodes[id] = n
	c.ids = append(c.ids, id)
	slices.Sort(c.ids)
	c.network.Register(id, n)
	return n
}

// Seed returns the seed the cluster was built with (print it on failure).
func (c *Cluster) Seed() int64 { return c.cfg.Seed }

// Clock returns the fake clock.
func (c *Cluster) Clock() *Clock { return c.clock }

// Network returns the simulated network, for fault injection and for
// registering stub handlers.
func (c *Cluster) Network() *Network { return c.network }

// Logger returns the timeline logger so that tests can add their own marks.
func (c *Cluster) Logger() *slog.Logger { return c.logger }

// Node returns the node with the given id, or nil.
func (c *Cluster) Node(id raft.NodeID) *SimNode { return c.nodes[id] }

// Nodes returns every node sorted by id.
func (c *Cluster) Nodes() []*SimNode {
	out := make([]*SimNode, 0, len(c.ids))
	for _, id := range c.ids {
		out = append(out, c.nodes[id])
	}
	return out
}

// Now returns the current simulated time.
func (c *Cluster) Now() time.Duration { return c.clock.Now() }

// Roles returns every node's current role.
func (c *Cluster) Roles() map[raft.NodeID]raft.Role {
	out := make(map[raft.NodeID]raft.Role, len(c.ids))
	for _, id := range c.ids {
		out[id] = c.nodes[id].Status().Role
	}
	return out
}

// Leader returns the unique Leader of the highest term among the nodes that
// currently believe they are Leader, or nil when there is none. A Leader of
// a lower term is a deposed Leader that has not heard about the new term
// yet (legal in Raft); two Leaders in the same highest term are an Election
// Safety violation, which the invariant checker reports and Leader() treats
// as "no unique Leader".
func (c *Cluster) Leader() *SimNode {
	var best *SimNode
	unique := false
	for _, id := range c.ids {
		n := c.nodes[id]
		st := n.Status()
		if st.Role != raft.Leader {
			continue
		}
		switch {
		case best == nil || st.Term > best.Status().Term:
			best, unique = n, true
		case st.Term == best.Status().Term:
			unique = false
		}
	}
	if !unique {
		return nil
	}
	return best
}

// Propose submits a client command to the current Leader (see Leader) and
// returns the log index it was assigned. It fails when no unique Leader
// exists; the caller decides whether to wait for one and retry.
func (c *Cluster) Propose(cmd []byte) (raft.Index, error) {
	leader := c.Leader()
	if leader == nil {
		return 0, fmt.Errorf("simulator: Propose(%q): no Leader", cmd)
	}
	return leader.Propose(cmd)
}

// LogsConverged reports whether every node holds exactly the Leader's
// persisted log. It is false without a unique Leader and false while any
// log cannot be read: a closed store, or a scripted core without a Storage
// (a log nobody can observe is not known to agree). It counts every node,
// whatever the network looks like: a predicate that only looked at the
// nodes the Leader can reach would be trivially true for a fully isolated
// Leader, and RunUntil on it would stop before anything was replicated. A
// partition test that wants "the majority agrees" should compare the logs
// it means.
func (c *Cluster) LogsConverged() bool {
	leader := c.Leader()
	if leader == nil {
		return false
	}
	want, err := leader.loadLog()
	if err != nil {
		return false
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == leader {
			continue
		}
		log, err := n.loadLog()
		if err != nil || !slices.EqualFunc(log, want, sameEntry) {
			return false
		}
	}
	return true
}

// sameEntry reports whether two entries are identical, command included.
func sameEntry(a, b raft.LogEntry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Command, b.Command)
}

// Run executes every event up to and including absolute time until, then
// sets the clock to until. Run(Now()) fires events due right now; a past
// until is a no-op. Run, Step and RunUntil belong to the test driver: calling
// them from inside the simulation (a Core method or a timer callback) panics,
// see Clock.
func (c *Cluster) Run(until time.Duration) {
	// Compare absolute times before subtracting: until-Now() wraps positive
	// for a sufficiently old until, which would reach Advance and panic.
	if until < c.clock.Now() {
		return
	}
	c.clock.Advance(until - c.clock.Now())
}

// RunFor executes every event in the next d of simulated time, then sets
// the clock to Now()+d. It panics on a negative d or when Now()+d would pass
// the end of logical time (the clock's overflow policy: never wrap, because
// a wrapped target would be "in the past" and silently do nothing).
func (c *Cluster) RunFor(d time.Duration) {
	if d < 0 {
		panic(fmt.Sprintf("simulator: RunFor(%v) with a negative duration", d))
	}
	// Advance checks the overflow itself; computing Now()+d here would wrap
	// before Run could see it.
	c.clock.Advance(d)
}

// StopHeartbeats silences node id as a Leader: every AppendEntries it sends
// is dropped by the network until ResumeHeartbeats. Everything else (its
// RequestVotes, messages to it, its followers' replies) still flows, so the
// scenario is "the Leader stopped talking", not a partition. Undo with
// ResumeHeartbeats.
func (c *Cluster) StopHeartbeats(id raft.NodeID) { c.network.SetMuteAppendEntries(id, true) }

// ResumeHeartbeats undoes StopHeartbeats.
func (c *Cluster) ResumeHeartbeats(id raft.NodeID) { c.network.SetMuteAppendEntries(id, false) }

// Step executes the next event; false when nothing is pending.
func (c *Cluster) Step() bool { return c.clock.Step() }

// RunUntil executes events one at a time until pred is true, and reports
// whether it became true. If the next event lies beyond maxTime (or nothing
// is pending), the clock is moved to maxTime and pred is evaluated one last
// time. pred is checked before every event, so the run stops at the first
// event that makes it hold.
func (c *Cluster) RunUntil(pred func() bool, maxTime time.Duration) bool {
	for !pred() {
		at, ok := c.clock.NextDeadline()
		if !ok || at > maxTime {
			c.Run(maxTime)
			return pred()
		}
		c.clock.Step()
	}
	return true
}

// Timeline returns every line logged so far, oldest first.
func (c *Cluster) Timeline() []string {
	s := strings.TrimSuffix(c.timeline.String(), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// Errors returns every error a core returned, in order. Tests assert it is
// empty; a non-empty result while the protocol is unimplemented is expected.
func (c *Cluster) Errors() []error { return slices.Clone(c.errs) }
