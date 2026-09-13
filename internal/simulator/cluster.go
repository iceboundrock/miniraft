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
}

// nopStateMachine stands in for the KV state machine of a later issue.
type nopStateMachine struct{}

func (nopStateMachine) Apply(raft.LogEntry) ([]byte, error) { return nil, nil }

// NewCluster builds the harness and one raft.Node per NodeID, each over a
// fresh storage.MemoryStorage and a no-op state machine.
//
// No timer is armed here: the initial election timeout is chosen by the
// core, and raft.Node has no start entry point until the election issue adds
// one (issue #5, Start() returning the first ResetElectionTimer). Until then
// a fresh cluster has no pending events and Run returns immediately.
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
		st := storage.NewMemoryStorage()
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
	return c, nil
}

// AddNode hosts core as node id: it becomes the network handler for id and
// its actions are translated by the returned SimNode. NewCluster uses it for
// raft.Nodes; tests use it for scripted cores. It panics on a duplicate id
// or a nil core.
func (c *Cluster) AddNode(id raft.NodeID, core Core) *SimNode {
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

// Run executes every event up to and including absolute time until, then
// sets the clock to until. Run(Now()) fires events due right now; a past
// until is a no-op.
func (c *Cluster) Run(until time.Duration) {
	// Compare absolute times before subtracting: until-Now() wraps positive
	// for a sufficiently old until, which would reach Advance and panic.
	if until < c.clock.Now() {
		return
	}
	c.clock.Advance(until - c.clock.Now())
}

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
