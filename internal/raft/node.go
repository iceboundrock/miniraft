package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

// ErrNotImplemented is returned by protocol entry points that a later issue
// implements. It exists so the skeleton compiles and is testable now.
var ErrNotImplemented = errors.New("raft: not implemented")

// Config configures a Node. All fields except Rand and Logger are required.
type Config struct {
	// ID is this node's identity.
	ID NodeID
	// Peers lists every other node in the cluster (excluding ID). The cluster
	// size is len(Peers)+1 and is fixed for the life of the cluster.
	Peers []NodeID
	// ElectionTimeoutMin and ElectionTimeoutMax bound the randomized election
	// timeout (§5.2). Each timeout is drawn uniformly from [Min, Max].
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often a Leader sends empty AppendEntries. It
	// must be much smaller than ElectionTimeoutMin.
	HeartbeatInterval time.Duration
	// Rand is the only source of randomness the core uses, so that a seeded
	// Rand makes the node deterministic. Defaults to a fixed-seed source.
	Rand *rand.Rand
	// Logger receives structured diagnostic events. Defaults to slog.Default.
	// Protocol behavior never depends on logging.
	Logger *slog.Logger
}

func (c *Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("raft: Config.ID is required")
	case c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax < c.ElectionTimeoutMin:
		return fmt.Errorf("raft: invalid election timeout range [%v, %v]", c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	case c.HeartbeatInterval <= 0:
		return errors.New("raft: Config.HeartbeatInterval must be positive")
	}
	seen := map[NodeID]bool{c.ID: true}
	for _, p := range c.Peers {
		if seen[p] {
			return fmt.Errorf("raft: duplicate or self peer %q", p)
		}
		seen[p] = true
	}
	return nil
}

// Status is a read-only snapshot of a Node used by tests, invariant checkers
// and status endpoints.
type Status struct {
	ID           NodeID
	Role         Role
	Term         Term
	VotedFor     NodeID
	CommitIndex  Index
	LastApplied  Index
	LastLogIndex Index
	LastLogTerm  Term
}

// Node is one Raft server. It is not safe for concurrent use: a single host
// goroutine (or a single-threaded simulator) must own it and serialize every
// call. Fields are grouped as in Figure 2.
type Node struct {
	cfg     Config
	storage Storage
	sm      StateMachine
	logger  *slog.Logger

	id    NodeID
	peers []NodeID
	role  Role

	// Persistent state (mirrored in storage; storage is written first).
	currentTerm Term
	votedFor    NodeID
	log         raftLog

	// Volatile state on all servers.
	commitIndex Index
	lastApplied Index

	// Volatile state on leaders, reinitialized after election.
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index
}

// NewNode constructs a Node, loading currentTerm, votedFor and the log from
// storage. A node always starts as a Follower with commitIndex = lastApplied
// = 0; committed entries are re-learned from the Leader after restart.
func NewNode(cfg Config, storage Storage, sm StateMachine) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(1))
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	state, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load persistent state: %w", err)
	}
	for i, e := range state.Entries {
		if e.Index != Index(i+1) {
			return nil, fmt.Errorf("raft: loaded entry %d has index %d", i, e.Index)
		}
	}
	peers := append([]NodeID(nil), cfg.Peers...)
	n := &Node{
		cfg:         cfg,
		storage:     storage,
		sm:          sm,
		logger:      cfg.Logger.With("node", cfg.ID),
		id:          cfg.ID,
		peers:       peers,
		role:        Follower,
		currentTerm: state.CurrentTerm,
		votedFor:    state.VotedFor,
		log:         raftLog{entries: append([]LogEntry(nil), state.Entries...)},
		nextIndex:   make(map[NodeID]Index, len(peers)),
		matchIndex:  make(map[NodeID]Index, len(peers)),
	}
	return n, nil
}

// ID returns the node's identity.
func (n *Node) ID() NodeID { return n.id }

// Status returns a snapshot of the node's observable state.
func (n *Node) Status() Status {
	return Status{
		ID:           n.id,
		Role:         n.role,
		Term:         n.currentTerm,
		VotedFor:     n.votedFor,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
}

// Step processes an incoming message and returns the resulting actions.
// Implemented in a later issue.
func (n *Node) Step(msg Message) ([]Action, error) {
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return nil, ErrNotImplemented
}

// ElectionTimeout tells the node its election timer fired.
// Implemented in a later issue.
func (n *Node) ElectionTimeout() ([]Action, error) {
	return nil, ErrNotImplemented
}

// HeartbeatTimeout tells the node its heartbeat timer fired.
// Implemented in a later issue.
func (n *Node) HeartbeatTimeout() ([]Action, error) {
	return nil, ErrNotImplemented
}

// Propose asks the node, if it is Leader, to append cmd to the log.
// Implemented in a later issue.
func (n *Node) Propose(cmd []byte) ([]Action, error) {
	return nil, ErrNotImplemented
}
