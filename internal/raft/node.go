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

// ErrTermOverflow is returned when starting an election would advance
// currentTerm past the maximum Term. Wrapping to 0 would violate Term
// Monotonicity, so the node refuses instead; it can still vote and follow
// in its current term.
var ErrTermOverflow = errors.New("raft: currentTerm is at its maximum, cannot start an election")

// Config configures a Node. All fields except Logger are required.
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
	// must be smaller than ElectionTimeoutMin (ideally by an order of
	// magnitude), otherwise followers time out between heartbeats.
	HeartbeatInterval time.Duration
	// Rand is the only source of randomness the core uses (election timeouts).
	// It is required and has no default: a deterministic host seeds it
	// explicitly, and a fixed default would give every node the same timeout
	// sequence, which defeats randomized elections. Nodes in one process may
	// share a single *rand.Rand as long as one goroutine drives them.
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
	case c.HeartbeatInterval >= c.ElectionTimeoutMin:
		return fmt.Errorf("raft: HeartbeatInterval %v must be smaller than ElectionTimeoutMin %v", c.HeartbeatInterval, c.ElectionTimeoutMin)
	case c.Rand == nil:
		return errors.New("raft: Config.Rand is required")
	}
	seen := map[NodeID]bool{c.ID: true}
	for _, p := range c.Peers {
		if p == None {
			return errors.New("raft: Config.Peers contains an empty NodeID")
		}
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

	// votes is the Candidate's tally for the current election, keyed by
	// voter (a set, so duplicated responses cannot double count). nil
	// outside an election.
	votes map[NodeID]bool
}

// NewNode constructs a Node, loading currentTerm, votedFor and the log from
// storage. A node always starts as a Follower with commitIndex = lastApplied
// = 0; committed entries are re-learned from the Leader after restart.
func NewNode(cfg Config, storage Storage, sm StateMachine) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, errors.New("raft: storage is required")
	}
	if sm == nil {
		return nil, errors.New("raft: state machine is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	state, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load persistent state: %w", err)
	}
	if err := ValidateEntries(state.Entries, 1); err != nil {
		return nil, fmt.Errorf("raft: load persistent state: %w", err)
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
		log:         raftLog{entries: CloneEntries(state.Entries)},
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
//
// A malformed envelope (Message.Validate) or a sender that is not a peer is
// rejected with an error before anything else: membership is fixed, so an
// unknown node must not be able to advance the term, obtain a vote or have
// a vote response counted. Rejection changes no state and yields no
// actions; the error is diagnostic for the host.
//
// The "higher term seen" rule (Figure 2, All Servers) then runs for every
// message type: a message from a later term makes this node a Follower in
// that term before the message itself is handled. AppendEntries handling is
// implemented in a later issue.
//
// Step is two transitions in sequence, each atomic on its own. If the
// step-down succeeded and only the handler's storage write failed, the node
// is durably a Follower in the new term, so the step-down actions are still
// returned alongside the error: a Leader's StopHeartbeatTimer and
// ResetElectionTimer must reach the host, or it keeps a heartbeat timer for
// a node that is no longer Leader and never arms an election timer.
func (n *Node) Step(msg Message) ([]Action, error) {
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	if !n.isPeer(msg.From) {
		return nil, fmt.Errorf("raft: message %s from %q, which is not a peer of %s", msg.Type, msg.From, n.id)
	}
	var actions []Action
	if msg.Term() > n.currentTerm {
		stepDown, err := n.becomeFollower(msg.Term())
		if err != nil {
			return nil, err
		}
		actions = append(actions, stepDown...)
	}
	switch msg.Type {
	case MsgRequestVote:
		more, err := n.handleRequestVote(msg.From, msg.RequestVote)
		if err != nil {
			return actions, err
		}
		return append(actions, more...), nil
	case MsgRequestVoteResponse:
		return append(actions, n.handleRequestVoteResponse(msg.From, msg.RequestVoteResponse)...), nil
	default:
		return actions, ErrNotImplemented
	}
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
