package simulator

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Core is the part of *raft.Node the simulator drives: the four inputs of
// ADR 0002, Start for the initial actions, and Status for inspection. It is
// an interface so that the harness can be tested with scripted cores.
type Core interface {
	Start() []raft.Action
	Step(msg raft.Message) ([]raft.Action, error)
	ElectionTimeout() ([]raft.Action, error)
	HeartbeatTimeout() ([]raft.Action, error)
	Propose(cmd []byte) (raft.Index, []raft.Action, error)
	Status() raft.Status
}

var _ Core = (*raft.Node)(nil)

// SimNode hosts one Core inside a Cluster: it is the node's Handler on the
// network, owns the node's election and heartbeat timers, and translates the
// core's Actions into clock and network operations.
type SimNode struct {
	// Storage is the node's in-memory store when the node was created by
	// NewCluster; nil for cores added with AddNode. It is exposed so that a
	// later issue can "restart" the node over the same store.
	Storage *storage.MemoryStorage

	id      raft.NodeID
	core    Core
	cluster *Cluster
	logger  *slog.Logger

	// electionTimer and heartbeatTimer are the armed timers (0 = none).
	// Every Reset*Timer action stops the previous timer first: a stale
	// election timer that still fires is the classic "phantom timeout" bug.
	electionTimer  TimerID
	heartbeatTimer TimerID
}

// ID returns the node's identity.
func (n *SimNode) ID() raft.NodeID { return n.id }

// Status returns the core's observable state.
func (n *SimNode) Status() raft.Status { return n.core.Status() }

// errNoStorage is loadLog's answer for a node without a Storage: there is
// no persisted log to observe, which is not the same as an empty one.
var errNoStorage = errors.New("simulator: node has no Storage")

// Log returns the node's persisted log, read back from Storage. Tests
// compare what nodes have actually written, not what a core claims. It
// panics when the node has no Storage (a scripted core) or the store is
// closed: a test that asks for a log that cannot be read is asking for
// something that does not exist.
func (n *SimNode) Log() []raft.LogEntry {
	log, err := n.loadLog()
	if err != nil {
		panic(fmt.Sprintf("simulator: node %s: load storage: %v", n.id, err))
	}
	return log
}

// loadLog is Log for the harness itself: a missing or closed store is
// reported as an error (errNoStorage, storage.ErrClosed) instead of a
// panic, because the invariant checker and LogsConverged run after every
// input and must keep going when a node has no store or a test has closed
// one to make its writes fail. An error means "not observable", never
// "empty".
func (n *SimNode) loadLog() ([]raft.LogEntry, error) {
	if n.Storage == nil {
		return nil, errNoStorage
	}
	st, err := n.Storage.Load()
	if err != nil {
		return nil, err
	}
	return st.Entries, nil
}

// ElectionTimerArmed reports whether an election timer is pending.
func (n *SimNode) ElectionTimerArmed() bool { return n.electionTimer != 0 }

// HeartbeatTimerArmed reports whether a heartbeat timer is pending.
func (n *SimNode) HeartbeatTimerArmed() bool { return n.heartbeatTimer != 0 }

// ElectionDeadline returns when the pending election timer fires; false when
// none is armed. Tests use it to check that an event did (or did not) reset
// the timer.
func (n *SimNode) ElectionDeadline() (time.Duration, bool) {
	return n.cluster.clock.Deadline(n.electionTimer)
}

// ForceElectionTimeout cancels the pending election timer, if any, and
// delivers ElectionTimeout to the core right now. It is the fault-injection
// hook for "this node's timer fires next": with it a test can make two nodes
// start an election at the same instant (a split vote) by construction
// instead of by searching for a seed. Like every other simulator entry
// point it must be called by the test driver, not from inside the
// simulation.
func (n *SimNode) ForceElectionTimeout() {
	n.cluster.clock.Stop(n.electionTimer)
	n.electionTimer = 0
	n.logger.Info("ForceElectionTimeout")
	n.drive(n.core.ElectionTimeout)
}

// HandleMessage implements Handler: the message is stepped into the core.
func (n *SimNode) HandleMessage(msg raft.Message) {
	n.drive(func() ([]raft.Action, error) { return n.core.Step(msg) })
}

// Propose submits a client command to the core and returns the index the
// core assigned to it. Unlike the other inputs, the error is returned to the
// caller instead of being recorded in Errors(): a rejected proposal
// (raft.ErrNotLeader on a follower) is the protocol answering a client, not
// the core misbehaving.
func (n *SimNode) Propose(cmd []byte) (raft.Index, error) {
	n.logger.Info("ClientPropose", "command", string(cmd))
	var idx raft.Index
	var err error
	n.drive(func() ([]raft.Action, error) {
		var actions []raft.Action
		idx, actions, err = n.core.Propose(cmd)
		return actions, nil
	})
	if err != nil {
		n.logger.Info("ClientProposeRejected", "err", err)
	}
	return idx, err
}

// drive calls one core input, records an error if it returns one, executes
// the returned actions (whatever was returned, even alongside an error, so
// that a partially failed step is visible on the timeline), and then runs
// the invariant checker over the whole cluster.
func (n *SimNode) drive(input func() ([]raft.Action, error)) {
	actions, err := input()
	if err != nil {
		n.logger.Info("error", "err", err)
		n.cluster.errs = append(n.cluster.errs, fmt.Errorf("node %s: %w", n.id, err))
	}
	n.Execute(actions)
	n.cluster.checkInvariants()
}

// Execute translates actions into simulator operations, in order:
//
//	SendMessage          -> Network.Send
//	ResetElectionTimer   -> stop previous, Clock.After -> core.ElectionTimeout
//	StopElectionTimer    -> stop
//	ResetHeartbeatTimer  -> stop previous, Clock.After -> core.HeartbeatTimeout
//	StopHeartbeatTimer   -> stop
//	Applied              -> timeline entry
func (n *SimNode) Execute(actions []raft.Action) {
	clock := n.cluster.clock
	for _, a := range actions {
		switch a := a.(type) {
		case raft.SendMessage:
			n.cluster.network.Send(a.Message)
		case raft.ResetElectionTimer:
			clock.Stop(n.electionTimer)
			n.electionTimer = clock.After(a.Timeout, n.fireElectionTimer)
			n.logger.Info("ResetElectionTimer", "timeout", a.Timeout)
		case raft.StopElectionTimer:
			clock.Stop(n.electionTimer)
			n.electionTimer = 0
			n.logger.Info("StopElectionTimer")
		case raft.ResetHeartbeatTimer:
			clock.Stop(n.heartbeatTimer)
			n.heartbeatTimer = clock.After(a.Interval, n.fireHeartbeatTimer)
			n.logger.Info("ResetHeartbeatTimer", "interval", a.Interval)
		case raft.StopHeartbeatTimer:
			clock.Stop(n.heartbeatTimer)
			n.heartbeatTimer = 0
			n.logger.Info("StopHeartbeatTimer")
		case raft.Applied:
			n.logger.Info("Applied", "index", a.Index, "term", a.Term)
		default:
			panic(fmt.Sprintf("simulator: unknown action %T", a))
		}
	}
}

func (n *SimNode) fireElectionTimer() {
	n.electionTimer = 0 // cleared before the core runs so it may re-arm
	n.logger.Info("ElectionTimeout")
	n.drive(n.core.ElectionTimeout)
}

func (n *SimNode) fireHeartbeatTimer() {
	n.heartbeatTimer = 0
	n.logger.Info("HeartbeatTimeout")
	n.drive(n.core.HeartbeatTimeout)
}
