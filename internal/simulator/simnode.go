package simulator

import (
	"fmt"
	"log/slog"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage"
)

// Core is the part of *raft.Node the simulator drives: the four inputs of
// ADR 0002 plus Status for inspection. It is an interface so that the
// harness can be tested with scripted cores before the protocol exists.
type Core interface {
	Step(msg raft.Message) ([]raft.Action, error)
	ElectionTimeout() ([]raft.Action, error)
	HeartbeatTimeout() ([]raft.Action, error)
	Propose(cmd []byte) ([]raft.Action, error)
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

// ElectionTimerArmed reports whether an election timer is pending.
func (n *SimNode) ElectionTimerArmed() bool { return n.electionTimer != 0 }

// HeartbeatTimerArmed reports whether a heartbeat timer is pending.
func (n *SimNode) HeartbeatTimerArmed() bool { return n.heartbeatTimer != 0 }

// HandleMessage implements Handler: the message is stepped into the core.
func (n *SimNode) HandleMessage(msg raft.Message) {
	n.drive(func() ([]raft.Action, error) { return n.core.Step(msg) })
}

// Propose submits a client command to the core.
func (n *SimNode) Propose(cmd []byte) {
	n.logger.Info("Propose", "command", string(cmd))
	n.drive(func() ([]raft.Action, error) { return n.core.Propose(cmd) })
}

// drive calls one core input, records an error if it returns one, and then
// executes the returned actions (whatever was returned, even alongside an
// error, so that a partially failed step is visible on the timeline).
func (n *SimNode) drive(input func() ([]raft.Action, error)) {
	actions, err := input()
	if err != nil {
		n.logger.Info("error", "err", err)
		n.cluster.errs = append(n.cluster.errs, fmt.Errorf("node %s: %w", n.id, err))
	}
	n.Execute(actions)
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
