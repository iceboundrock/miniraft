package raft

import "time"

// Action is an instruction the core returns to its host after processing an
// event. The host executes actions in order after the core method returns,
// which gives a simple persistence rule: everything the core wrote to Storage
// during the event is durable before any resulting message leaves the node.
//
// Action is a closed set of value types; hosts switch on the concrete type.
type Action interface {
	isAction()
}

// SendMessage asks the host to deliver a message to Message.To.
type SendMessage struct {
	Message Message
}

// ResetElectionTimer asks the host to (re)arm the election timer so that it
// fires ElectionTimeout after Timeout, cancelling any previously armed
// election timer for this node.
type ResetElectionTimer struct {
	Timeout time.Duration
}

// StopElectionTimer asks the host to cancel the election timer (a Leader does
// not time out waiting for itself).
type StopElectionTimer struct{}

// ResetHeartbeatTimer asks the host to (re)arm the heartbeat timer so that it
// fires HeartbeatTimeout after Interval.
type ResetHeartbeatTimer struct {
	Interval time.Duration
}

// StopHeartbeatTimer asks the host to cancel the heartbeat timer (only a
// Leader sends heartbeats).
type StopHeartbeatTimer struct{}

// Applied reports that the entry at Index (created in Term) has been applied
// to the state machine and produced Result. Hosts use it to complete pending
// client proposals; the Term lets them detect that a proposal was overwritten
// by a different Leader's entry at the same index.
type Applied struct {
	Index  Index
	Term   Term
	Result []byte
}

func (SendMessage) isAction()         {}
func (ResetElectionTimer) isAction()  {}
func (StopElectionTimer) isAction()   {}
func (ResetHeartbeatTimer) isAction() {}
func (StopHeartbeatTimer) isAction()  {}
func (Applied) isAction()             {}
