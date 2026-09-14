package raft

import "fmt"

// This file implements the AppendEntries RPC (§5.3) as far as heartbeats
// go: a Leader broadcasts empty AppendEntries on every heartbeat timeout,
// and a Follower that accepts one resets its election timer and records who
// the Leader is. The consistency check (prevLogIndex/prevLogTerm) already
// runs here so that a heartbeat reports Success=false to a follower whose
// log diverged; carrying entries, truncation and the Leader's
// nextIndex/matchIndex bookkeeping are a later issue.

// HeartbeatTimeout tells the node its heartbeat timer fired. A Leader sends
// an empty AppendEntries to every peer and re-arms the timer. Any other
// role ignores it: the host stops the heartbeat timer on step-down, so this
// can only be a stale fire, and a Follower must never claim leadership.
func (n *Node) HeartbeatTimeout() ([]Action, error) {
	if n.role != Leader {
		n.logger.Info("HeartbeatTimeout", "term", n.currentTerm, "role", n.role, "ignored", true)
		return nil, nil
	}
	prevIndex, prevTerm := n.log.lastIndex(), n.log.lastTerm()
	n.logger.Info("Heartbeat", "term", n.currentTerm, "role", n.role,
		"prevIndex", prevIndex, "prevTerm", prevTerm, "commitIndex", n.commitIndex)
	actions := make([]Action, 0, len(n.peers)+1)
	for _, p := range n.peers {
		actions = append(actions, SendMessage{Message: Message{
			From: n.id, To: p, Type: MsgAppendEntries,
			AppendEntries: &AppendEntries{
				Term:         n.currentTerm,
				LeaderID:     n.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      nil,
				LeaderCommit: n.commitIndex,
			},
		}})
	}
	return append(actions, ResetHeartbeatTimer{Interval: n.cfg.HeartbeatInterval}), nil
}

// handleAppendEntries implements the receiver side of AppendEntries
// (Figure 2) for heartbeats. The caller has already validated the message
// (so ae.LeaderID == from and from is a peer) and applied the higher-term
// rule, so ae.Term <= currentTerm here.
//
// Order of decisions:
//
//  1. A stale term is rejected with the current term and nothing else
//     changes — in particular the election timer is not reset, or a deposed
//     Leader could keep followers from timing out.
//  2. Equal term: a Candidate yields (someone else won this term); a Leader
//     receiving another Leader's AppendEntries in its own term is an
//     Election Safety violation, reported as an error with no state change.
//  3. The sender is now the recognized Leader of this term: record it and
//     reset the election timer. This happens whether or not the log matches,
//     because a legitimate Leader whose log differs from ours is still the
//     Leader — the mismatch is repaired by replication, not by an election.
//  4. The consistency check decides Success; on success MatchIndex is
//     PrevLogIndex because nothing was appended.
//
// LeaderCommit is ignored until commit propagation is implemented. Entries
// are not handled yet: the heartbeat part above is still honored, but no
// reply is produced and ErrNotImplemented is returned with the actions so
// far.
func (n *Node) handleAppendEntries(from NodeID, ae *AppendEntries) ([]Action, error) {
	log := n.logger.With("term", n.currentTerm, "role", n.role, "peer", from)
	if ae.Term < n.currentTerm {
		log.Info("RejectStaleLeader", "leaderTerm", ae.Term)
		return []Action{SendMessage{Message: Message{
			From: n.id, To: from, Type: MsgAppendEntriesResponse,
			AppendEntriesResponse: &AppendEntriesResponse{Term: n.currentTerm, Success: false},
		}}}, nil
	}
	switch n.role {
	case Candidate:
		// Same term (Step handled a higher one): no persistence, no timer
		// actions, since a Candidate keeps its election timer running.
		if _, err := n.becomeFollower(ae.Term); err != nil {
			return nil, err
		}
	case Leader:
		return nil, fmt.Errorf("raft: Leader %s received AppendEntries from %s in its own term %d (Election Safety violated)", n.id, from, n.currentTerm)
	}
	if n.leaderID != from {
		n.leaderID = from
		log.Info("RecognizedLeader", "leader", from)
	}
	actions := []Action{ResetElectionTimer{Timeout: n.randomTimeout()}}
	if len(ae.Entries) != 0 {
		return actions, ErrNotImplemented
	}
	success := n.log.matches(ae.PrevLogIndex, ae.PrevLogTerm)
	resp := &AppendEntriesResponse{Term: n.currentTerm, Success: success}
	if success {
		resp.MatchIndex = ae.PrevLogIndex
	} else {
		log.Info("ConsistencyCheckFailed", "prevIndex", ae.PrevLogIndex, "prevTerm", ae.PrevLogTerm,
			"lastIndex", n.log.lastIndex(), "lastTerm", n.log.lastTerm())
	}
	return append(actions, SendMessage{Message: Message{
		From: n.id, To: from, Type: MsgAppendEntriesResponse, AppendEntriesResponse: resp,
	}}), nil
}

// handleAppendEntriesResponse receives a follower's reply. Only a Leader in
// exactly the response's term is interested (a stale response belongs to a
// previous leadership). nextIndex/matchIndex bookkeeping is a later issue;
// for now the result is only logged.
func (n *Node) handleAppendEntriesResponse(from NodeID, resp *AppendEntriesResponse) []Action {
	if n.role != Leader || resp.Term != n.currentTerm {
		return nil
	}
	n.logger.Info("HeartbeatAck", "term", n.currentTerm, "role", n.role, "peer", from,
		"success", resp.Success, "matchIndex", resp.MatchIndex)
	return nil
}
