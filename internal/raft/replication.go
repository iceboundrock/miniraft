package raft

import "fmt"

// This file implements the AppendEntries RPC (§5.3): the Leader's
// per-peer replication state (nextIndex/matchIndex), Propose's fan-out and
// the heartbeat, the follower's consistency check and conflict repair, and
// the Leader's handling of replies. Commit and apply (LeaderCommit,
// commitIndex) are a later issue.

// HeartbeatTimeout tells the node its heartbeat timer fired. A Leader sends
// AppendEntries to every peer and re-arms the timer; a peer that is caught
// up gets an empty one (the heartbeat proper), a peer that is behind gets
// the entries it is missing. Any other role ignores it: the host stops the
// heartbeat timer on step-down, so this can only be a stale fire, and a
// Follower must never claim leadership.
func (n *Node) HeartbeatTimeout() ([]Action, error) {
	if n.role != Leader {
		n.logger.Info("HeartbeatTimeout", "term", n.currentTerm, "role", n.role, "ignored", true)
		return nil, nil
	}
	n.logger.Info("Heartbeat", "term", n.currentTerm, "role", n.role,
		"lastIndex", n.log.lastIndex(), "commitIndex", n.commitIndex)
	actions := make([]Action, 0, len(n.peers)+1)
	for _, p := range n.peers {
		actions = append(actions, n.sendAppendEntries(p))
	}
	return append(actions, ResetHeartbeatTimer{Interval: n.cfg.HeartbeatInterval}), nil
}

// sendAppendEntries builds the AppendEntries for one peer from the Leader's
// replication state: the suffix starting at nextIndex[peer], preceded by
// (prevLogIndex, prevLogTerm) = the entry just before it, which is what the
// follower's consistency check compares against. An empty suffix is a
// heartbeat. Entries are deep copies (entriesFrom), so the message can be
// held by a transport without sharing bytes with the log.
func (n *Node) sendAppendEntries(peer NodeID) Action {
	next := n.nextIndex[peer]
	prevIndex := next - 1
	prevTerm := n.log.termAt(prevIndex)
	entries := n.log.entriesFrom(next)
	n.logger.Info("AppendEntriesSend", "term", n.currentTerm, "role", n.role, "peer", peer,
		"prevIndex", prevIndex, "prevTerm", prevTerm, "n", len(entries))
	return SendMessage{Message: Message{
		From: n.id, To: peer, Type: MsgAppendEntries,
		AppendEntries: &AppendEntries{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		},
	}}
}

// handleAppendEntries implements the receiver side of AppendEntries
// (Figure 2). The caller has already validated the message (so ae.LeaderID
// == from, from is a peer, and Entries is a well-formed suffix following
// PrevLogIndex) and applied the higher-term rule, so ae.Term <= currentTerm
// here.
//
// Order of decisions:
//
//  1. A stale term is rejected with the current term and nothing else
//     changes — in particular the election timer is not reset, or a deposed
//     Leader could keep followers from timing out.
//  2. Equal term: a Candidate yields (someone else won this term); a Leader
//     receiving another Leader's AppendEntries in its own term is an
//     Election Safety violation, reported as an error with no state change.
//     This is also what keeps a Leader out of the truncation below (Leader
//     Append-Only).
//  3. The sender is now the recognized Leader of this term: record it and
//     reset the election timer. This happens whether or not the log matches,
//     because a legitimate Leader whose log differs from ours is still the
//     Leader — the mismatch is repaired by replication, not by an election.
//  4. The consistency check: without (PrevLogIndex, PrevLogTerm) in the log
//     the whole batch is rejected and the Leader will retry further back.
//  5. Conflict repair and append (appendFromLeader), persisted before the
//     reply: Success with MatchIndex = PrevLogIndex + len(Entries), the
//     last index the follower now knows to agree with the Leader.
//
// LeaderCommit is ignored until commit propagation is implemented.
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
	resp := &AppendEntriesResponse{Term: n.currentTerm}
	if !n.log.matches(ae.PrevLogIndex, ae.PrevLogTerm) {
		log.Info("AppendEntriesReject", "prevIndex", ae.PrevLogIndex, "prevTerm", ae.PrevLogTerm,
			"lastIndex", n.log.lastIndex(), "lastTerm", n.log.lastTerm())
	} else {
		if err := n.appendFromLeader(ae.Entries); err != nil {
			return actions, err
		}
		resp.Success = true
		resp.MatchIndex = ae.PrevLogIndex + Index(len(ae.Entries))
	}
	return append(actions, SendMessage{Message: Message{
		From: n.id, To: from, Type: MsgAppendEntriesResponse, AppendEntriesResponse: resp,
	}}), nil
}

// appendFromLeader reconciles the Leader's entries with the follower's log
// after the consistency check has passed (Figure 2, receiver steps 3-4).
// Entries the log already holds with the same term are skipped: by Log
// Matching they are identical, and skipping is what makes a duplicated or
// reordered AppendEntries harmless — an older batch must never delete
// entries a newer one already appended. The first entry whose term differs
// from the existing one at that index is a conflict: that entry and every
// entry after it are deleted, in storage and then in memory, and the
// Leader's remaining entries are appended, again storage first. Nothing is
// deleted merely because the local log is longer than the batch.
func (n *Node) appendFromLeader(entries []LogEntry) error {
	i := 0
	for ; i < len(entries); i++ {
		e := entries[i]
		if e.Index > n.log.lastIndex() {
			break // the rest is new
		}
		if n.log.termAt(e.Index) != e.Term {
			if err := n.truncateSuffix(e.Index); err != nil {
				return err
			}
			break
		}
	}
	rest := entries[i:]
	if len(rest) == 0 {
		return nil
	}
	if err := n.storage.AppendEntries(rest); err != nil {
		return fmt.Errorf("raft: persist entries %d..%d: %w", rest[0].Index, rest[len(rest)-1].Index, err)
	}
	n.log.append(rest...)
	return nil
}

// truncateSuffix deletes every entry with Index >= from, in storage and
// then in memory. It is the only place a node deletes log entries, and
// only a follower repairing a conflict reaches it: a Leader never
// overwrites or deletes entries in its own log (Leader Append-Only, §5.3),
// so reaching this as Leader is a programming error and panics.
func (n *Node) truncateSuffix(from Index) error {
	if n.role == Leader {
		panic(fmt.Sprintf("raft: Leader %s would truncate its own log at %d (Leader Append-Only violated)", n.id, from))
	}
	n.logger.Info("TruncateSuffix", "term", n.currentTerm, "role", n.role,
		"index", from, "lastIndex", n.log.lastIndex())
	if err := n.storage.TruncateSuffix(from); err != nil {
		return fmt.Errorf("raft: truncate log at %d: %w", from, err)
	}
	n.log.truncateSuffix(from)
	return nil
}

// handleAppendEntriesResponse is the Leader's side of the replication loop
// (Figure 2, "Rules for Servers / Leaders"). Only a Leader in exactly the
// response's term is interested: a stale response belongs to a previous
// leadership (a higher term has already made Step step down).
//
// Success: matchIndex[peer] advances to the follower's MatchIndex and
// nextIndex[peer] follows to matchIndex+1. matchIndex never moves backwards,
// so a delayed or duplicated success for an older prefix is harmless. A
// MatchIndex beyond the Leader's own log is impossible for a correct
// follower (it can only have matched entries this Leader sent) and is
// reported as an error with no state change rather than trusted: the
// commit rule would otherwise count an index that does not exist.
//
// Failure: the follower lacks (prevLogIndex, prevLogTerm), so nextIndex is
// walked back by one and the probe is resent immediately (§5.3). The walk
// never goes below matchIndex+1 — everything up to matchIndex is known to
// match, so a rejection of an older probe that arrives after a success must
// not undo it — which also keeps nextIndex >= 1.
func (n *Node) handleAppendEntriesResponse(from NodeID, resp *AppendEntriesResponse) ([]Action, error) {
	if n.role != Leader || resp.Term != n.currentTerm {
		return nil, nil
	}
	log := n.logger.With("term", n.currentTerm, "role", n.role, "peer", from)
	log.Info("AppendEntriesAck", "success", resp.Success, "matchIndex", resp.MatchIndex)
	if !resp.Success {
		next := n.nextIndex[from] - 1
		if floor := n.matchIndex[from] + 1; next < floor {
			next = floor
		}
		n.nextIndex[from] = next
		return []Action{n.sendAppendEntries(from)}, nil
	}
	if resp.MatchIndex > n.log.lastIndex() {
		return nil, fmt.Errorf("raft: %s reports matchIndex %d beyond %s's last index %d",
			from, resp.MatchIndex, n.id, n.log.lastIndex())
	}
	if resp.MatchIndex > n.matchIndex[from] {
		n.matchIndex[from] = resp.MatchIndex
		log.Info("MatchIndexAdvance", "matchIndex", resp.MatchIndex)
	}
	n.nextIndex[from] = n.matchIndex[from] + 1
	return nil, nil
}
