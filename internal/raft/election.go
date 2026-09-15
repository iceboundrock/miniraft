package raft

import (
	"fmt"
	"math"
	"time"
)

// This file implements leader election (§5.2) and the election restriction
// (§5.4.1): the Follower -> Candidate -> Leader transitions, RequestVote
// handling, vote counting and the shared "higher term seen" step-down.
//
// Persistence rule used throughout: currentTerm and votedFor are written to
// Storage before the in-memory copies change. If the write fails the
// transition returns the error with its state untouched and produces no
// actions, so nothing a peer could observe ever runs ahead of what is
// durable. Node.Step composes two such transitions (step-down, then the
// handler) and returns the actions of whichever completed.

// candidateLogUpToDate is the election restriction of §5.4.1: a voter grants
// its vote only if the candidate's log is at least as up-to-date as its own.
// The last terms are compared first; only equal last terms compare the
// index. It is a standalone pure function because getting this comparison
// wrong lets a Leader without the committed entries win, which breaks Leader
// Completeness.
func candidateLogUpToDate(candLastTerm Term, candLastIndex Index, localLastTerm Term, localLastIndex Index) bool {
	if candLastTerm != localLastTerm {
		return candLastTerm > localLastTerm
	}
	return candLastIndex >= localLastIndex
}

// quorum is the number of votes (including the node's own) that forms a
// majority of the cluster.
func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}

// randomTimeout draws an election timeout uniformly from
// [ElectionTimeoutMin, ElectionTimeoutMax] (§5.2: randomized timeouts are
// what makes split votes rare).
func (n *Node) randomTimeout() time.Duration {
	span := int64(n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin)
	switch {
	case span == 0:
		return n.cfg.ElectionTimeoutMin
	case span == math.MaxInt64:
		// span+1 would overflow; Int63 already covers [0, MaxInt64].
		return n.cfg.ElectionTimeoutMin + time.Duration(n.cfg.Rand.Int63())
	default:
		return n.cfg.ElectionTimeoutMin + time.Duration(n.cfg.Rand.Int63n(span+1))
	}
}

// Start returns the actions that arm a freshly constructed node: the first
// election timer. The host executes them once, before feeding any event.
func (n *Node) Start() []Action {
	return []Action{ResetElectionTimer{Timeout: n.randomTimeout()}}
}

// ElectionTimeout tells the node its election timer fired. A Follower or
// Candidate starts a new election; a Leader ignores it (its election timer
// was stopped when it won, so this can only be a stale fire).
func (n *Node) ElectionTimeout() ([]Action, error) {
	n.logger.Info("ElectionTimeout", "term", n.currentTerm, "role", n.role)
	if n.role == Leader {
		return nil, nil
	}
	return n.becomeCandidate()
}

// becomeCandidate starts an election (§5.2): increment currentTerm, vote for
// self, persist both, reset the election timer and ask every peer for its
// vote. A single-node cluster already holds a majority and becomes Leader at
// once.
func (n *Node) becomeCandidate() ([]Action, error) {
	if n.currentTerm == math.MaxUint64 {
		return nil, ErrTermOverflow // term+1 would wrap to 0
	}
	term := n.currentTerm + 1
	if err := n.storage.SaveTermVote(term, n.id); err != nil {
		return nil, fmt.Errorf("raft: persist term %d vote: %w", term, err)
	}
	n.currentTerm = term
	n.votedFor = n.id
	n.role = Candidate
	n.leaderID = None
	n.votes = map[NodeID]bool{n.id: true}
	n.logger.Info("BecameCandidate", "term", n.currentTerm, "role", n.role)

	if len(n.votes) >= n.quorum() {
		return n.becomeLeader(), nil
	}
	actions := make([]Action, 0, len(n.peers)+1)
	for _, p := range n.peers {
		actions = append(actions, SendMessage{Message: Message{
			From: n.id, To: p, Type: MsgRequestVote,
			RequestVote: &RequestVote{
				Term:         n.currentTerm,
				CandidateID:  n.id,
				LastLogIndex: n.log.lastIndex(),
				LastLogTerm:  n.log.lastTerm(),
			},
		}})
	}
	actions = append(actions, ResetElectionTimer{Timeout: n.randomTimeout()})
	return actions, nil
}

// becomeLeader completes a won election: reinitialize the per-peer
// replication state (Figure 2, "Volatile state on leaders"), stop the
// election timer and start the heartbeat timer. The first heartbeat goes
// out when that timer fires (HeartbeatTimeout).
func (n *Node) becomeLeader() []Action {
	n.role = Leader
	n.leaderID = n.id
	n.votes = nil
	next := n.log.lastIndex() + 1
	for _, p := range n.peers {
		n.nextIndex[p] = next
		n.matchIndex[p] = 0
	}
	// The Leader's own log trivially matches itself; keeping matchIndex[self]
	// current lets the commit rule count the Leader like any other replica.
	n.matchIndex[n.id] = n.log.lastIndex()
	n.logger.Info("BecameLeader", "term", n.currentTerm, "role", n.role)
	return []Action{StopElectionTimer{}, ResetHeartbeatTimer{Interval: n.cfg.HeartbeatInterval}}
}

// becomeFollower is the shared "saw a higher term" transition (Figure 2,
// "Rules for Servers / All Servers"). It adopts term, clears the vote and
// persists both when the term actually changes; with an equal term it only
// changes the role (a Candidate yielding to the Leader of its own term). The
// recognized leaderID is forgotten with the old term; the caller records the
// new one if the message that caused the step-down came from a Leader.
//
// Timer policy: a Leader runs no election timer, so stepping down from
// Leader must arm one (and stop the heartbeat timer), otherwise a Leader
// deposed by a RequestVote it *denies* would never time out again. A
// Follower or Candidate already has a timer running and it is deliberately
// left alone: the election timer is reset only when starting an election,
// granting a vote, or accepting AppendEntries from the current Leader, so a
// disruptive candidate cannot postpone everyone's timeouts. (A Candidate
// stepping down to the Leader of its own term gets its reset from the
// AppendEntries handler, like any other valid heartbeat.)
//
// term < currentTerm is a programming error (terms never decrease) and
// panics rather than silently corrupting the state.
func (n *Node) becomeFollower(term Term) ([]Action, error) {
	if term < n.currentTerm {
		panic(fmt.Sprintf("raft: becomeFollower(%d) would decrease currentTerm %d", term, n.currentTerm))
	}
	grew := term > n.currentTerm
	if grew {
		if err := n.storage.SaveTermVote(term, None); err != nil {
			return nil, fmt.Errorf("raft: persist term %d: %w", term, err)
		}
	}
	from := n.role
	if !grew && from == Follower {
		return nil, nil // nothing to change
	}
	if grew {
		n.currentTerm = term
		n.votedFor = None
		n.leaderID = None
	}
	n.role = Follower
	n.votes = nil
	if from == Follower {
		n.logger.Info("TermAdvanced", "term", n.currentTerm, "role", n.role)
	} else {
		n.logger.Info("SteppedDown", "term", n.currentTerm, "role", n.role, "from", from)
	}
	if from == Leader {
		return []Action{StopHeartbeatTimer{}, ResetElectionTimer{Timeout: n.randomTimeout()}}, nil
	}
	return nil, nil
}

// handleRequestVote implements the receiver side of RequestVote (Figure 2).
// The caller has already validated the message (so rv.CandidateID == from
// and from is a peer) and applied the higher-term rule, so msg.Term <=
// currentTerm here. A vote is granted iff the request is for the current
// term, the node has not voted for someone else in it, and the candidate's
// log is at least as up-to-date. The grant is persisted before the response
// is produced; a denial produces no timer action.
func (n *Node) handleRequestVote(from NodeID, rv *RequestVote) ([]Action, error) {
	log := n.logger.With("term", n.currentTerm, "role", n.role, "peer", from)
	granted := false
	switch {
	case rv.Term < n.currentTerm:
		log.Info("VoteDenied", "reason", "stale term", "candidateTerm", rv.Term)
	case n.votedFor != None && n.votedFor != rv.CandidateID:
		log.Info("VoteDenied", "reason", "already voted", "votedFor", n.votedFor)
	case !candidateLogUpToDate(rv.LastLogTerm, rv.LastLogIndex, n.log.lastTerm(), n.log.lastIndex()):
		log.Info("VoteDenied", "reason", "stale log",
			"candidateLastIndex", rv.LastLogIndex, "candidateLastTerm", rv.LastLogTerm,
			"lastIndex", n.log.lastIndex(), "lastTerm", n.log.lastTerm())
	default:
		if err := n.storage.SaveTermVote(n.currentTerm, rv.CandidateID); err != nil {
			return nil, fmt.Errorf("raft: persist vote for %s in term %d: %w", rv.CandidateID, n.currentTerm, err)
		}
		n.votedFor = rv.CandidateID
		granted = true
		log.Info("VoteGranted")
	}

	var actions []Action
	if granted {
		actions = append(actions, ResetElectionTimer{Timeout: n.randomTimeout()})
	}
	return append(actions, SendMessage{Message: Message{
		From: n.id, To: from, Type: MsgRequestVoteResponse,
		RequestVoteResponse: &RequestVoteResponse{Term: n.currentTerm, VoteGranted: granted},
	}}), nil
}

// handleRequestVoteResponse tallies a vote. Only a Candidate in exactly the
// response's term counts it (a stale response belongs to an election that
// is over), and the tally is a set keyed by voter so a duplicated response
// cannot count twice. Step has already rejected senders that are not peers,
// so every counted voter is a cluster member.
func (n *Node) handleRequestVoteResponse(from NodeID, resp *RequestVoteResponse) []Action {
	if n.role != Candidate || resp.Term != n.currentTerm || !resp.VoteGranted {
		return nil
	}
	n.votes[from] = true
	if len(n.votes) >= n.quorum() {
		return n.becomeLeader()
	}
	return nil
}

// isPeer reports whether id is a member of the cluster other than this node.
func (n *Node) isPeer(id NodeID) bool {
	for _, p := range n.peers {
		if p == id {
			return true
		}
	}
	return false
}
