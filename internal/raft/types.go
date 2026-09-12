// Package raft implements the core of the Raft consensus algorithm as a pure,
// event-driven state machine.
//
// The package deliberately owns no goroutines, timers, sockets or files. A
// host (the deterministic simulator in tests, or the real-time runtime in
// production) feeds events into a Node and executes the Actions it returns.
// Storage and the replicated state machine are reached only through the
// interfaces declared in this package, which makes this the leaf package of
// the module: internal/storage and internal/statemachine import raft for its
// types, never the other way round.
//
// Names follow Figure 2 of "In Search of an Understandable Consensus
// Algorithm" (Ongaro & Ousterhout) so that code can be mapped back to the
// paper line by line.
package raft

import "fmt"

// NodeID identifies a Raft server. The empty string means "no node" and is
// used for votedFor when the node has not voted in the current term.
type NodeID string

// None is the NodeID meaning "no node".
const None NodeID = ""

// Term is a Raft term number. Terms are monotonically increasing logical
// clocks; a node's currentTerm never decreases.
type Term uint64

// Index is a position in the replicated log. Index 0 is a sentinel for "the
// empty log" (with Term 0); the first real entry has Index 1.
type Index uint64

// Role is the server state from Figure 4 of the paper.
type Role int

// Server roles.
const (
	Follower Role = iota
	Candidate
	Leader
)

// String returns the role name.
func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

// LogEntry is one entry in the replicated log.
type LogEntry struct {
	Index   Index  `json:"index"`
	Term    Term   `json:"term"`
	Command []byte `json:"command"`
}

// RequestVote is the RequestVote RPC request (Figure 2). It is sent by a
// Candidate to gather votes.
type RequestVote struct {
	Term         Term   `json:"term"`         // candidate's term
	CandidateID  NodeID `json:"candidateId"`  // candidate requesting the vote
	LastLogIndex Index  `json:"lastLogIndex"` // index of candidate's last log entry
	LastLogTerm  Term   `json:"lastLogTerm"`  // term of candidate's last log entry
}

// RequestVoteResponse is the RequestVote RPC reply.
type RequestVoteResponse struct {
	Term        Term `json:"term"`        // currentTerm, for the candidate to update itself
	VoteGranted bool `json:"voteGranted"` // true means the candidate received the vote
}

// AppendEntries is the AppendEntries RPC request (Figure 2). It is sent by the
// Leader to replicate entries; with no entries it is a heartbeat.
type AppendEntries struct {
	Term         Term       `json:"term"`         // leader's term
	LeaderID     NodeID     `json:"leaderId"`     // so followers can redirect clients
	PrevLogIndex Index      `json:"prevLogIndex"` // index of the entry immediately preceding Entries
	PrevLogTerm  Term       `json:"prevLogTerm"`  // term of the entry at PrevLogIndex
	Entries      []LogEntry `json:"entries"`      // entries to store (empty for heartbeat)
	LeaderCommit Index      `json:"leaderCommit"` // leader's commitIndex
}

// AppendEntriesResponse is the AppendEntries RPC reply.
//
// MatchIndex is an MVP addition to the paper's reply: on success it carries
// the index of the last entry the follower now knows to match the Leader's
// log. It lets the Leader update matchIndex without correlating replies to
// the exact request that produced them, which matters once messages can be
// delayed, duplicated or reordered.
type AppendEntriesResponse struct {
	Term       Term  `json:"term"`       // currentTerm, for the leader to update itself
	Success    bool  `json:"success"`    // true if the follower matched PrevLogIndex/PrevLogTerm
	MatchIndex Index `json:"matchIndex"` // valid only when Success is true
}

// MessageType tags which RPC payload a Message carries.
type MessageType int

// Message types.
const (
	MsgRequestVote MessageType = iota + 1
	MsgRequestVoteResponse
	MsgAppendEntries
	MsgAppendEntriesResponse
)

// String returns the message type name.
func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResponse:
		return "RequestVoteResponse"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResponse:
		return "AppendEntriesResponse"
	default:
		return fmt.Sprintf("MessageType(%d)", int(t))
	}
}

// Message is the envelope that transports carry between nodes. Exactly one of
// the payload pointers is non-nil and Type says which. The envelope is plain
// data so that any transport (simulated, HTTP/JSON, ...) can move it.
type Message struct {
	From NodeID      `json:"from"`
	To   NodeID      `json:"to"`
	Type MessageType `json:"type"`

	RequestVote           *RequestVote           `json:"requestVote,omitempty"`
	RequestVoteResponse   *RequestVoteResponse   `json:"requestVoteResponse,omitempty"`
	AppendEntries         *AppendEntries         `json:"appendEntries,omitempty"`
	AppendEntriesResponse *AppendEntriesResponse `json:"appendEntriesResponse,omitempty"`
}

// Term returns the term carried by the payload. Every Raft RPC and reply
// carries the sender's term, which is what the "higher term ⇒ step down" rule
// inspects.
func (m Message) Term() Term {
	switch m.Type {
	case MsgRequestVote:
		return m.RequestVote.Term
	case MsgRequestVoteResponse:
		return m.RequestVoteResponse.Term
	case MsgAppendEntries:
		return m.AppendEntries.Term
	case MsgAppendEntriesResponse:
		return m.AppendEntriesResponse.Term
	default:
		return 0
	}
}

// Validate reports whether the envelope is well formed: Type is known and the
// matching payload (and only that payload) is set.
func (m Message) Validate() error {
	set := 0
	var want bool
	if m.RequestVote != nil {
		set++
		want = want || m.Type == MsgRequestVote
	}
	if m.RequestVoteResponse != nil {
		set++
		want = want || m.Type == MsgRequestVoteResponse
	}
	if m.AppendEntries != nil {
		set++
		want = want || m.Type == MsgAppendEntries
	}
	if m.AppendEntriesResponse != nil {
		set++
		want = want || m.Type == MsgAppendEntriesResponse
	}
	switch {
	case m.Type < MsgRequestVote || m.Type > MsgAppendEntriesResponse:
		return fmt.Errorf("raft: unknown message type %d", int(m.Type))
	case set != 1:
		return fmt.Errorf("raft: message %s carries %d payloads, want 1", m.Type, set)
	case !want:
		return fmt.Errorf("raft: message type %s does not match its payload", m.Type)
	}
	return nil
}
