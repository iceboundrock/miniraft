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

import (
	"bytes"
	"errors"
	"fmt"
)

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
//
// A real entry always has Index >= 1 and Term >= 1: index 0 / term 0 are the
// sentinels for "before the first entry", and an entry can only be created by
// a Leader, which implies an election in a term >= 1. ValidateEntries
// enforces this wherever entries enter a log.
//
// Command is replicated state-machine input and must never change once the
// entry is in a log. Every log boundary (raftLog, Storage, outgoing messages)
// therefore deep-copies entries with CloneEntries so that a caller mutating
// its own slice afterwards cannot alter what a node will apply.
type LogEntry struct {
	Index   Index  `json:"index"`
	Term    Term   `json:"term"`
	Command []byte `json:"command"`
}

// CloneEntries returns a deep copy of entries: the slice and every Command
// are freshly allocated. It returns nil for an empty input.
func CloneEntries(entries []LogEntry) []LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{Index: e.Index, Term: e.Term, Command: bytes.Clone(e.Command)}
	}
	return out
}

// ValidateEntries reports whether entries form a well-formed log suffix
// starting at index first: first is a real log index (>= 1), indexes are
// contiguous from first without wrapping past the maximum Index, and every
// term is non-zero. It is the single definition of "acceptable entries"
// shared by the log, Storage implementations and AppendEntries validation, so
// that a malformed batch is rejected at whichever boundary it arrives.
//
// first == 0 is rejected even for an empty batch: it can only arise from
// PrevLogIndex+1 wrapping (Message.Validate) or from a caller mistaking the
// sentinel for a real position, and both are malformed.
func ValidateEntries(entries []LogEntry, first Index) error {
	if first == 0 {
		return errors.New("raft: entries cannot start at index 0 (reserved for the sentinel)")
	}
	// want is advanced one entry at a time instead of computed as
	// first+Index(i) so that wrapping is observable: with first >= 1 it can
	// only become 0 by overflowing the maximum index.
	want := first
	for i, e := range entries {
		if want == 0 {
			return fmt.Errorf("raft: entry %d would exceed the maximum log index", i)
		}
		if e.Index != want {
			return fmt.Errorf("raft: entry %d has index %d, want %d", i, e.Index, want)
		}
		if e.Term == 0 {
			return fmt.Errorf("raft: entry at index %d has term 0 (reserved for the sentinel)", e.Index)
		}
		want++
	}
	return nil
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

// Clone returns a deep copy of m: every payload pointer is freshly
// allocated and AppendEntries.Entries is copied with CloneEntries. A
// transport that holds a message after Send returns (the simulator, which
// delivers later and may deliver twice) must clone it, otherwise the sender
// mutating its own copy, or one recipient mutating what it received, would
// change a message still in flight. A real transport gets the same isolation
// from serialization, so this is what keeps simulated and real delivery
// semantics identical.
//
// Like every other log boundary, Clone canonicalizes a zero-length Entries
// to nil (see CloneEntries). A heartbeat is an AppendEntries with
// len(Entries) == 0; receivers must not distinguish nil from empty, and a
// JSON transport sending "null" for one and "[]" for the other must be read
// the same way.
func (m Message) Clone() Message {
	out := m
	if m.RequestVote != nil {
		rv := *m.RequestVote
		out.RequestVote = &rv
	}
	if m.RequestVoteResponse != nil {
		rvr := *m.RequestVoteResponse
		out.RequestVoteResponse = &rvr
	}
	if m.AppendEntries != nil {
		ae := *m.AppendEntries
		ae.Entries = CloneEntries(m.AppendEntries.Entries)
		out.AppendEntries = &ae
	}
	if m.AppendEntriesResponse != nil {
		aer := *m.AppendEntriesResponse
		out.AppendEntriesResponse = &aer
	}
	return out
}

// Term returns the term carried by the payload. Every Raft RPC and reply
// carries the sender's term, which is what the "higher term ⇒ step down" rule
// inspects.
//
// A malformed envelope (unknown Type, or the payload selected by Type is nil)
// yields 0, the "no term" sentinel, rather than panicking; use Validate to
// diagnose it. Term never inspects a payload that does not match Type.
func (m Message) Term() Term {
	switch m.Type {
	case MsgRequestVote:
		if m.RequestVote != nil {
			return m.RequestVote.Term
		}
	case MsgRequestVoteResponse:
		if m.RequestVoteResponse != nil {
			return m.RequestVoteResponse.Term
		}
	case MsgAppendEntries:
		if m.AppendEntries != nil {
			return m.AppendEntries.Term
		}
	case MsgAppendEntriesResponse:
		if m.AppendEntriesResponse != nil {
			return m.AppendEntriesResponse.Term
		}
	}
	return 0
}

// Validate reports whether the envelope is well formed: Type is known, the
// matching payload (and only that payload) is set, and an AppendEntries
// payload carries a well-formed suffix that follows PrevLogIndex (see
// ValidateEntries).
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
	if ae := m.AppendEntries; ae != nil {
		if err := ValidateEntries(ae.Entries, ae.PrevLogIndex+1); err != nil {
			return fmt.Errorf("raft: AppendEntries payload: %w", err)
		}
	}
	return nil
}
