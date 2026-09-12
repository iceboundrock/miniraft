package raft

// PersistentState is everything Figure 2 requires to be on stable storage
// before a node responds to an RPC: currentTerm, votedFor and the log.
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Entries     []LogEntry // contiguous, starting at Index 1; nil when empty
}

// Storage is the durable-state abstraction the core writes through. Every
// method must be durable when it returns: the core assumes that once
// SaveTermVote or AppendEntries has returned nil, a crash cannot lose that
// write. Implementations live in internal/storage.
//
// Ownership: entries passed to AppendEntries and returned by Load never share
// memory with the caller (see CloneEntries), so neither side can alter the
// other's Command bytes after the call.
//
// The interface is declared here rather than in internal/storage because the
// core is the consumer and this keeps raft a leaf package (no import cycle).
type Storage interface {
	// Load returns the persisted state. A fresh store returns the zero state.
	Load() (PersistentState, error)
	// SaveTermVote durably records currentTerm and votedFor together. They are
	// always written as a pair because a vote is only meaningful in its term.
	SaveTermVote(term Term, votedFor NodeID) error
	// AppendEntries durably appends entries, which must be contiguous and
	// start at the current last index + 1. The call is all-or-nothing: on
	// error the stored log is unchanged.
	AppendEntries(entries []LogEntry) error
	// TruncateSuffix durably deletes every entry with Index >= fromIndex.
	TruncateSuffix(fromIndex Index) error
	// Close releases resources. Further calls may fail.
	Close() error
}

// StateMachine is the replicated state machine that committed entries are
// applied to, strictly in index order. Implementations live in
// internal/statemachine.
type StateMachine interface {
	// Apply executes the entry's command and returns its result.
	Apply(entry LogEntry) ([]byte, error)
}
