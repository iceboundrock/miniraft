package raft

import "fmt"

// raftLog is the in-memory view of the replicated log. Entries are stored
// contiguously starting at Index 1; entries[i] has Index i+1. Index 0 is the
// sentinel "before the first entry" position with Term 0, which is what makes
// the AppendEntries consistency check for an empty log work without special
// cases (prevLogIndex == 0 always matches).
//
// raftLog is purely volatile; the durable copy lives behind Storage and the
// core keeps the two in sync by writing to Storage first.
type raftLog struct {
	entries []LogEntry
}

// lastIndex returns the index of the last entry, or 0 for an empty log.
func (l *raftLog) lastIndex() Index {
	return Index(len(l.entries))
}

// lastTerm returns the term of the last entry, or 0 for an empty log.
func (l *raftLog) lastTerm() Term {
	return l.termAt(l.lastIndex())
}

// termAt returns the term of the entry at i. It returns 0 for the sentinel
// index 0 and for indexes beyond the log; callers that need to distinguish
// "absent" from "term 0" must check lastIndex() first (see matches).
func (l *raftLog) termAt(i Index) Term {
	if i == 0 || i > l.lastIndex() {
		return 0
	}
	return l.entries[i-1].Term
}

// entryAt returns the entry at i (1 <= i <= lastIndex).
func (l *raftLog) entryAt(i Index) LogEntry {
	return l.entries[i-1]
}

// entriesFrom returns a deep copy of all entries with Index >= from (see
// CloneEntries), so that outgoing messages never share Command bytes with the
// log. It returns nil when from is beyond the log.
func (l *raftLog) entriesFrom(from Index) []LogEntry {
	if from == 0 {
		from = 1
	}
	if from > l.lastIndex() {
		return nil
	}
	return CloneEntries(l.entries[from-1:])
}

// matches reports whether the log contains an entry at prevIndex with term
// prevTerm — the AppendEntries consistency check. The sentinel index 0 is
// always present with term 0, so (0, 0) matches every log and (0, t≠0)
// matches none: a Leader never produces the latter, so it is rejected like
// any other mismatch instead of being special-cased.
func (l *raftLog) matches(prevIndex Index, prevTerm Term) bool {
	if prevIndex > l.lastIndex() {
		return false
	}
	return l.termAt(prevIndex) == prevTerm
}

// append adds deep copies of entries to the end of the log (the log owns its
// Command bytes). Each entry's Index must equal the current lastIndex + 1; a
// violation is a programming error.
func (l *raftLog) append(entries ...LogEntry) {
	for _, e := range CloneEntries(entries) {
		if e.Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("raft: append index %d, want %d", e.Index, l.lastIndex()+1))
		}
		l.entries = append(l.entries, e)
	}
}

// truncateSuffix deletes every entry with Index >= from. Truncating beyond
// the log is a no-op; from == 0 empties the log.
func (l *raftLog) truncateSuffix(from Index) {
	if from == 0 {
		l.entries = nil
		return
	}
	if from > l.lastIndex() {
		return
	}
	l.entries = l.entries[:from-1]
}
