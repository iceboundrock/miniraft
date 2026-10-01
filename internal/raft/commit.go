package raft

import (
	"fmt"
	"slices"
)

// This file implements commitment and application (§5.3, §5.4.2): the
// Leader's majority commit rule with the current-term restriction of Figure
// 8, the follower's LeaderCommit rule, and the strictly ordered apply loop
// that every input ends with.

// maybeAdvanceCommit is the Leader's commit rule (Figure 2, "Rules for
// Servers / Leaders"): commitIndex becomes the largest N > commitIndex such
// that a majority of matchIndex[i] >= N (the Leader's own matchIndex
// included) and log[N].Term == currentTerm. It is called whenever a
// matchIndex grows.
//
// The majority part: in the sorted matchIndex values, the quorum()-th
// largest is the highest index that at least quorum() nodes hold.
//
// The term part is §5.4.2 (Figure 8): an entry from an earlier term can be
// on a majority and still be overwritten by a later Leader, so counting
// replicas never commits it. It is committed indirectly — by Log Matching,
// every entry before a committed one is committed — once an entry of the
// current term after it is on a majority. Walking N down from the majority
// index is the literal search for the rule's N; it stops at commitIndex.
func (n *Node) maybeAdvanceCommit() {
	matched := make([]Index, 0, len(n.peers)+1)
	matched = append(matched, n.matchIndex[n.id])
	for _, p := range n.peers {
		matched = append(matched, n.matchIndex[p])
	}
	slices.Sort(matched)
	majority := matched[len(matched)-n.quorum()]
	for N := majority; N > n.commitIndex; N-- {
		if n.log.termAt(N) == n.currentTerm {
			n.commitIndex = N
			n.logger.Info("CommitAdvance", "term", n.currentTerm, "role", n.role, "commitIndex", N)
			return
		}
	}
}

// followerCommit is the follower's commit rule (Figure 2, AppendEntries
// receiver step 5), applied after a successful AppendEntries: commitIndex =
// min(leaderCommit, lastNew), where lastNew is the index of the last entry
// the message covered (PrevLogIndex + len(Entries)).
//
// The bound is lastNew and not the local last index: only up to lastNew has
// this message proved that the log matches the Leader's. Entries beyond it
// may be a stale suffix from an earlier term that the Leader has not
// reached yet, and LeaderCommit speaks about the Leader's log, not about
// them. commitIndex never decreases: a delayed AppendEntries for a shorter
// prefix carries a smaller bound.
func (n *Node) followerCommit(leaderCommit, lastNew Index) {
	if c := min(leaderCommit, lastNew); c > n.commitIndex {
		n.commitIndex = c
		n.logger.Info("CommitAdvance", "term", n.currentTerm, "role", n.role, "commitIndex", c)
	}
}

// finish ends every input (Step, ElectionTimeout, HeartbeatTimeout,
// Propose). Once the input's own transitions have succeeded — every storage
// write included — newly committed entries are applied, and their Applied
// actions come after the input's other actions, so no Applied ever precedes
// the durable state it depends on. A failed input is returned unchanged; it
// did not advance commitIndex, and anything already committed is applied
// by the next input.
func (n *Node) finish(actions []Action, err error) ([]Action, error) {
	if err != nil {
		return actions, err
	}
	applied, err := n.applyCommitted()
	return append(actions, applied...), err
}

// applyCommitted applies every committed entry not yet applied, strictly in
// index order lastApplied+1 .. commitIndex (Figure 2, "Rules for Servers /
// All Servers"), handing the state machine a deep copy of each (entryAt).
// Each one is reported as an Applied action carrying the entry's term, so a
// host waiting on a proposal can tell its own entry from one that replaced
// it.
//
// If the state machine fails, lastApplied stays just below the failed entry
// and the error is returned with the Applied actions produced so far; the
// next input tries the same entry again. Skipping it instead would make this
// node's state depend on how it handled the error, which the replicated log
// does not record (see StateMachine).
func (n *Node) applyCommitted() ([]Action, error) {
	var actions []Action
	for n.lastApplied < n.commitIndex {
		entry := n.log.entryAt(n.lastApplied + 1)
		result, err := n.sm.Apply(entry)
		if err != nil {
			return actions, fmt.Errorf("raft: apply entry %d: %w", entry.Index, err)
		}
		n.lastApplied = entry.Index
		n.logger.Info("Apply", "term", n.currentTerm, "role", n.role,
			"index", entry.Index, "lastApplied", n.lastApplied, "commitIndex", n.commitIndex)
		actions = append(actions, Applied{Index: entry.Index, Term: entry.Term, Result: result})
	}
	return actions, nil
}
