package simulator

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// invariants is the Raft safety checker. It observes every node's Status
// after every core input and remembers just enough history to catch:
//
//   - Election Safety: at most one Leader per term, across the whole run
//     (not only at one instant — a second, later Leader of a term that
//     already had one is a violation too).
//   - Term Monotonicity: a node's currentTerm never decreases.
//   - Vote Safety: within one term a node never votes for two different
//     nodes. Every non-empty vote is compared against the first one observed
//     for that node and term, not against the previous observation, so a
//     vote that is cleared and then re-granted to someone else in the same
//     term is still caught.
//   - Log Matching: for every pair of nodes, if their logs hold an entry
//     with the same index and term then that entry and every earlier one
//     are identical, command included (Raft §5.3). Checked on the highest
//     such index of each pair, which covers all lower ones.
//   - Leader Append-Only: while a node stays Leader of a term, its log only
//     grows: the log observed last time must be a prefix of the current one
//     (Raft §5.3). Tracked per (node, term), so a node that loses and later
//     regains leadership in a new term starts a fresh history.
//   - Storage Consistency: Status().LastLogIndex/LastLogTerm agree with the
//     last entry in the node's Storage. Not a Raft invariant but the
//     precondition for trusting the two above, which read Storage.
//   - Monotonic Progress: a node's commitIndex and lastApplied never
//     decrease.
//   - Apply/Commit Bounds: lastApplied <= commitIndex <= lastLogIndex.
//   - State Machine Safety, on the log: every node holds the same entry at
//     every index it has committed. The committed log is recorded from the
//     first node seen to commit each index, and every later observation is
//     compared against it.
//   - Leader Completeness: a Leader holds every committed entry whose
//     commit was observed in an earlier term than its own (Raft §5.4). Each
//     recorded entry keeps the term of the node that first reported it
//     committed; that node's term is at least the term in which the entry
//     was committed, so requiring it of Leaders of later terms is sound.
//     A deposed Leader of an older term may lack it, which is legal.
//   - State Machine Safety, on what was applied: two nodes that apply the
//     same index apply the same entry (same term) with the same result.
//     Each node's apply history (its Applied actions) is compared against
//     the first one recorded for every index.
//   - Apply Order: a node's Applied actions go strictly in index order,
//     1, 2, 3, ...
//
// The log invariants read each node's Storage and are skipped for nodes
// without one (scripted cores) and for nodes whose store is closed: the
// log is on "disk" but unreadable until the node restarts over the store
// (issue #9), so there is nothing to compare this round. Skipping is safe
// because a closed store also rejects every write, so the log it holds is
// the one that was already checked.
//
// Violations are logged to the timeline and accumulated; they are never
// repaired and never stop the simulation, so a test sees the full timeline
// that led to the first violation.
type invariants struct {
	leaders    map[raft.Term]raft.NodeID   // first Leader observed in each term
	votes      map[voteKey]raft.NodeID     // first non-empty vote observed per node and term
	last       map[raft.NodeID]raft.Status // last Status observed per node
	leaderLogs map[voteKey][]raft.LogEntry // last log observed per Leader and term
	committed  []committedEntry            // the committed log: index i at position i-1
	applied    []raft.Applied              // first Applied recorded per index: index i at position i-1
	checked    map[raft.NodeID]int         // how much of each node's apply history has been checked
	violations []error
}

// committedEntry is an entry of the committed log, with the term of the
// node that was first seen to have committed it.
type committedEntry struct {
	entry raft.LogEntry
	term  raft.Term
}

// voteKey identifies one node's vote in one term.
type voteKey struct {
	node raft.NodeID
	term raft.Term
}

// checkInvariants observes the current state of every node and records any
// violation. It is called by SimNode.drive after every core input and by
// AssertInvariants.
func (c *Cluster) checkInvariants() {
	if c.inv.leaders == nil {
		c.inv.leaders = make(map[raft.Term]raft.NodeID)
		c.inv.votes = make(map[voteKey]raft.NodeID)
		c.inv.last = make(map[raft.NodeID]raft.Status)
		c.inv.leaderLogs = make(map[voteKey][]raft.LogEntry)
		c.inv.checked = make(map[raft.NodeID]int)
	}
	logs := make(map[raft.NodeID][]raft.LogEntry, len(c.ids))
	statuses := make(map[raft.NodeID]raft.Status, len(c.ids))
	for _, id := range c.ids {
		n := c.nodes[id]
		st := n.Status()
		statuses[id] = st
		if log, err := n.loadLog(); err == nil {
			logs[id] = log
			c.checkLogInvariants(id, st, log)
		}
		if st.Role == raft.Leader {
			if other, seen := c.inv.leaders[st.Term]; !seen {
				c.inv.leaders[st.Term] = id
			} else if other != id {
				c.violate("Election Safety", "term %d has Leaders %s and %s", st.Term, other, id)
			}
		}
		if st.VotedFor != raft.None {
			key := voteKey{id, st.Term}
			if first, seen := c.inv.votes[key]; !seen {
				c.inv.votes[key] = st.VotedFor
			} else if first != st.VotedFor {
				c.violate("Vote Safety", "node %s voted for %s and then %s in term %d", id, first, st.VotedFor, st.Term)
			}
		}
		if prev, seen := c.inv.last[id]; seen {
			if st.Term < prev.Term {
				c.violate("Term Monotonicity", "node %s went from term %d to %d", id, prev.Term, st.Term)
			}
			if st.CommitIndex < prev.CommitIndex || st.LastApplied < prev.LastApplied {
				c.violate("Monotonic Progress", "node %s went from commitIndex %d, lastApplied %d to %d, %d",
					id, prev.CommitIndex, prev.LastApplied, st.CommitIndex, st.LastApplied)
			}
		}
		if st.LastApplied > st.CommitIndex || st.CommitIndex > st.LastLogIndex {
			c.violate("Apply/Commit Bounds", "node %s has lastApplied %d, commitIndex %d, lastLogIndex %d",
				id, st.LastApplied, st.CommitIndex, st.LastLogIndex)
		}
		c.inv.last[id] = st
		c.checkApplied(id, n.applied)
	}
	c.checkCommitted(logs, statuses)
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			if la, ok := logs[a]; ok {
				if lb, ok := logs[b]; ok {
					c.checkLogMatching(a, la, b, lb)
				}
			}
		}
	}
}

// checkLogInvariants checks one node's Storage against its Status and, for a
// Leader, against the log it had last time it was seen leading this term.
func (c *Cluster) checkLogInvariants(id raft.NodeID, st raft.Status, log []raft.LogEntry) {
	var lastIndex raft.Index
	var lastTerm raft.Term
	if n := len(log); n != 0 {
		lastIndex, lastTerm = log[n-1].Index, log[n-1].Term
	}
	if st.LastLogIndex != lastIndex || st.LastLogTerm != lastTerm {
		c.violate("Storage Consistency", "node %s reports last log (%d, %d) but its storage ends at (%d, %d)",
			id, st.LastLogIndex, st.LastLogTerm, lastIndex, lastTerm)
	}
	if st.Role != raft.Leader {
		return
	}
	key := voteKey{id, st.Term}
	if prev, seen := c.inv.leaderLogs[key]; seen && !isPrefix(prev, log) {
		c.violate("Leader Append-Only", "Leader %s of term %d changed its log from %s to %s",
			id, st.Term, describeLog(prev), describeLog(log))
	}
	c.inv.leaderLogs[key] = log
}

// checkCommitted extends the committed log with every index some node has
// newly committed, and then checks every node against it: the entries a
// node has committed must be the recorded ones (State Machine Safety), and
// a Leader must hold every entry committed before its term (Leader
// Completeness). All nodes extend the record before any is checked, so a
// Leader is compared against what the whole cluster has committed so far.
func (c *Cluster) checkCommitted(logs map[raft.NodeID][]raft.LogEntry, statuses map[raft.NodeID]raft.Status) {
	for _, id := range c.ids {
		log, ok := logs[id]
		if !ok {
			continue
		}
		st := statuses[id]
		for i := len(c.inv.committed); i < int(min(st.CommitIndex, raft.Index(len(log)))); i++ {
			c.inv.committed = append(c.inv.committed, committedEntry{entry: log[i], term: st.Term})
		}
	}
	for _, id := range c.ids {
		log, ok := logs[id]
		if !ok {
			continue
		}
		st := statuses[id]
		for i := range min(int(st.CommitIndex), len(log), len(c.inv.committed)) {
			if want := c.inv.committed[i].entry; !sameEntry(log[i], want) {
				c.violate("State Machine Safety", "node %s committed %s at index %d, but %s was committed there",
					id, describeLog(log[i:i+1]), want.Index, describeLog([]raft.LogEntry{want}))
				break
			}
		}
		if st.Role != raft.Leader {
			continue
		}
		for i, ce := range c.inv.committed {
			if ce.term >= st.Term {
				continue
			}
			if i >= len(log) || !sameEntry(log[i], ce.entry) {
				c.violate("Leader Completeness", "Leader %s of term %d lacks entry %s, committed by term %d",
					id, st.Term, describeLog([]raft.LogEntry{ce.entry}), ce.term)
				break
			}
		}
	}
}

// checkApplied checks the part of node id's apply history not checked yet:
// each Applied must follow the previous one in index order (Apply Order)
// and agree in term and result with the first Applied recorded for its
// index on any node (State Machine Safety).
func (c *Cluster) checkApplied(id raft.NodeID, history []raft.Applied) {
	for k := c.inv.checked[id]; k < len(history); k++ {
		a := history[k]
		var prev raft.Index
		if k > 0 {
			prev = history[k-1].Index
		}
		if a.Index != prev+1 {
			c.violate("Apply Order", "node %s applied entry %d after entry %d", id, a.Index, prev)
			continue
		}
		switch i := int(a.Index) - 1; {
		case i < len(c.inv.applied):
			if want := c.inv.applied[i]; a.Term != want.Term || !bytes.Equal(a.Result, want.Result) {
				c.violate("State Machine Safety", "node %s applied entry %d from term %d with result %q, another node applied it from term %d with result %q",
					id, a.Index, a.Term, a.Result, want.Term, want.Result)
			}
		case i == len(c.inv.applied):
			c.inv.applied = append(c.inv.applied, a)
		}
		// i > len: an earlier index of this node was out of order and is
		// already reported; there is nothing to compare against.
	}
	c.inv.checked[id] = len(history)
}

// checkLogMatching compares the logs of nodes a and b at the highest index
// where they hold the same term: everything up to and including that index
// must be identical.
func (c *Cluster) checkLogMatching(a raft.NodeID, la []raft.LogEntry, b raft.NodeID, lb []raft.LogEntry) {
	for i := min(len(la), len(lb)) - 1; i >= 0; i-- {
		if la[i].Term != lb[i].Term {
			continue
		}
		if !isPrefix(la[:i+1], lb) {
			c.violate("Log Matching", "nodes %s and %s agree on entry (%d, %d) but their logs differ before it: %s vs %s",
				a, b, la[i].Index, la[i].Term, describeLog(la[:i+1]), describeLog(lb[:i+1]))
		}
		return
	}
}

// isPrefix reports whether every entry of prefix is present, identically, at
// the same position in log.
func isPrefix(prefix, log []raft.LogEntry) bool {
	if len(prefix) > len(log) {
		return false
	}
	for i := range prefix {
		if !sameEntry(prefix[i], log[i]) {
			return false
		}
	}
	return true
}

// describeLog renders a log as [index:term ...] for violation messages.
func describeLog(log []raft.LogEntry) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, e := range log {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%d:%d", e.Index, e.Term)
	}
	sb.WriteByte(']')
	return sb.String()
}

func (c *Cluster) violate(invariant, format string, args ...any) {
	err := fmt.Errorf("%s violated at t=%v: %s", invariant, c.clock.Now(), fmt.Sprintf(format, args...))
	c.logger.Info("invariant-violation", "invariant", invariant, "detail", fmt.Sprintf(format, args...))
	c.inv.violations = append(c.inv.violations, err)
}

// AssertInvariants checks the current state once more and returns every
// violation observed so far, joined, or nil. Tests call it at the end (the
// test helper does so automatically) and after important events.
func (c *Cluster) AssertInvariants() error {
	c.checkInvariants()
	return errors.Join(c.inv.violations...)
}
