package simulator

import (
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
	violations []error
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
	}
	logs := make(map[raft.NodeID][]raft.LogEntry, len(c.ids))
	for _, id := range c.ids {
		n := c.nodes[id]
		st := n.Status()
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
		if prev, seen := c.inv.last[id]; seen && st.Term < prev.Term {
			c.violate("Term Monotonicity", "node %s went from term %d to %d", id, prev.Term, st.Term)
		}
		c.inv.last[id] = st
	}
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
