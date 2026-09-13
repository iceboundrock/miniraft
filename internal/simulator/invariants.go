package simulator

import (
	"errors"
	"fmt"

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
//
// Violations are logged to the timeline and accumulated; they are never
// repaired and never stop the simulation, so a test sees the full timeline
// that led to the first violation.
type invariants struct {
	leaders    map[raft.Term]raft.NodeID   // first Leader observed in each term
	votes      map[voteKey]raft.NodeID     // first non-empty vote observed per node and term
	last       map[raft.NodeID]raft.Status // last Status observed per node
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
	}
	for _, id := range c.ids {
		st := c.nodes[id].Status()
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
