# ADR 0004: Proposal lifecycle and apply errors

## Status

Accepted.

## Context

A client `SET` may only be acknowledged once it is committed and applied.
The core is a pure state machine (ADR 0002): it does not track client
requests, and it cannot block until a proposal's fate is known. A
proposal's entry can also vanish. A Leader that appends it and then loses
leadership before replicating it to a majority will have it overwritten by
the next Leader's entry at the same index.

The state machine is outside the core's control, so `Apply` can fail. Every
replica must reach the same state from the same log (State Machine Safety),
whatever an individual node does with that error.

## Decision

**Proposal lifecycle.** `Node.Propose(cmd)` returns `(index, term, actions,
err)`. Every node applies committed entries in order at the end of each
input and reports each one as `Applied{Index, Term, Result}`. The host that
proposed waits for `Applied` at that index:

- the same `Term`: the proposal was committed and applied, and `Result` is
  the answer;
- a different `Term`: another entry was committed at that index, and by
  State Machine Safety the proposal can never be applied. The host reports
  `raft.ErrProposalLost`, which the core defines so that every host reports
  it alike.

(index, term) identifies an entry uniquely: at most one Leader exists per
term (Election Safety), and a Leader never rewrites its own log. No other
request ID is needed. In the simulator the host is `Cluster.Submit` /
`Cluster.Await`; the real runtime will do the same.

There is no client retry and no deduplication. A host that stops waiting
reports a timeout, and the command may still be committed later.
Idempotent client IDs are out of MVP scope.

**Apply errors are fail-stop.** `StateMachine.Apply` must be deterministic
and must leave the state unchanged when it returns an error. When it fails,
the core keeps `lastApplied` just below that entry and returns the error to
the host, together with the actions already produced. The next input tries
the same entry again. The core never skips an entry: whether an entry was
applied must depend only on the log, never on how one node handled an
error.

A committed entry that can never apply would therefore halt apply on every
node. Hosts must validate commands before proposing them
(`statemachine.Decode` for the KV).

## Consequences

- Success is reported strictly after commit and apply on the proposing node.
  Followers apply the same entry once they learn the new `commitIndex`,
  usually on the next `AppendEntries`.
- A lost proposal is detected when the old Leader rejoins and applies the
  replacement entry. While it stays partitioned, its client sees only a
  timeout.
- Liveness depends on the state machine accepting every committed command.
  Malformed input is rejected at the client boundary, not inside the log.
- `commitIndex` and `lastApplied` are volatile. Without snapshots, a node
  restarted over its storage (issue #9) rebuilds its state by re-applying
  the committed log from index 1. It therefore needs a fresh, empty state
  machine, not the one it had before the crash.
