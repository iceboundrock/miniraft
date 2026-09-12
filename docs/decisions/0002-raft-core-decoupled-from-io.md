# ADR 0002: The Raft core is a pure event-driven state machine

## Status

Accepted.

## Context

The core must be drivable by both a deterministic simulator and a real-time
runtime, and its persistence ordering must be easy to audit. Goroutines,
timers, sockets and file formats inside the core would defeat all three.

## Decision

`internal/raft.Node` exposes four inputs — `Step(msg)`, `ElectionTimeout()`,
`HeartbeatTimeout()`, `Propose(cmd)` — and returns `[]Action` from each. Actions
are a closed set of plain values (`SendMessage`, `ResetElectionTimer`,
`StopElectionTimer`, `ResetHeartbeatTimer`, `StopHeartbeatTimer`, `Applied`).
The host executes them after the call returns.

The core reaches durable state and the replicated state machine only through
the `raft.Storage` and `raft.StateMachine` interfaces, which are declared in
package `raft` (the consumer) so that `raft` remains a leaf package and
`internal/storage` / `internal/statemachine` depend on it, not vice versa.
Randomness comes solely from `Config.Rand`, which is required (no default:
a fixed default seed would give every node the same election-timeout sequence);
logging is diagnostic and never affects behavior.

There is deliberately **no `Clock` or `Timer` interface inside the core**.
Time appears only as durations inside `ResetElectionTimer` /
`ResetHeartbeatTimer`; the host owns the timers and calls `ElectionTimeout()` /
`HeartbeatTimeout()` when they fire. The fake clock therefore lives in the
simulator (issue #3) and `time.Timer` in the real runtime, and neither is
visible to `raft`. Log entries are deep-copied at every core boundary
(`raft.CloneEntries`) so hosts and storage never share `Command` bytes with
the log, and validated at every ingress (`raft.ValidateEntries`: contiguous
indexes, non-zero terms) so a malformed entry can never reach a log.

Storage is invoked synchronously inside the core. Because actions are executed
only after the core returns, everything written to storage while handling an
event is durable before any resulting message is sent.

## Consequences

- `Node` is not safe for concurrent use; a single owner serializes calls.
  The production runtime will be a single event-loop goroutine.
- Adding the HTTP transport should not touch the core.
- The simulator can inspect `Status()` and check invariants after every event.
