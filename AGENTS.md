# AGENTS.md

## Project Purpose

This repository implements a minimal, educational Raft consensus system in Go.

The goal is to learn Raft by implementing the protocol directly and testing its safety properties under failures. This is not intended to be a production-grade replacement for etcd or another mature consensus system.

Priorities, in order:

1. Correctness
2. Testability and determinism
3. Readability and traceability to the Raft paper
4. Simplicity
5. Performance

When a design trade-off conflicts with Raft safety, safety wins.

---

## Technology Constraints

- Language: Go
- Persistence: local filesystem + JSON
- Prefer the Go standard library exclusively.
- Do not add third-party libraries merely for convenience.
- Google-authored Go libraries may be used only when there is a clear need and the standard library is insufficient.
- Do not add test assertion frameworks, mocking frameworks, CLI frameworks, logging frameworks, HTTP routers, configuration frameworks, protobuf, or gRPC without explicit justification.
- Prefer:
  - `testing`
  - `net/http`
  - `httptest`
  - `encoding/json`
  - `os`
  - `io`
  - `sync`
  - `context`
  - `log/slog`

Before adding any dependency, inspect:

```bash
go list -m all
```

If a new non-standard-library dependency appears, stop and document why it is necessary.

---

## MVP Scope

The MVP supports a fixed 3-node or 5-node Raft cluster with:

- Follower / Candidate / Leader roles
- Terms and voting
- RequestVote RPC
- Leader election
- AppendEntries RPC
- Heartbeats
- Log replication
- Log conflict detection and truncation
- `nextIndex` / `matchIndex`
- Majority commit
- State machine apply
- A simple key-value state machine
- Local file persistence
- Crash and restart recovery
- Deterministic failure simulation
- A real local multi-process demo

At minimum, the client supports:

```text
SET key value
```

`GET` may be implemented, but MVP reads do not need to be linearizable.

### Explicit Non-Goals

Do not implement these unless a later issue explicitly asks for them:

- snapshots
- log compaction
- dynamic membership
- joint consensus
- PreVote
- leader transfer
- ReadIndex
- leader leases
- linearizable reads
- batching or replication pipelining
- production WAL segmentation
- TLS/authentication
- service discovery
- Kubernetes integration
- Multi-Raft

Do not over-engineer ahead of the current issue.

---

## Architecture

Raft consensus logic must remain separate from transport, storage, time, and the state machine.

Preferred dependency direction:

```text
Client
  |
  v
Raft Core
  |-- Storage
  |-- Transport
  |-- Clock / Timer abstraction
  `-- State Machine
```

Suggested repository layout:

```text
cmd/
  raftnode/
  raftctl/

internal/
  raft/
  storage/
  transport/
  simulator/
  statemachine/
```

Adapt this layout to the repository when necessary, but preserve clear package boundaries.

### Raft Core Rules

The Raft core must not:

- depend directly on HTTP
- depend directly on a concrete on-disk format
- call `time.Sleep` as part of protocol logic
- perform network I/O while holding a consensus-state mutex
- rely on logging for correctness

Prefer a deterministic, event-driven design where practical:

```text
Event -> Raft state transition -> Actions
```

Time, network delivery, and storage must be replaceable in tests.

---

## Core State

Every node maintains at least:

```text
id
role
currentTerm
votedFor
log
commitIndex
lastApplied
```

Roles:

```text
Follower
Candidate
Leader
```

A log entry contains at least:

```text
Index
Term
Command
```

A Leader additionally maintains:

```text
nextIndex[peer]
matchIndex[peer]
```

Persistent state:

```text
currentTerm
votedFor
log
```

Volatile state:

```text
commitIndex
lastApplied
nextIndex
matchIndex
```

---

## Raft Correctness Requirements

The following invariants are non-negotiable and should be asserted in tests whenever possible.

### Election Safety

At most one valid Leader may exist in a given term.

### Vote Safety

A node may grant at most one vote per term.

### Term Monotonicity

`currentTerm` never decreases.

When an RPC or response contains a term greater than `currentTerm`, the node must update its term, become a Follower, and persist the required state before proceeding.

### Leader Append-Only

A Leader never overwrites or deletes entries in its own log.

### Log Matching

If two logs contain entries with the same index and term, the preceding entries must match.

### Leader Completeness

Committed entries must be present in the logs of future Leaders.

### State Machine Safety

If two nodes apply the same log index, they must apply the same command.

### Monotonic Progress

These values must never decrease:

```text
currentTerm
commitIndex
lastApplied
```

Always maintain:

```text
lastApplied <= commitIndex
commitIndex <= lastLogIndex
```

---

## Critical Protocol Semantics

### RequestVote

A Candidate's log is at least as up-to-date as the receiver's log when:

```text
candidateLastLogTerm > localLastLogTerm
```

or:

```text
candidateLastLogTerm == localLastLogTerm &&
candidateLastLogIndex >= localLastLogIndex
```

Compare the last log term first. Only compare indexes when the terms are equal.

### AppendEntries

Followers must validate:

```text
prevLogIndex
prevLogTerm
```

If an existing entry conflicts with a Leader entry at the same index but a different term, delete that entry and every entry after it before appending the Leader's suffix.

The simplest acceptable replication retry strategy for MVP is:

```text
nextIndex[peer]--
```

followed by retry.

Do not implement conflict-term optimization unless an issue explicitly asks for it.

### Commit Rule

A Leader may advance `commitIndex` to candidate index `N` by majority counting only when both conditions hold:

```text
majority(matchIndex >= N)
log[N].Term == currentTerm
```

Do not directly commit an older-term entry solely because it is currently stored on a majority. Older entries become committed indirectly when a current-term entry after them is committed.

### Apply Rule

Apply entries strictly in index order from:

```text
lastApplied + 1
```

through:

```text
commitIndex
```

For the MVP, report a successful client mutation only after the command is committed and applied.

---

## Persistence

Define a storage abstraction before depending on a concrete filesystem implementation.

The concrete MVP storage is `FileStorage` using only the Go standard library.

It must preserve across crash/restart:

```text
currentTerm
votedFor
log
```

Keep the durability design simple and explainable. Favor correctness over throughput.

Acceptable techniques include temporary files, `fsync`, and atomic rename where appropriate.

Do not build a production-grade segmented WAL for the MVP.

Document durability guarantees and known failure modes.

Persistence ordering that affects Raft safety must be covered by tests or explained explicitly.

---

## Simulator-First Development

Implement and use a deterministic simulator before relying on real networking.

The simulator should support:

- explicit message delivery
- message drop
- message delay
- message duplication
- message reordering
- directed link disconnection
- network partitions
- partition healing
- node crash
- node restart
- fake-clock advancement

Prefer a deterministic event queue.

Randomized tests must log their random seed so failures can be reproduced.

Example:

```text
seed=123456
```

Do not fix flaky consensus tests by adding longer sleeps.

---

## Timing

Election timeouts must be randomized.

Real-process defaults may use values approximately like:

```text
election timeout: 500ms-1000ms
heartbeat interval: 100ms-200ms
```

Tests should use a fake clock and explicit time advancement rather than wall-clock waiting.

---

## Logging

Use `log/slog` unless the repository already has a justified standard-library-based logging approach.

Useful structured fields include:

```text
node
term
role
event
peer
index
prevIndex
prevTerm
commitIndex
lastApplied
```

Logs are for diagnosis only. Protocol behavior must not depend on log output.

---

## Concurrency

Keep concurrency ownership simple.

Prefer a single-owner event loop for mutable consensus state when practical.

If using mutexes:

- document which fields each mutex protects
- keep critical sections small
- do not perform network requests while holding the Raft state lock
- be deliberate about storage I/O ordering relative to state transitions

All concurrency-related work must pass the race detector.

---

## GitHub Issue Workflow

Work is organized under a Raft EPIC and child Issues.

Before implementing an issue:

1. Read the issue body completely.
2. Check its dependencies.
3. Check the current branch and working tree.
4. Do not overwrite unrelated user changes.
5. Implement only the scope of the current issue.
6. Do not pull later-issue functionality forward for convenience.

When the repository uses per-issue branches, use a name such as:

```text
codex/issue-12-raft-core-types
```

One issue should correspond to one coherent change set.

If the repository uses pull requests, prefer one PR per issue and include:

```text
Fixes #<issue-number>
```

Follow existing repository conventions if they are already established.

---

## Per-Issue Development Loop

For every issue:

1. Re-read the issue and acceptance criteria.
2. Add or update tests first when practical.
3. Implement the smallest correct change.
4. Format code.
5. Run focused tests.
6. Run the full suite.
7. Run static checks.
8. Run the race detector for concurrent code.
9. Inspect dependencies.
10. Update relevant docs.
11. Commit with a focused message.
12. Update the issue with design decisions, tests, limitations, and commit/PR reference.

Required commands before considering an implementation complete:

```bash
gofmt -w <changed-go-files>
go test ./...
go vet ./...
go list -m all
```

For concurrency-related changes:

```bash
go test -race ./...
```

Never delete or weaken a legitimate failing test merely to make CI pass.

---

## Testing Expectations

Do not test only the happy path.

Important scenarios include:

- normal 3-node election
- split vote and re-election
- Leader crash and replacement
- old Leader rejoining with a stale term
- rejecting a Candidate with an outdated log
- heartbeat preventing unnecessary elections
- follower catch-up
- divergent follower suffix repair
- repeated `nextIndex` rollback
- uncommitted Leader-local entries
- majority commit
- old-term commit semantics
- isolated Leader unable to commit
- majority partition electing a new Leader
- partition healing and log convergence
- Leader crash before majority replication
- Leader crash after majority replication but before commit notification
- vote persisted across crash/restart
- stale node restart
- duplicated messages
- delayed messages
- reordered messages
- repeated crash/restart
- deterministic randomized fault tests

After important simulated cluster events, call an invariant checker when available.

---

## Real Network Mode

Implement real networking only after the simulator-backed Raft behavior is stable.

Prefer standard-library networking, especially `net/http`, unless the repository documents a better stdlib-based choice.

The real transport must implement the same conceptual interface as the simulated transport.

Adding real networking should not require a large redesign of the Raft core. If it does, improve the abstraction instead of coupling the core to the transport.

---

## Documentation

Keep `README.md` current as functionality is added.

By the end of the MVP it should explain:

- project goal and non-goals
- architecture
- persistent vs volatile Raft state
- safety invariants
- how to run tests
- how to run race detection
- how to run deterministic simulation
- how to start a 3-node local cluster
- how to issue `SET`
- how to kill the Leader
- how to observe re-election
- how to restart the old node
- known limitations

Architecture decisions that materially affect future work may be recorded under:

```text
docs/decisions/
```

Useful ADR topics include:

- simulator-first architecture
- Raft core / transport separation
- why MVP does not use gRPC
- FileStorage durability model
- consensus concurrency model

Keep ADRs short and decision-oriented.

---

## Definition of Done

A change is not done merely because it compiles.

An issue is complete only when:

- its acceptance criteria are satisfied
- relevant tests exist
- `go test ./...` passes
- `go vet ./...` passes
- `go test -race ./...` passes when concurrency is involved
- no unjustified dependencies were added
- relevant documentation is updated
- Raft safety invariants remain intact

The overall MVP is complete only when a local 3-node cluster can:

1. elect a Leader
2. accept and commit `SET`
3. survive Leader failure and elect a replacement
4. prevent a minority partition from committing
5. allow a majority partition to continue committing
6. heal the network and converge logs
7. restart nodes from local persistent state
8. converge all state machines to the same committed state

---

## Agent Behavior

When working in this repository:

- inspect existing instructions before editing code
- respect `README.md`, `CONTRIBUTING.md`, and repository conventions
- do not overwrite unrelated changes
- do not make broad refactors unless the current issue requires them
- prefer explicit, boring, testable code over clever abstractions
- keep implementation close to Raft terminology
- explain non-obvious safety-critical decisions in comments or issue notes
- when uncertain, derive behavior from Raft invariants rather than guessing
- do not weaken correctness to simplify implementation
- stop scope creep early

The project exists to make Raft understandable. Code that is easier to prove correct and easier to study is preferred over code that is merely shorter or faster.
