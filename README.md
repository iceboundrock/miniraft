# miniraft

A minimal, educational implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go.

The purpose of this repository is to *learn Raft by building it*: the code is
written so that every mechanism can be traced back to Figure 2 of the paper,
and every safety property can be checked under simulated failures.

Priorities, in order: **correctness → testability/determinism → readability → simplicity → performance**.

## Goals (MVP)

- Fixed 3- or 5-node cluster.
- Leader election (`RequestVote`), heartbeats and log replication (`AppendEntries`).
- Log consistency check, conflicting-suffix truncation, `nextIndex` / `matchIndex`.
- Majority commit with the current-term restriction; ordered state-machine apply.
- Simple key-value state machine (`SET key value`; non-linearizable `GET`).
- Local file persistence of `currentTerm`, `votedFor` and the log; crash/restart recovery.
- Deterministic simulator with fake clock, message delay/drop/duplication/reordering,
  partitions, crashes and restarts, plus an automatic Raft invariant checker.
- Real `net/http` transport and a 3-process local demo.

## Non-goals

Snapshots, log compaction, dynamic membership, PreVote, leader transfer,
ReadIndex/lease reads, linearizable reads, batching/pipelining, a production
WAL, TLS, authentication, service discovery, gRPC, Multi-Raft.

## Architecture

```
Client
   |
   v
Raft Node (internal/raft) — pure, event-driven: Event -> state transition -> []Action
   |
   +---- Storage        (internal/storage)      persistent state
   +---- Transport      (internal/transport)    real-mode message delivery
   +---- Clock/Timers   (host-provided)         fake clock in tests, time.Timer in production
   +---- State Machine  (internal/statemachine) replicated KV
```

The Raft core owns no goroutines, timers, sockets or files. A *host* feeds it
events (`Step(msg)`, `ElectionTimeout()`, `HeartbeatTimeout()`, `Propose(cmd)`)
and executes the `Action`s it returns (`SendMessage`, `ResetElectionTimer`,
`ResetHeartbeatTimer`, `Applied`, ...). In tests the host is a single-threaded
deterministic simulator; in production it is a real-time event loop.

Storage is called synchronously *inside* the core, so anything persisted while
handling an event is durable before the resulting messages are sent — the
ordering Figure 2 requires ("updated on stable storage before responding to RPCs").

`internal/raft` is the leaf package: it declares the `Storage` and
`StateMachine` interfaces it consumes, and the other packages import it.

### Layout

```
cmd/raftnode/           real node process (later issue)
cmd/raftctl/            client CLI (later issue)
internal/raft/          Raft core: types, log, node, actions
internal/storage/       Storage implementations (MemoryStorage now, FileStorage later)
internal/statemachine/  State machine implementations (KV later)
internal/simulator/     deterministic simulator (later issue)
internal/transport/     real transport (HTTP, later issue)
docs/decisions/         architecture decision records
```

### Raft state

| Persistent (via `Storage`) | Volatile | Leader-only volatile |
|---|---|---|
| `currentTerm`, `votedFor`, `log[]` | `commitIndex`, `lastApplied` | `nextIndex[]`, `matchIndex[]` |

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

The module uses only the Go standard library; `go list -m all` should print the
module name alone.

Work is tracked in the [EPIC issue](https://github.com/iceboundrock/miniraft/issues/1);
one branch and one pull request per child issue.
