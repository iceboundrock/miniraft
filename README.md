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
internal/simulator/     deterministic simulator: fake clock, event queue, network, cluster harness
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

### Deterministic simulation

`internal/simulator` runs a whole cluster in one goroutine on a fake clock, so
every test is reproducible from its seed. Its pieces:

- `EventQueue` — min-heap ordered by `(At, Seq)`; ties break by creation order.
- `Clock` — `Now()`, `Advance(d)`, `After(d, fn) TimerID`, `Stop(id)`.
- `Network` — per-node inboxes; `Send` schedules delivery after a fixed or
  seeded-random latency. Faults: `Drop`, `Disconnect`/`Reconnect` (directional),
  `Isolate`, `Partition`/`Heal`, and an off-by-default `SetDuplicate` hook.
  Link policy is checked at send *and* at delivery, so messages already in
  flight are lost when a partition or disconnect appears. Every scheduled
  delivery carries its own deep copy of the message, as serialization would
  on a real transport.
- `Cluster` — hosts N `*raft.Node`s over `MemoryStorage`, translates the
  `Action`s they return into timers and sends, and drives everything with
  `Run(until)`, `Step()` or `RunUntil(pred, maxTime)`.

Every event is logged on a timeline stamped with the fake clock:

```
t=0 event=cluster seed=1 nodes=3
t=150 event=ElectionTimeout node=a
t=152 event=send from=a to=b type=RequestVote term=1 latency=5ms
t=157 event=deliver from=a to=b type=RequestVote term=1
```

Every simulator test logs `seed=<n>`. To replay a failure, construct the
cluster with the same `Config.Seed` (or set it in the test) and run it again:

```sh
go test ./internal/simulator/ -run TestDeterministicReplay -v
```

Out of scope until later issues: node crash/restart, message reordering and
random drop rates, and the invariant checker.

Work is tracked in the [EPIC issue](https://github.com/iceboundrock/miniraft/issues/1);
one branch and one pull request per child issue.
