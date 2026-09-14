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

The Raft core owns no goroutines, timers, sockets or files. A *host* arms the
first election timer with `Start()`, then feeds it events (`Step(msg)`,
`ElectionTimeout()`, `HeartbeatTimeout()`, `Propose(cmd)`) and executes the
`Action`s it returns (`SendMessage`, `ResetElectionTimer`,
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
internal/storage/       Storage implementations: MemoryStorage, FileStorage; storagetest conformance suite
internal/statemachine/  State machine implementations (KV later)
internal/simulator/     deterministic simulator: fake clock, event queue, network, cluster harness
internal/transport/     real transport (HTTP, later issue)
docs/decisions/         architecture decision records
```

### Raft state

| Persistent (via `Storage`) | Volatile | Leader-only volatile |
|---|---|---|
| `currentTerm`, `votedFor`, `log[]` | `commitIndex`, `lastApplied` | `nextIndex[]`, `matchIndex[]` |

### Leader election

`internal/raft/election.go` implements §5.2 and the election restriction of
§5.4.1. The flow, with the persistence points marked:

```
Follower ──ElectionTimeout──▶ Candidate: term+1, votedFor=self  [SaveTermVote]
                                  │  RequestVote(term, lastLogIndex, lastLogTerm) → every peer
                                  │  ResetElectionTimer(random)
   voter: grant iff term == currentTerm
          && votedFor ∈ {none, candidate}
          && candidate log up-to-date          [SaveTermVote before replying]
                                  │
        majority of RequestVoteResponse{granted} ──▶ Leader: nextIndex/matchIndex reset,
                                                             StopElectionTimer, ResetHeartbeatTimer
        any message with a higher term ──▶ Follower in that term, votedFor=none  [SaveTermVote]

Leader ──HeartbeatTimeout──▶ AppendEntries(term, prevLogIndex, prevLogTerm, log[nextIndex:]) → every peer
                             ResetHeartbeatTimer
   follower: term < currentTerm → Success=false, timer untouched
             else leaderID=sender, ResetElectionTimer,
                  Success = log has (prevLogIndex, prevLogTerm)  (see Log replication)
```

Rules worth knowing because they are easy to get subtly wrong:

- **Log up-to-date** (`candidateLogUpToDate`): compare the last log *term*
  first; only equal terms compare the index. A longer log with an older last
  term loses.
- **One vote per term**: `votedFor` is persisted before the response is
  produced, so a crash between the two cannot lead to a second vote.
- **Envelope identity is checked before the vote decision**: `Message.Validate`
  requires a sender and a destination and rejects a `RequestVote` whose
  `CandidateID` is empty or differs from `From` (the voter persists
  `CandidateID` but replies to `From`, so a mismatch would let one node
  count a vote durably cast for another, and an empty `CandidateID` would
  be persisted as "no vote" and leave the term open for a second grant).
  `Step` then rejects any message not addressed to this node — every Raft
  RPC is point-to-point, so a misrouted or replayed `RequestVoteResponse`
  granted to another candidate must not be counted as this node's vote —
  and any message whose sender is not a peer, both before the higher-term
  rule runs, so neither can advance the term or be counted. A rejected
  message changes nothing and yields no actions; the host gets an error.
- **Vote tally is a set** keyed by voter, so a duplicated response cannot
  count twice.
- **Election timer resets** happen only when a node starts an election,
  grants a vote, or accepts an `AppendEntries` from the current Leader. A *denied*
  `RequestVote` never resets the timer, even when it carried a higher term —
  otherwise a node with a stale log could keep the cluster from electing
  anyone. The one exception is a Leader stepping down: it has no election
  timer running (it was stopped on election), so `becomeFollower` arms one
  and stops the heartbeat timer.
- **Terms never decrease**: `becomeFollower(term)` panics on a lower term,
  and a node whose `currentTerm` is already the maximum `Term` refuses to
  start an election (`ErrTermOverflow`) rather than wrap to 0.
- **Storage first**: `currentTerm`/`votedFor` are written before the
  in-memory copies change; a storage error leaves that transition untouched
  and produces no actions from it. `Step` is a step-down followed by the
  message handler, each atomic on its own: if the step-down persisted and
  only the handler's write failed, the step-down actions are still returned
  with the error, so a deposed Leader's `StopHeartbeatTimer` and
  `ResetElectionTimer` reach the host.

### Heartbeats

A Leader keeps its authority by sending an `AppendEntries` to every peer
each time its heartbeat timer fires (`HeartbeatTimeout()`), then re-arming
the timer. A heartbeat is not a special message: it is the same
`sendAppendEntries(peer)` used by replication, so it carries whatever
entries the peer is missing (`log[nextIndex[peer]:]`) and is empty only when
the peer is up to date. `Config` requires `HeartbeatInterval <= ElectionTimeoutMin/3`,
so a follower has to miss several heartbeats in a row before it starts an
election. `internal/raft/replication.go` holds the receiver side:

- **Stale term** (`Term < currentTerm`): reply `Success=false` with the
  current term and change nothing — in particular the election timer is not
  reset, so a deposed Leader cannot keep followers from timing out.
- **Higher term**: `Step` has already made the node a Follower in that term
  (persisted) before the handler runs.
- **Same term, Candidate**: someone else won; become Follower (the vote for
  itself stays, nothing to re-persist). A *Leader* receiving another
  Leader's `AppendEntries` in its own term is an Election Safety violation
  and is reported as an error with no state change.
- **Recognize the Leader**: record `LeaderID` (visible in `Status`) and reset
  the election timer. This happens on *every* accepted `AppendEntries`, and
  even when the consistency check below fails — a legitimate Leader whose
  log differs from ours is still the Leader; the log is repaired by
  replication, not by an election.
- **Consistency check**: `raftLog.matches(PrevLogIndex, PrevLogTerm)`
  decides `Success`. Index 0 always matches, so an `AppendEntries` to an
  empty log succeeds.

The simulator's `Cluster.StopHeartbeats(id)` drops that node's outgoing
`AppendEntries` — and nothing else — to simulate a stalled Leader;
`Cluster.RunFor(d)` advances the clock by a duration. Multi-round election
runs are issue #17.

### Log replication

`Propose(cmd)` on the Leader appends `{Index: lastIndex+1, Term: currentTerm, cmd}`
to storage and memory, sets `matchIndex[self]`, and returns the new index
together with one `AppendEntries` per peer — the entry goes out immediately,
not on the next heartbeat. On any other node it returns a `*NotLeaderError`
(`errors.Is(err, ErrNotLeader)`) carrying the Leader hint the node last
heard from, or `None` if it knows of none. §5.3 of the paper, as implemented
in `internal/raft/replication.go`:

```
Leader                                            Follower
  nextIndex[p]=lastIndex+1, matchIndex[p]=0 on election
  send prev=(nextIndex-1, term at it), entries=log[nextIndex:]
                                        ──▶       log has (prevIndex, prevTerm)?
                                                    no  → Success=false                 [nothing changes]
                                                    yes → for each entry, in order:
                                                            index beyond my log   → append the rest       [AppendEntries]
                                                            same index, same term → skip (already have it)
                                                            same index, other term→ TruncateSuffix(index)  [TruncateSuffix]
                                                                                    then append the rest   [AppendEntries]
                                                          Success=true, MatchIndex=prevIndex+len(entries)
  Success=false → nextIndex[p] = max(nextIndex[p]-1, matchIndex[p]+1), resend at once
  Success=true  → matchIndex[p] = max(matchIndex[p], MatchIndex), nextIndex[p] = matchIndex[p]+1
```

Rules worth knowing:

- **Skip, don't truncate, on a same-term entry**: an `AppendEntries` may be
  duplicated or arrive after a newer one has already appended more. Deleting
  everything after `prevLogIndex` and re-appending would throw away the
  newer entries; Figure 2 says to truncate only at a *conflicting* entry
  (same index, different term), and only from there. The follower is
  therefore idempotent: replaying any earlier `AppendEntries` is a no-op.
- **Empty `AppendEntries` never deletes anything**: a follower with extra
  entries beyond the Leader's log ((c) and (d) in Figure 7 of the paper)
  keeps them until the Leader's next entry at that index conflicts with
  them. This is the paper's behavior, not a shortcut.
- **`matchIndex` never moves backwards**: a delayed or duplicated ack for an
  older batch reports a smaller `MatchIndex`; `max` keeps the newer value.
  `MatchIndex` in the response (an addition to Figure 2) lets the Leader
  update `matchIndex` without matching acks to requests. An ack claiming
  more than the Leader's own `lastIndex` is a protocol error and is reported
  as such.
- **Rollback floor**: `nextIndex` is decremented one entry per rejection,
  the MVP strategy of AGENTS.md (the conflict-term optimization of §5.3 is
  out of scope), but never below `matchIndex+1`: a stale rejection
  arriving after a success cannot make the Leader resend entries the
  follower has acknowledged.
- **Leader Append-Only is asserted, not assumed**: `truncateSuffix` panics
  if ever called on a Leader. A same-term `AppendEntries` from another
  Leader is refused before it gets there (Election Safety), so the panic
  guards against a bug, not a message.
- **Storage first, reply after**: the follower truncates and appends in
  storage before touching its in-memory log, and replies only when both
  succeeded; a storage error returns the error to the host with no reply,
  so the Leader retries later instead of counting a match that was never
  persisted. Memory reflects exactly what storage holds even when the
  second write (append after truncate) fails.

Timeline events: `Propose`, `AppendEntriesSend(peer, prevIndex, prevTerm, n)`,
`AppendEntriesReject(prevIndex, prevTerm, lastIndex, lastTerm)`,
`TruncateSuffix(index, lastIndex)`, `AppendEntriesAck(peer, success, matchIndex)`,
`MatchIndexAdvance(peer, matchIndex)`. Not yet: `commitIndex`/`LeaderCommit`
handling and apply (issue #8), so `Applied` never fires and `Propose`
returns as soon as the entry is in the Leader's own log.

In the simulator, `Cluster.Propose(cmd)` routes to `Leader()` (an error
when there is none), `SimNode.Propose(cmd)` targets one node — both return
the client's answer rather than recording it in `Errors()` — and
`Cluster.LogsConverged()` is the usual `RunUntil` predicate: every node holds
exactly the Leader's persisted log. It counts every node regardless of the
network, so it never reports an isolated Leader as converged; a partition
test that only cares about one side compares those logs itself.
`SimNode.Log()` reads the log back from storage. A node whose
`MemoryStorage` was closed by a test answers every write with
`storage.ErrClosed`; the invariant checker skips its log until it is
readable again, and `LogsConverged()` is false while it is closed.

### Persistence

`storage.FileStorage` (see [ADR 0003](docs/decisions/0003-file-storage-durability.md))
keeps a node's persistent state in one directory of human-readable JSON:

```
<dir>/state.json   {"currentTerm":3,"votedFor":"b"}      replaced atomically on every SaveTermVote
<dir>/log.jsonl    {"index":1,"term":1,"command":"AQ=="}  append-only NDJSON, fsync after every append;
                   {"index":2,"term":3,"command":"Ag=="}  rewritten atomically by TruncateSuffix
```

Guaranteed: a write is durable when `SaveTermVote`/`AppendEntries` returns;
`state.json` is never half-written; a partial trailing line left by a crash
mid-append is discarded on `Open` (that append was never acknowledged). Not
guaranteed: per-entry checksums, torn-write protection finer than one line,
log rotation or compaction, a lock against two processes on one directory.
Corruption before the last line is an error from `Open`, never repaired.
`MemoryStorage` and `FileStorage` pass the same conformance suite
(`go test ./internal/storage/ -run TestStorageConformance -v`).

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
- `Cluster` — hosts N `*raft.Node`s over `MemoryStorage` (or stores
  pre-loaded through `Config.Stores`), arms their first election timers via
  `Start()`, translates the `Action`s they return into timers and sends, and
  drives everything with `Run(until)`, `Step()` or `RunUntil(pred, maxTime)`.
  `Leader()` returns the unique Leader of the highest term, `Roles()` every
  node's role, and `SimNode.ForceElectionTimeout()` makes a node start an
  election right now (how the split-vote test creates two simultaneous
  candidates by construction rather than by seed hunting).
- **Invariant checker** — after every core input the cluster observes every
  node's `Status()` and storage and records violations of Election Safety
  (at most one Leader per term over the whole run), Term Monotonicity (a
  node's term never decreases), Vote Safety (a node never votes for two
  different nodes within one term — every non-empty vote is compared
  against the first one observed for that node and term, so a vote that is
  cleared and re-granted to someone else is still caught), Log Matching
  (for every pair of nodes, an entry with the same index and term implies
  identical logs up to it, commands included), Leader Append-Only (while a
  node leads a term, the log it had last time must be a prefix of the one
  it has now) and Storage Consistency (`Status().LastLogIndex/LastLogTerm`
  match the last entry actually persisted).
  `AssertInvariants()` returns them; the test helper calls it at the end of
  every simulator test.

Every event is logged on a timeline stamped with the fake clock (excerpt of
a three-node election under seed 1):

```
t=0 event=cluster seed=1 nodes=3
t=0 event=ResetElectionTimer node=c timeout=152.915349ms
t=152 event=ElectionTimeout node=c term=0 role=Follower
t=152 event=BecameCandidate node=c term=1 role=Candidate
t=152 event=send from=c to=b type=RequestVote term=1 latency=15.942972ms
t=168 event=deliver from=c to=b type=RequestVote term=1
t=168 event=TermAdvanced node=b term=1 role=Follower
t=168 event=VoteGranted node=b term=1 role=Follower peer=c
t=178 event=BecameLeader node=c term=1 role=Leader
t=178 event=StopElectionTimer node=c
t=178 event=ResetHeartbeatTimer node=c interval=50ms
```

Every test that builds a `Cluster` or a `Network` logs `seed=<n>` (the
clock, event queue and timeline tests draw no random numbers and log none).
To replay a failure, construct the cluster with the same `Config.Seed` (or
set it in the test) and run it again:

```sh
go test ./internal/simulator/ -run TestDeterministicReplay -v
```

Messages already reorder under a random latency range: a later message with
a shorter latency arrives first, and `TestDeterministicReplay` relies on
that. Out of scope until later issues: node crash/restart and explicit fault
policies (random drop, duplication and reorder rates).

Work is tracked in the [EPIC issue](https://github.com/iceboundrock/miniraft/issues/1);
one branch and one pull request per child issue.
