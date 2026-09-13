# ADR 0003: FileStorage durability model

## Status

Accepted.

## Context

Figure 2 requires `currentTerm`, `votedFor` and `log[]` to be on stable
storage before a node responds to an RPC. Vote Safety and Term Monotonicity
fail if a restarted node forgets its term or vote; Leader Completeness fails
if a follower acknowledges an entry it can later lose. The MVP needs a
durable `raft.Storage` that is small enough to audit and debuggable with
`cat`, not a production write-ahead log.

## Decision

`internal/storage.FileStorage` keeps everything in one directory as JSON:

| File | Content | Write path |
|---|---|---|
| `state.json` | `{"currentTerm": N, "votedFor": "id"}` | atomic replace on every `SaveTermVote` |
| `log.jsonl` | one `raft.LogEntry` per line (NDJSON) | `AppendEntries`: one `write` of the whole batch on an `O_APPEND` handle, then `fsync`; `TruncateSuffix`: rewrite from the in-memory copy by atomic replace |

Atomic replace is: write `<name>.tmp`, `fsync` it, `rename` over `<name>`,
`fsync` the directory. Each step closes one hole: the tmp file keeps the old
version intact while the new one is incomplete; the file `fsync` orders the
data before the rename (rename is not ordered against data writes); POSIX
`rename` is atomic so a reader sees the whole old file or the whole new one;
the directory `fsync` makes the rename itself durable. That last `fsync` is
the commit point: once it returns, the in-memory copy is updated and closing
file handles is best-effort cleanup that is never reported as a failure of
the operation, so `nil` from `SaveTermVote`/`TruncateSuffix` always means
"durable" and an error always means "the in-memory copy is unchanged".

`Open` loads both files into memory, so `Load()` is a copy and never reads
disk. Recovery rules, in order of what a crash can leave behind:

- Missing files: a fresh node (zero state).
- A leftover `*.tmp`: the crash happened before the rename, nothing was
  acknowledged; the file is deleted.
- `log.jsonl` whose final segment has no trailing newline: the crash happened
  between `write` and `fsync` in `AppendEntries`, so that call never returned
  and the core never acknowledged the batch. The segment is discarded, even
  if it happens to parse, and the file is trimmed so the next append starts on
  a line boundary.
- Any newline-terminated line that fails to parse, or entry indexes that are
  not contiguous from 1, or an unparsable `state.json`: an error from `Open`.
  Corruption before the last append is never repaired silently.

## What is guaranteed

- When `SaveTermVote` or `AppendEntries` returns `nil`, the data survives a
  process crash and, on a filesystem with ordered `fsync` semantics, a power
  loss.
- `state.json` is never observed half-written.
- `AppendEntries` is all-or-nothing from the core's point of view: on error
  the in-memory log is unchanged and the on-disk tail is trimmed back
  (best-effort in-process, guaranteed by the load rule on next `Open`).

## What is not guaranteed

- No per-entry checksum: a bit flip inside a line that still parses as JSON
  is not detected (only `ValidateEntries` structural checks apply).
- No torn-write protection finer than a line: a batch of N entries can
  survive as its first K complete lines. This is safe for Raft because those
  entries were never acknowledged and the leader's `prevLogIndex/prevLogTerm`
  check reconciles them, but it means "durable" is per line, not per batch.
- No segment rotation or compaction: `log.jsonl` grows without bound and
  `TruncateSuffix` rewrites the whole file (acceptable: truncation only
  happens on a log conflict).
- Rename atomicity and directory `fsync` semantics are filesystem-dependent;
  the implementation targets Linux/ext4-style behavior and is untested on
  network or FAT filesystems.
- `Open` does not `fsync` the parent of the storage directory after
  `MkdirAll`, so a directory created by `Open` moments before a power loss
  may itself be missing on restart (equivalent to a fresh node, which is
  safe only because nothing was acknowledged yet).
- Two processes opening the same directory are not detected (no lock file).

## Consequences

- Persistence ordering that Raft safety depends on is expressed in one
  function (`replaceFile`) and one append path, both commented step by step.
- `MemoryStorage` and `FileStorage` are held to the same contract by the
  conformance suite in `internal/storage/storagetest`.
- A production deployment would need checksums, segmented logs and
  snapshots; these are explicit non-goals of the MVP.
