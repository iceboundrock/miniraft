// Package storage provides implementations of raft.Storage, the durable-state
// abstraction of the Raft core.
//
// The interface itself is declared in package raft (the consumer) so that raft
// stays a leaf package; this package imports raft for the types and offers the
// alias Storage for readability at call sites.
//
// Two implementations exist: MemoryStorage for the simulator and unit tests,
// and FileStorage for a real process. Both are checked against the same
// contract by the conformance suite in the storagetest subpackage.
//
// # FileStorage durability model
//
// FileStorage keeps one directory of human-readable JSON: state.json holds
// currentTerm and votedFor and is replaced atomically (tmp file, fsync,
// rename, directory fsync) on every SaveTermVote; log.jsonl is append-only
// NDJSON, one entry per line, appended and fsynced by AppendEntries and
// rewritten atomically by TruncateSuffix.
//
// Guaranteed: when SaveTermVote or AppendEntries returns nil the write
// survives a crash; state.json is never seen half-written; a crash mid-append
// leaves a partial trailing line that Open discards as "the append did not
// happen" (it was never acknowledged), trimming the file so the next append
// is well-formed. A failed AppendEntries rolls the file back to its previous
// length and fsyncs the cut, so the core's retry appends the batch once. When
// FileStorage cannot restore a known state (the rollback fails, or an atomic
// replace fails after its rename) it fails closed: every later call returns
// an error wrapping ErrFailed and the process must reopen the directory, at
// which point the load rules reconcile whatever the failed write left.
//
// Not guaranteed: no per-entry checksum, no torn-write protection finer than
// a line, no segment rotation or compaction, no lock against two processes
// sharing a directory, and rename/fsync atomicity is whatever the filesystem
// provides. Corruption anywhere before the last line is an error from Open and
// is never repaired silently. See docs/decisions/0003-file-storage-durability.md.
package storage

import "github.com/iceboundrock/miniraft/internal/raft"

// Storage is an alias for raft.Storage.
type Storage = raft.Storage
