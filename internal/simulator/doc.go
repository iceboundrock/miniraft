// Package simulator is a deterministic, single-threaded simulation of a Raft
// cluster: a fake clock with ordered timers, an event queue, a network that
// delivers raft.Message values with configurable latency, disconnects and
// partitions, and a Cluster harness that hosts nodes and translates their
// raft.Actions.
//
// Everything runs on the goroutine that calls Cluster.Run, Step or RunUntil;
// there are no goroutines, mutexes or wall-clock reads, so the same seed
// always reproduces the same timeline. Every node created by NewCluster
// applies committed entries to its own statemachine.KV, and Cluster.Submit
// is the client path: propose to the Leader, run until that node has
// applied the entry, return the result.
//
// After every core input the cluster checks the Raft safety invariants it
// can observe from Status, Storage and the nodes' Applied actions (Election
// Safety, Term Monotonicity, Vote Safety, Log Matching, Leader Append-Only,
// Leader Completeness, State Machine Safety, monotonic commit and apply,
// and their bounds; see invariants) and records violations for
// AssertInvariants. Crash/restart and seeded drop/duplicate rates are added
// by a later issue; only the hooks exist here.
package simulator
