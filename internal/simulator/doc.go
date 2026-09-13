// Package simulator is a deterministic, single-threaded simulation of a Raft
// cluster: a fake clock with ordered timers, an event queue, a network that
// delivers raft.Message values with configurable latency, disconnects and
// partitions, and a Cluster harness that hosts nodes and translates their
// raft.Actions.
//
// Everything runs on the goroutine that calls Cluster.Run, Step or RunUntil;
// there are no goroutines, mutexes or wall-clock reads, so the same seed
// always reproduces the same timeline. After every core input the cluster
// checks the Raft safety invariants it can observe from Status (Election
// Safety, Term Monotonicity, Vote Safety) and records violations for
// AssertInvariants. Crash/restart and seeded drop/duplicate rates are added
// by a later issue; only the hooks exist here.
package simulator
