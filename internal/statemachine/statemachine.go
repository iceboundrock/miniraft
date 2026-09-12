// Package statemachine provides implementations of raft.StateMachine, the
// replicated state machine that committed log entries are applied to. The
// key-value machine is added by a later issue.
//
// The interface itself is declared in package raft (the consumer) so that raft
// stays a leaf package; this package imports raft for the types and offers the
// alias StateMachine for readability at call sites, mirroring storage.Storage.
package statemachine

import "github.com/iceboundrock/miniraft/internal/raft"

// StateMachine is an alias for raft.StateMachine.
type StateMachine = raft.StateMachine
