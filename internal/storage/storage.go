// Package storage provides implementations of raft.Storage, the durable-state
// abstraction of the Raft core.
//
// The interface itself is declared in package raft (the consumer) so that raft
// stays a leaf package; this package imports raft for the types and offers the
// alias Storage for readability at call sites.
package storage

import "github.com/iceboundrock/miniraft/internal/raft"

// Storage is an alias for raft.Storage.
type Storage = raft.Storage
