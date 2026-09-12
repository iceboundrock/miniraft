package statemachine_test

import (
	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/statemachine"
)

type nopSM struct{}

func (nopSM) Apply(raft.LogEntry) ([]byte, error) { return nil, nil }

// The alias must be usable as the core's StateMachine contract.
var _ statemachine.StateMachine = nopSM{}
var _ raft.StateMachine = statemachine.StateMachine(nopSM{})
