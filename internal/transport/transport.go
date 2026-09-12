// Package transport defines the message transport used in real (non-simulated)
// deployments. The Raft core never depends on it: the core emits
// raft.SendMessage actions and consumes raft.Message values, and a host wires
// those to a Transport. The HTTP implementation is added by a later issue.
package transport

import (
	"context"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// Transport moves messages between nodes. Delivery is best effort: Raft
// tolerates lost, delayed and duplicated messages, so implementations should
// fail fast rather than retry indefinitely.
type Transport interface {
	// Send delivers msg to msg.To.
	Send(ctx context.Context, msg raft.Message) error
	// Inbox yields messages addressed to this node.
	Inbox() <-chan raft.Message
}
