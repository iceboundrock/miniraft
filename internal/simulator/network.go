package simulator

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// Handler receives messages from the Network. SimNode implements it by
// feeding the message to its Raft core; tests implement it with stubs.
type Handler interface {
	HandleMessage(msg raft.Message)
}

// NetworkConfig sets the base latency of every link. MinLatency == MaxLatency
// gives a fixed latency and therefore FIFO delivery per link; a wider range
// draws each message's latency from the cluster's seeded random source, which
// is also the only way messages get reordered in this issue. Latencies are
// added to the clock, so a range reaching toward math.MaxInt64 is accepted here
// but makes Clock.After panic once Now()+latency would overflow logical time.
type NetworkConfig struct {
	MinLatency time.Duration
	MaxLatency time.Duration
}

// link is a directed edge between two nodes.
type link struct {
	from, to raft.NodeID
}

// Network routes raft.Message values between registered handlers by
// scheduling delivery events on the Clock. Send never blocks and never
// delivers synchronously; delivery happens when the clock reaches the
// scheduled instant.
//
// Link policy (disconnects, partition) is evaluated twice: when a message is
// sent and again when it would be delivered. A message in flight when its
// link goes down is therefore lost, which matches what a real partition does
// to packets on the wire and keeps "partition, then advance" tests simple.
//
// Every policy change and every send/deliver/drop is written to the timeline.
type Network struct {
	clock  *Clock
	rng    *rand.Rand
	logger *slog.Logger
	cfg    NetworkConfig

	handlers     map[raft.NodeID]Handler
	disconnected map[link]bool
	partition    map[raft.NodeID]int // node -> group; nil when not partitioned
	drops        map[link]int        // pending one-shot drops per link
	duplicate    map[link]bool       // Duplicate hook, off by default
}

// NewNetwork creates an empty network. rng is the cluster's single random
// source; it is used only for latency when MaxLatency > MinLatency.
func NewNetwork(clock *Clock, rng *rand.Rand, logger *slog.Logger, cfg NetworkConfig) *Network {
	if cfg.MinLatency < 0 || cfg.MaxLatency < cfg.MinLatency {
		panic(fmt.Sprintf("simulator: invalid latency range [%v, %v]", cfg.MinLatency, cfg.MaxLatency))
	}
	if clock == nil || rng == nil || logger == nil {
		panic("simulator: NewNetwork requires a clock, a random source and a logger")
	}
	return &Network{
		clock:        clock,
		rng:          rng,
		logger:       logger,
		cfg:          cfg,
		handlers:     make(map[raft.NodeID]Handler),
		disconnected: make(map[link]bool),
		drops:        make(map[link]int),
		duplicate:    make(map[link]bool),
	}
}

// Register makes h the recipient of messages addressed to id, replacing any
// previous handler for id.
func (n *Network) Register(id raft.NodeID, h Handler) {
	if h == nil {
		panic("simulator: Register with nil Handler")
	}
	n.handlers[id] = h
}

// Send schedules msg for delivery to msg.To according to the current link
// policy and latency. It only enqueues; nothing runs until the clock moves.
func (n *Network) Send(msg raft.Message) {
	l := link{msg.From, msg.To}
	log := n.logger.With("from", msg.From, "to", msg.To, "type", msg.Type, "term", msg.Term())
	if n.drops[l] > 0 {
		n.drops[l]--
		if n.drops[l] == 0 {
			delete(n.drops, l)
		}
		log.Info("drop", "reason", "requested")
		return
	}
	if !n.Connected(msg.From, msg.To) {
		log.Info("drop", "reason", "unreachable")
		return
	}
	n.schedule(msg, log)
	if n.duplicate[l] {
		n.schedule(msg, log.With("duplicate", true))
	}
}

// schedule enqueues one delivery of msg. Each delivery gets its own deep
// copy taken now, so the sender reusing its payload after Send, or the
// recipient of one duplicate mutating it, cannot change what a later
// delivery carries — the same isolation serialization gives a real transport.
func (n *Network) schedule(msg raft.Message, log *slog.Logger) {
	latency := n.latency()
	log.Info("send", "latency", latency)
	m := msg.Clone()
	n.clock.After(latency, func() { n.deliver(m, log) })
}

func (n *Network) deliver(msg raft.Message, log *slog.Logger) {
	if !n.Connected(msg.From, msg.To) {
		log.Info("drop", "reason", "unreachable-in-flight")
		return
	}
	h, ok := n.handlers[msg.To]
	if !ok {
		log.Info("drop", "reason", "no-handler")
		return
	}
	log.Info("deliver")
	h.HandleMessage(msg)
}

// latency draws one message latency uniformly from [MinLatency, MaxLatency].
func (n *Network) latency() time.Duration {
	span := int64(n.cfg.MaxLatency - n.cfg.MinLatency)
	switch {
	case span == 0:
		return n.cfg.MinLatency
	case span == math.MaxInt64:
		// span+1 would overflow; Int63 already covers [0, MaxInt64].
		return n.cfg.MinLatency + time.Duration(n.rng.Int63())
	default:
		return n.cfg.MinLatency + time.Duration(n.rng.Int63n(span+1))
	}
}

// Connected reports whether a message from -> to would currently be
// delivered: the directed link is not disconnected and, if the network is
// partitioned, both nodes are in the same group.
func (n *Network) Connected(from, to raft.NodeID) bool {
	if n.disconnected[link{from, to}] {
		return false
	}
	if n.partition != nil {
		gf, okf := n.partition[from]
		gt, okt := n.partition[to]
		if !okf || !okt || gf != gt {
			return false
		}
	}
	return true
}

// Disconnect blocks messages from -> to (the reverse direction is unaffected).
func (n *Network) Disconnect(from, to raft.NodeID) {
	n.disconnected[link{from, to}] = true
	n.logger.Info("disconnect", "from", from, "to", to)
}

// Reconnect undoes Disconnect(from, to).
func (n *Network) Reconnect(from, to raft.NodeID) {
	delete(n.disconnected, link{from, to})
	n.logger.Info("reconnect", "from", from, "to", to)
}

// Isolate disconnects id from every registered node in both directions.
// Undo with Reconnect per link or Heal.
func (n *Network) Isolate(id raft.NodeID) {
	for other := range n.handlers { // order irrelevant: only map writes
		if other == id {
			continue
		}
		n.disconnected[link{id, other}] = true
		n.disconnected[link{other, id}] = true
	}
	n.logger.Info("isolate", "node", id)
}

// Partition splits the network: nodes in different groups cannot exchange
// messages in either direction. A node that appears in no group can talk to
// nobody; a node listed in two groups is a mistake and panics. A new
// Partition replaces the previous one; directed disconnects stay in force
// independently.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	partition := make(map[raft.NodeID]int)
	for g, ids := range groups {
		for _, id := range ids {
			if _, dup := partition[id]; dup {
				panic(fmt.Sprintf("simulator: Partition lists node %q in more than one group", id))
			}
			partition[id] = g
		}
	}
	n.partition = partition
	n.logger.Info("partition", "groups", fmt.Sprint(groups))
}

// Heal removes the partition and every directed disconnect (including those
// made by Isolate). Pending one-shot drops and the Duplicate hook are kept.
func (n *Network) Heal() {
	n.partition = nil
	n.disconnected = make(map[link]bool)
	n.logger.Info("heal")
}

// Drop discards the next message sent from -> to. Calling it k times drops
// the next k messages. It lets a test lose one specific RPC deterministically.
func (n *Network) Drop(from, to raft.NodeID) {
	n.drops[link{from, to}]++
	n.logger.Info("drop-next", "from", from, "to", to, "pending", n.drops[link{from, to}])
}

// SetDuplicate turns message duplication on the link on or off. While on,
// every Send schedules two deliveries, each with its own latency and its own
// copy of the message. Off by default; a later issue adds a seeded
// duplication rate on top of this hook.
//
// This is the "Duplicate(from,to) toggle" of issue #3. It takes an explicit
// on/off argument instead of flipping state so that a test reads as a
// statement of the link's condition, like Disconnect/Reconnect do.
func (n *Network) SetDuplicate(from, to raft.NodeID, on bool) {
	if on {
		n.duplicate[link{from, to}] = true
	} else {
		delete(n.duplicate, link{from, to})
	}
	n.logger.Info("duplicate", "from", from, "to", to, "on", on)
}
