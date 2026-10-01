package statemachine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// The operations a KV command can carry.
const (
	OpSet = "SET" // SET <key> <value>: store value under key, result is value
	OpGet = "GET" // GET <key>: result is the current value, empty if unset
)

// Command is a KV command as it is stored in raft.LogEntry.Command, encoded
// as JSON: {"op":"SET","key":"x","value":"1"} or {"op":"GET","key":"x"}.
// Build commands with Set and Get, and check untrusted ones with Decode
// before proposing them (see KV.Apply for why).
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Set encodes SET key value.
func Set(key, value string) []byte { return Command{Op: OpSet, Key: key, Value: value}.encode() }

// Get encodes GET key. Applied through the log it returns the value as of
// that log position; KV.Get is the local, non-linearizable read instead.
func Get(key string) []byte { return Command{Op: OpGet, Key: key}.encode() }

func (c Command) encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // unreachable: a struct of strings always marshals
	}
	return b
}

// Decode parses and validates an encoded command: it must be JSON with an
// op of exactly SET or GET and a non-empty key.
func Decode(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("statemachine: malformed command %q: %w", b, err)
	}
	if c.Op != OpSet && c.Op != OpGet {
		return Command{}, fmt.Errorf("statemachine: unknown op %q in command %q", c.Op, b)
	}
	if c.Key == "" {
		return Command{}, errors.New("statemachine: command has an empty key")
	}
	return c, nil
}

// KV is the MVP's replicated state machine: a map from string keys to
// string values, changed only by applying committed SET entries in log
// order. Like raft.Node it is not safe for concurrent use; the goroutine
// that drives the node owns it. The zero value is an empty KV.
type KV struct {
	data map[string]string
}

// NewKV returns an empty KV.
func NewKV() *KV { return &KV{data: make(map[string]string)} }

// Apply implements raft.StateMachine. It is deterministic — the result and
// the new state depend only on the current state and the command — which is
// what makes replicas that apply the same log agree. A malformed command is
// rejected with an error before anything changes; the Raft core then stops
// applying at that entry on every node (see raft.StateMachine), so hosts
// must Decode untrusted commands before proposing them.
func (kv *KV) Apply(entry raft.LogEntry) ([]byte, error) {
	c, err := Decode(entry.Command)
	if err != nil {
		return nil, fmt.Errorf("statemachine: entry %d: %w", entry.Index, err)
	}
	switch c.Op {
	case OpSet:
		if kv.data == nil {
			kv.data = make(map[string]string)
		}
		kv.data[c.Key] = c.Value
		return []byte(c.Value), nil
	default: // OpGet
		return []byte(kv.data[c.Key]), nil
	}
}

// Get is the local read path: the value this replica has applied for key,
// and whether the key is set. It is not linearizable — a follower, or a
// deposed Leader, may be behind the cluster — which the MVP accepts.
func (kv *KV) Get(key string) (string, bool) {
	v, ok := kv.data[key]
	return v, ok
}

// Bytes returns a canonical encoding of the whole state (JSON with sorted
// keys), so two machines hold the same state exactly when their Bytes are
// equal.
func (kv *KV) Bytes() []byte {
	data := kv.data
	if data == nil {
		data = map[string]string{}
	}
	b, err := json.Marshal(data)
	if err != nil {
		panic(err) // unreachable: a map of strings always marshals
	}
	return b
}
