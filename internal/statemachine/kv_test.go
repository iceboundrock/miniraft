package statemachine_test

import (
	"bytes"
	"testing"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/statemachine"
)

var _ raft.StateMachine = (*statemachine.KV)(nil)

// apply applies cmd as the entry at index and fails the test on error.
func apply(t *testing.T, kv *statemachine.KV, index raft.Index, cmd []byte) []byte {
	t.Helper()
	res, err := kv.Apply(raft.LogEntry{Index: index, Term: 1, Command: cmd})
	if err != nil {
		t.Fatalf("Apply(%s): %v", cmd, err)
	}
	return res
}

func TestKVApplySetGet(t *testing.T) {
	kv := statemachine.NewKV()
	if res := apply(t, kv, 1, statemachine.Get("x")); len(res) != 0 {
		t.Fatalf("GET of an unset key = %q, want empty", res)
	}
	if res := apply(t, kv, 2, statemachine.Set("x", "1")); string(res) != "1" {
		t.Fatalf("SET x 1 = %q, want the value", res)
	}
	if res := apply(t, kv, 3, statemachine.Get("x")); string(res) != "1" {
		t.Fatalf("GET x = %q, want 1", res)
	}
	apply(t, kv, 4, statemachine.Set("x", "2"))
	apply(t, kv, 5, statemachine.Set("y", ""))
	if v, ok := kv.Get("x"); !ok || v != "2" {
		t.Fatalf("Get(x) = (%q, %v), want (2, true): SET overwrites", v, ok)
	}
	if v, ok := kv.Get("y"); !ok || v != "" {
		t.Fatalf("Get(y) = (%q, %v), want an empty value that is set", v, ok)
	}
	if _, ok := kv.Get("z"); ok {
		t.Fatal("Get(z) reports a key that was never set")
	}
}

func TestKVRejectsMalformedCommandsWithoutChangingState(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  string
	}{
		{"not json", "SET x 1"},
		{"empty", ""},
		{"unknown op", `{"op":"DEL","key":"x"}`},
		{"lowercase op", `{"op":"set","key":"x","value":"1"}`},
		{"missing key", `{"op":"SET","value":"1"}`},
		{"empty key", `{"op":"GET","key":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := statemachine.Decode([]byte(tc.cmd)); err == nil {
				t.Fatalf("Decode(%q) accepted a malformed command", tc.cmd)
			}
			kv := statemachine.NewKV()
			apply(t, kv, 1, statemachine.Set("x", "1"))
			before := kv.Bytes()
			if res, err := kv.Apply(raft.LogEntry{Index: 2, Term: 1, Command: []byte(tc.cmd)}); err == nil {
				t.Fatalf("Apply(%q) = %q, want an error", tc.cmd, res)
			}
			if !bytes.Equal(kv.Bytes(), before) {
				t.Fatalf("a rejected command changed the state: %s -> %s", before, kv.Bytes())
			}
		})
	}
}

func TestKVCommandEncoding(t *testing.T) {
	if got, want := string(statemachine.Set("x", "1")), `{"op":"SET","key":"x","value":"1"}`; got != want {
		t.Fatalf("Set(x, 1) = %s, want %s", got, want)
	}
	if got, want := string(statemachine.Get("x")), `{"op":"GET","key":"x"}`; got != want {
		t.Fatalf("Get(x) = %s, want %s", got, want)
	}
	cmd, err := statemachine.Decode(statemachine.Set("k", "v w"))
	if err != nil || cmd != (statemachine.Command{Op: statemachine.OpSet, Key: "k", Value: "v w"}) {
		t.Fatalf("Decode(Set(k, \"v w\")) = (%+v, %v)", cmd, err)
	}
}

// TestKVBytesIsCanonical: two machines that applied the same commands in
// the same order have identical Bytes, whatever the map's internal order;
// different contents give different Bytes.
func TestKVBytesIsCanonical(t *testing.T) {
	a, b := statemachine.NewKV(), statemachine.NewKV()
	for i, k := range []string{"b", "a", "c", "e", "d"} {
		apply(t, a, raft.Index(i+1), statemachine.Set(k, k+"1"))
		apply(t, b, raft.Index(i+1), statemachine.Set(k, k+"1"))
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatalf("same history, different Bytes: %s vs %s", a.Bytes(), b.Bytes())
	}
	if got, want := string(statemachine.NewKV().Bytes()), "{}"; got != want {
		t.Fatalf("empty KV Bytes = %s, want %s", got, want)
	}
	apply(t, b, 6, statemachine.Set("a", "changed"))
	if bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("different contents, same Bytes")
	}
}
