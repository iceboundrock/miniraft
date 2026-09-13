// Package storagetest is a conformance suite for raft.Storage implementations.
//
// Run exercises the contract documented on raft.Storage (durable pair
// SaveTermVote, all-or-nothing validated AppendEntries, TruncateSuffix,
// deep-copy ownership, and ErrClosed-style failure after Close) against any
// implementation, so that MemoryStorage and FileStorage are held to the same
// behavior.
package storagetest

import (
	"reflect"
	"testing"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// Opener returns a fresh, empty store for one test. It is called once per
// subtest, so implementations backed by a directory should create a new
// t.TempDir() each time.
type Opener func(t *testing.T) raft.Storage

// Run runs the conformance suite against stores produced by open.
func Run(t *testing.T, open Opener) {
	t.Helper()
	t.Run("FreshIsZero", func(t *testing.T) { testFreshIsZero(t, open(t)) })
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, open(t)) })
	t.Run("LoadDeepCopies", func(t *testing.T) { testLoadDeepCopies(t, open(t)) })
	t.Run("AppendCopiesCommand", func(t *testing.T) { testAppendCopiesCommand(t, open(t)) })
	t.Run("AppendRejectsGap", func(t *testing.T) { testAppendRejectsGap(t, open(t)) })
	t.Run("AppendIsAtomic", func(t *testing.T) { testAppendIsAtomic(t, open(t)) })
	t.Run("AppendRejectsTermZero", func(t *testing.T) { testAppendRejectsTermZero(t, open(t)) })
	t.Run("TruncateZeroEmptiesLog", func(t *testing.T) { testTruncateZeroEmptiesLog(t, open(t)) })
	t.Run("TruncateBeyondEndIsNoop", func(t *testing.T) { testTruncateBeyondEndIsNoop(t, open(t)) })
	t.Run("ClosedRejectsCalls", func(t *testing.T) { testClosedRejectsCalls(t, open(t)) })
	t.Run("ConcurrentLoadDuringMutation", func(t *testing.T) { testConcurrentLoadDuringMutation(t, open(t)) })
}

// Entries builds entries from (index, term) pairs; Command is the index byte.
func Entries(pairs ...[2]uint64) []raft.LogEntry {
	out := make([]raft.LogEntry, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, raft.LogEntry{Index: raft.Index(p[0]), Term: raft.Term(p[1]), Command: []byte{byte(p[0])}})
	}
	return out
}

func mustLoad(t *testing.T, st raft.Storage) raft.PersistentState {
	t.Helper()
	s, err := st.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return s
}

func testFreshIsZero(t *testing.T, st raft.Storage) {
	defer st.Close()
	if s := mustLoad(t, st); !reflect.DeepEqual(s, raft.PersistentState{}) {
		t.Fatalf("fresh Load() = %+v, want zero", s)
	}
}

func testRoundTrip(t *testing.T, st raft.Storage) {
	defer st.Close()
	if err := st.SaveTermVote(4, "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries(Entries([2]uint64{1, 1}, [2]uint64{2, 3})); err != nil {
		t.Fatal(err)
	}
	if err := st.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries(Entries([2]uint64{2, 4}, [2]uint64{3, 4})); err != nil {
		t.Fatal(err)
	}
	want := raft.PersistentState{CurrentTerm: 4, VotedFor: "b", Entries: Entries([2]uint64{1, 1}, [2]uint64{2, 4}, [2]uint64{3, 4})}
	if s := mustLoad(t, st); !reflect.DeepEqual(s, want) {
		t.Fatalf("Load() = %+v, want %+v", s, want)
	}
	// Clearing the vote must round-trip too (a new term with no vote yet).
	if err := st.SaveTermVote(5, raft.None); err != nil {
		t.Fatal(err)
	}
	if s := mustLoad(t, st); s.CurrentTerm != 5 || s.VotedFor != raft.None {
		t.Fatalf("Load() term/vote = %d/%q, want 5/\"\"", s.CurrentTerm, s.VotedFor)
	}
}

func testLoadDeepCopies(t *testing.T, st raft.Storage) {
	defer st.Close()
	if err := st.AppendEntries(Entries([2]uint64{1, 1})); err != nil {
		t.Fatal(err)
	}
	s := mustLoad(t, st)
	s.Entries[0].Term = 99
	s.Entries[0].Command[0] = 0xff
	if s2 := mustLoad(t, st); s2.Entries[0].Term != 1 || s2.Entries[0].Command[0] != 1 {
		t.Fatal("Load must deep-copy entries")
	}
}

func testAppendCopiesCommand(t *testing.T, st raft.Storage) {
	defer st.Close()
	in := Entries([2]uint64{1, 1})
	if err := st.AppendEntries(in); err != nil {
		t.Fatal(err)
	}
	in[0].Command[0] = 0xff
	if s := mustLoad(t, st); s.Entries[0].Command[0] != 1 {
		t.Fatal("AppendEntries must not share Command bytes with the caller")
	}
}

func testAppendRejectsGap(t *testing.T, st raft.Storage) {
	defer st.Close()
	if err := st.AppendEntries(Entries([2]uint64{1, 1}, [2]uint64{2, 3})); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries(Entries([2]uint64{4, 3})); err == nil {
		t.Fatal("AppendEntries accepted an index gap")
	}
	if s := mustLoad(t, st); len(s.Entries) != 2 {
		t.Fatalf("store has %d entries after rejected append, want 2", len(s.Entries))
	}
}

func testAppendIsAtomic(t *testing.T, st raft.Storage) {
	defer st.Close()
	// Valid prefix (index 1) followed by an invalid suffix (index 3): nothing
	// may be appended.
	if err := st.AppendEntries(Entries([2]uint64{1, 1}, [2]uint64{3, 1})); err == nil {
		t.Fatal("AppendEntries accepted an index gap")
	}
	if s := mustLoad(t, st); len(s.Entries) != 0 {
		t.Fatalf("partial append: store has %d entries, want 0", len(s.Entries))
	}
}

func testAppendRejectsTermZero(t *testing.T, st raft.Storage) {
	defer st.Close()
	if err := st.AppendEntries(Entries([2]uint64{1, 1}, [2]uint64{2, 0})); err == nil {
		t.Fatal("AppendEntries accepted a real entry with term 0")
	}
	if s := mustLoad(t, st); len(s.Entries) != 0 {
		t.Fatalf("partial append: store has %d entries, want 0", len(s.Entries))
	}
}

func testTruncateZeroEmptiesLog(t *testing.T, st raft.Storage) {
	defer st.Close()
	if err := st.AppendEntries(Entries([2]uint64{1, 1}, [2]uint64{2, 1})); err != nil {
		t.Fatal(err)
	}
	if err := st.TruncateSuffix(0); err != nil {
		t.Fatal(err)
	}
	if s := mustLoad(t, st); len(s.Entries) != 0 {
		t.Fatal("TruncateSuffix(0) must empty the log")
	}
	// The log must be usable again from index 1.
	if err := st.AppendEntries(Entries([2]uint64{1, 2})); err != nil {
		t.Fatal(err)
	}
	if s := mustLoad(t, st); !reflect.DeepEqual(s.Entries, Entries([2]uint64{1, 2})) {
		t.Fatalf("Entries = %+v, want [1/2]", s.Entries)
	}
}

func testTruncateBeyondEndIsNoop(t *testing.T, st raft.Storage) {
	defer st.Close()
	if err := st.AppendEntries(Entries([2]uint64{1, 1})); err != nil {
		t.Fatal(err)
	}
	if err := st.TruncateSuffix(5); err != nil {
		t.Fatal(err)
	}
	if s := mustLoad(t, st); len(s.Entries) != 1 {
		t.Fatalf("TruncateSuffix past the end changed the log: %+v", s.Entries)
	}
}

func testClosedRejectsCalls(t *testing.T, st raft.Storage) {
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Load(); err == nil {
		t.Fatal("Load after Close succeeded")
	}
	if err := st.SaveTermVote(1, "a"); err == nil {
		t.Fatal("SaveTermVote after Close succeeded")
	}
	if err := st.AppendEntries(Entries([2]uint64{1, 1})); err == nil {
		t.Fatal("AppendEntries after Close succeeded")
	}
	if err := st.TruncateSuffix(1); err == nil {
		t.Fatal("TruncateSuffix after Close succeeded")
	}
	// Close must be idempotent (a host may close on every shutdown path).
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func testConcurrentLoadDuringMutation(t *testing.T, st raft.Storage) {
	defer st.Close()
	// Storage is called by a single owner in the core, but hosts and tests
	// may Load from another goroutine; the store must serialize that.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			if _, err := st.Load(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := uint64(1); i <= 20; i++ {
		if err := st.AppendEntries(Entries([2]uint64{i, 1})); err != nil {
			t.Fatal(err)
		}
		if err := st.SaveTermVote(raft.Term(i), "a"); err != nil {
			t.Fatal(err)
		}
	}
	<-done
	if s := mustLoad(t, st); len(s.Entries) != 20 || s.CurrentTerm != 20 {
		t.Fatalf("final state: %d entries, term %d", len(s.Entries), s.CurrentTerm)
	}
}
