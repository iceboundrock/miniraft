package storage

import (
	"reflect"
	"testing"

	"github.com/iceboundrock/miniraft/internal/raft"
)

func entries(pairs ...[2]uint64) []raft.LogEntry {
	out := make([]raft.LogEntry, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, raft.LogEntry{Index: raft.Index(p[0]), Term: raft.Term(p[1]), Command: []byte{byte(p[0])}})
	}
	return out
}

func TestMemoryStorage(t *testing.T) {
	var st Storage = NewMemoryStorage()

	s, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s, raft.PersistentState{}) {
		t.Fatalf("fresh Load() = %+v, want zero", s)
	}

	if err := st.SaveTermVote(4, "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries(entries([2]uint64{1, 1}, [2]uint64{2, 3})); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries(entries([2]uint64{4, 3})); err == nil {
		t.Fatal("AppendEntries accepted an index gap")
	}
	if err := st.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntries(entries([2]uint64{2, 4}, [2]uint64{3, 4})); err != nil {
		t.Fatal(err)
	}

	s, err = st.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := raft.PersistentState{CurrentTerm: 4, VotedFor: "b", Entries: entries([2]uint64{1, 1}, [2]uint64{2, 4}, [2]uint64{3, 4})}
	if !reflect.DeepEqual(s, want) {
		t.Fatalf("Load() = %+v, want %+v", s, want)
	}

	// Load must return a deep copy, including Command bytes.
	s.Entries[0].Term = 99
	s.Entries[0].Command[0] = 0xff
	s2, _ := st.Load()
	if s2.Entries[0].Term != 1 || s2.Entries[0].Command[0] != 1 {
		t.Fatal("Load must deep-copy entries")
	}

	if err := st.TruncateSuffix(0); err != nil {
		t.Fatal(err)
	}
	if s, _ := st.Load(); len(s.Entries) != 0 {
		t.Fatal("TruncateSuffix(0) must empty the log")
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Load(); err != ErrClosed {
		t.Fatalf("Load after Close = %v, want ErrClosed", err)
	}
}

func TestMemoryStorageAppendCopiesCommand(t *testing.T) {
	st := NewMemoryStorage()
	in := entries([2]uint64{1, 1})
	if err := st.AppendEntries(in); err != nil {
		t.Fatal(err)
	}
	in[0].Command[0] = 0xff
	s, _ := st.Load()
	if s.Entries[0].Command[0] != 1 {
		t.Fatal("AppendEntries must not share Command bytes with the caller")
	}
}

func TestMemoryStorageAppendIsAtomic(t *testing.T) {
	st := NewMemoryStorage()
	// Valid prefix (index 1) followed by an invalid suffix (index 3): nothing
	// may be appended.
	if err := st.AppendEntries(entries([2]uint64{1, 1}, [2]uint64{3, 1})); err == nil {
		t.Fatal("AppendEntries accepted an index gap")
	}
	s, _ := st.Load()
	if len(s.Entries) != 0 {
		t.Fatalf("partial append: store has %d entries, want 0", len(s.Entries))
	}
}

func TestMemoryStorageRejectsTermZeroEntry(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.AppendEntries(entries([2]uint64{1, 1}, [2]uint64{2, 0})); err == nil {
		t.Fatal("AppendEntries accepted a real entry with term 0")
	}
	// The rejection must be all-or-nothing like the index check.
	if s, _ := st.Load(); len(s.Entries) != 0 {
		t.Fatalf("partial append: store has %d entries, want 0", len(s.Entries))
	}
}
