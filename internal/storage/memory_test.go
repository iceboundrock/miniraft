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

	// Load must return a copy.
	s.Entries[0].Term = 99
	s2, _ := st.Load()
	if s2.Entries[0].Term != 1 {
		t.Fatal("Load must copy entries")
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
