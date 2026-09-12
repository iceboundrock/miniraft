package raft

import (
	"reflect"
	"testing"
)

func TestRaftLogEmpty(t *testing.T) {
	var l raftLog
	if l.lastIndex() != 0 || l.lastTerm() != 0 {
		t.Fatalf("empty log: lastIndex=%d lastTerm=%d, want 0/0", l.lastIndex(), l.lastTerm())
	}
	if !l.matches(0, 0) {
		t.Fatal("empty log must match prevIndex 0")
	}
	if l.matches(1, 0) {
		t.Fatal("empty log must not match prevIndex 1")
	}
	if l.matches(0, 5) {
		t.Fatal("sentinel index 0 must only match term 0")
	}
	if got := l.entriesFrom(1); got != nil {
		t.Fatalf("entriesFrom(1) = %v, want nil", got)
	}
}

func TestRaftLogHelpers(t *testing.T) {
	l := raftLog{}
	l.append(entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2}, [2]uint64{4, 3})...)

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"lastIndex", l.lastIndex(), Index(4)},
		{"lastTerm", l.lastTerm(), Term(3)},
		{"termAt(0)", l.termAt(0), Term(0)},
		{"termAt(2)", l.termAt(2), Term(1)},
		{"termAt(3)", l.termAt(3), Term(2)},
		{"termAt(beyond)", l.termAt(9), Term(0)},
		{"matches(0,0)", l.matches(0, 0), true},
		{"matches(0,99)", l.matches(0, 99), false}, // sentinel index 0 has term 0
		{"matches(3,2)", l.matches(3, 2), true},
		{"matches(3,1)", l.matches(3, 1), false},
		{"matches(5,3)", l.matches(5, 3), false},
		{"entriesFrom(3)", l.entriesFrom(3), entries([2]uint64{3, 2}, [2]uint64{4, 3})},
		{"entriesFrom(0)", len(l.entriesFrom(0)), 4},
		{"entriesFrom(5)", l.entriesFrom(5), []LogEntry(nil)},
	}
	for _, tc := range tests {
		if !reflect.DeepEqual(tc.got, tc.want) {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	// entriesFrom must return a deep copy so callers cannot mutate the log,
	// including the Command bytes.
	cp := l.entriesFrom(1)
	cp[0].Term = 99
	cp[0].Command[0] = 0xff
	if l.termAt(1) != 1 || l.entryAt(1).Command[0] != 1 {
		t.Fatal("entriesFrom must deep-copy entries")
	}
}

func TestRaftLogAppendCopiesCommand(t *testing.T) {
	var l raftLog
	in := entries([2]uint64{1, 1})
	l.append(in...)
	in[0].Command[0] = 0xff
	if l.entryAt(1).Command[0] != 1 {
		t.Fatal("append must not share Command bytes with the caller")
	}
}

func TestRaftLogTruncateSuffix(t *testing.T) {
	l := raftLog{}
	l.append(entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2})...)

	l.truncateSuffix(9) // beyond: no-op
	if l.lastIndex() != 3 {
		t.Fatalf("truncate beyond log changed lastIndex to %d", l.lastIndex())
	}
	l.truncateSuffix(2)
	if l.lastIndex() != 1 || l.lastTerm() != 1 {
		t.Fatalf("after truncateSuffix(2): lastIndex=%d lastTerm=%d", l.lastIndex(), l.lastTerm())
	}
	l.append(entries([2]uint64{2, 5})...)
	if l.termAt(2) != 5 {
		t.Fatalf("append after truncate: termAt(2)=%d, want 5", l.termAt(2))
	}
	l.truncateSuffix(0)
	if l.lastIndex() != 0 {
		t.Fatal("truncateSuffix(0) must empty the log")
	}
}

func TestRaftLogAppendGapPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("append with an index gap must panic")
		}
	}()
	var l raftLog
	l.append(LogEntry{Index: 2, Term: 1})
}
