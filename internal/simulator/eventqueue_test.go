package simulator

import (
	"testing"
	"time"
)

// TestEventQueueOrdering checks the (At, Seq) order: earlier deadlines first,
// and for equal deadlines the event that was pushed first.
func TestEventQueueOrdering(t *testing.T) {
	var q EventQueue
	var got []string
	push := func(at time.Duration, name string) *Event {
		return q.Push(at, func() { got = append(got, name) })
	}
	push(10*time.Millisecond, "first@10")
	push(5*time.Millisecond, "first@5")
	push(10*time.Millisecond, "second@10")
	push(5*time.Millisecond, "second@5")

	if q.Len() != 4 {
		t.Fatalf("Len() = %d, want 4", q.Len())
	}
	if ev := q.Peek(); ev.At != 5*time.Millisecond || ev.Seq != 2 {
		t.Fatalf("Peek() = (%v, %d), want (5ms, 2)", ev.At, ev.Seq)
	}
	for ev := q.Pop(); ev != nil; ev = q.Pop() {
		ev.Fn()
	}
	want := []string{"first@5", "second@5", "first@10", "second@10"}
	if len(got) != len(want) {
		t.Fatalf("ran %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ran %v, want %v", got, want)
		}
	}
	if q.Len() != 0 || q.Pop() != nil || q.Peek() != nil {
		t.Fatalf("queue not empty after draining: Len()=%d", q.Len())
	}
}

// TestEventQueueCancel checks that a cancelled event is never popped, that
// Len counts only live events, and that Cancel is idempotent.
func TestEventQueueCancel(t *testing.T) {
	var q EventQueue
	a := q.Push(1*time.Millisecond, func() {})
	b := q.Push(2*time.Millisecond, func() {})
	c := q.Push(3*time.Millisecond, func() {})

	if !q.Cancel(a) {
		t.Fatal("Cancel(a) = false, want true for a pending event")
	}
	if q.Cancel(a) {
		t.Fatal("second Cancel(a) = true, want false")
	}
	if q.Len() != 2 {
		t.Fatalf("Len() = %d, want 2 after cancelling one of three", q.Len())
	}
	if ev := q.Peek(); ev != b {
		t.Fatalf("Peek() = %+v, want b (cancelled a must be skipped)", ev)
	}
	if ev := q.Pop(); ev != b {
		t.Fatalf("Pop() = %+v, want b", ev)
	}
	if q.Cancel(b) {
		t.Fatal("Cancel(b) after Pop = true, want false: it already ran")
	}
	if q.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", q.Len())
	}
	if ev := q.Pop(); ev != c {
		t.Fatalf("Pop() = %+v, want c", ev)
	}
}
