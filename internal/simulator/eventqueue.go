package simulator

import (
	"container/heap"
	"time"
)

// Event is one unit of scheduled work: at logical time At, run Fn.
//
// Seq is the creation sequence number and breaks ties between events with the
// same At, so two events scheduled for the same instant run in the order they
// were pushed (FIFO). A cancelled event stays in the heap with Fn == nil until
// it reaches the front and is discarded (lazy deletion), which keeps Cancel
// O(1) and the heap code simple.
type Event struct {
	At  time.Duration
	Seq uint64
	Fn  func()

	owner  *EventQueue // the queue that pushed it; Cancel checks it
	popped bool        // set by Pop; a popped event can no longer be cancelled
}

// EventQueue is a min-heap of events ordered by (At, Seq). It is the single
// source of ordering in the simulator: timers and message deliveries are all
// events in one queue, which is what makes a whole run deterministic.
//
// The zero value is ready to use.
type EventQueue struct {
	events  eventHeap
	nextSeq uint64
	live    int // events pushed and neither popped nor cancelled
}

// Push schedules fn at logical time at and returns the event so that the
// caller can Cancel it later.
func (q *EventQueue) Push(at time.Duration, fn func()) *Event {
	if fn == nil {
		panic("simulator: EventQueue.Push with nil Fn")
	}
	q.nextSeq++
	ev := &Event{At: at, Seq: q.nextSeq, Fn: fn, owner: q}
	heap.Push(&q.events, ev)
	q.live++
	return ev
}

// Peek returns the earliest live event without removing it, or nil if there
// is none. Cancelled events at the front are discarded.
func (q *EventQueue) Peek() *Event {
	q.dropCancelled()
	if len(q.events) == 0 {
		return nil
	}
	return q.events[0]
}

// Pop removes and returns the earliest live event, or nil if there is none.
func (q *EventQueue) Pop() *Event {
	q.dropCancelled()
	if len(q.events) == 0 {
		return nil
	}
	ev := heap.Pop(&q.events).(*Event)
	ev.popped = true
	q.live--
	return ev
}

// Cancel marks ev so that Pop never returns it. It reports whether ev was
// still pending (not yet popped or cancelled). ev must have been pushed on
// q: cancelling an event through a different queue would corrupt both
// queues' live counts, so it panics instead.
func (q *EventQueue) Cancel(ev *Event) bool {
	if ev == nil {
		return false
	}
	if ev.owner != q {
		panic("simulator: EventQueue.Cancel with an event from another queue")
	}
	if ev.popped || ev.Fn == nil {
		return false
	}
	ev.Fn = nil
	q.live--
	return true
}

// Len returns the number of live (pending, not cancelled) events.
func (q *EventQueue) Len() int { return q.live }

func (q *EventQueue) dropCancelled() {
	for len(q.events) > 0 && q.events[0].Fn == nil {
		heap.Pop(&q.events)
	}
}

// eventHeap implements heap.Interface ordered by (At, Seq).
type eventHeap []*Event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].At != h[j].At {
		return h[i].At < h[j].At
	}
	return h[i].Seq < h[j].Seq
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) { *h = append(*h, x.(*Event)) }

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return ev
}
