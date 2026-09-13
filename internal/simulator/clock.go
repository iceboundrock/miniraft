package simulator

import (
	"math"
	"time"
)

// TimerID identifies a timer created by Clock.After. The zero value never
// identifies a timer, so hosts can use it to mean "no timer armed".
type TimerID uint64

// Clock is the simulated clock. Time is a logical time.Duration since the
// start of the simulation and only moves when the test calls Advance or Step;
// nothing in the simulator reads wall-clock time.
//
// Clock owns the EventQueue: every timer and every message delivery is an
// event in the same queue, so "what happens next" has exactly one answer.
// Invariant: Now() <= At of every pending event. Logical time ends at
// math.MaxInt64; After and Advance panic rather than wrap past it, because a
// wrapped deadline would sort before Now() and move the clock backwards.
//
// Only the test driver moves time. A timer callback may call Now, After,
// Stop, NextDeadline and Pending, but not Advance or Step: the clock panics
// on a nested drive, because the outer call would otherwise finish by setting
// Now() to its own, older target.
type Clock struct {
	now     time.Duration
	queue   EventQueue
	timers  map[TimerID]*Event
	driving bool // set while Advance or Step runs; rejects a nested drive
}

// NewClock returns a clock at time 0 with no timers.
func NewClock() *Clock {
	return &Clock{timers: make(map[TimerID]*Event)}
}

// Now returns the current logical time.
func (c *Clock) Now() time.Duration { return c.now }

// After arms a timer that runs fn at Now()+d. Timers fire in deadline order;
// timers with the same deadline fire in the order they were armed. The timer
// can be cancelled with Stop until it fires. It panics on a negative d or on
// a deadline beyond the end of logical time.
func (c *Clock) After(d time.Duration, fn func()) TimerID {
	if d < 0 {
		panic("simulator: Clock.After with negative duration")
	}
	ev := c.queue.Push(c.deadline(d, "After"), fn)
	id := TimerID(ev.Seq) // Seq is unique, so it doubles as the timer id.
	c.timers[id] = ev
	return id
}

// Stop cancels a timer. It reports whether the timer was pending: false for
// a timer that already fired, was already stopped, or the zero TimerID.
func (c *Clock) Stop(id TimerID) bool {
	ev, ok := c.timers[id]
	if !ok {
		return false
	}
	delete(c.timers, id)
	return c.queue.Cancel(ev)
}

// Advance moves time forward by d, firing every timer whose deadline is
// <= Now()+d in order. While a callback runs, Now() equals its deadline;
// timers armed by a callback with a deadline inside the window fire in the
// same call. Afterwards Now() == the target time even if no timer fired. It
// panics on a negative d, on a target beyond the end of logical time, or when
// called from a timer callback.
func (c *Clock) Advance(d time.Duration) {
	if d < 0 {
		panic("simulator: Clock.Advance with negative duration")
	}
	defer c.drive("Advance")()
	target := c.deadline(d, "Advance")
	for {
		ev := c.queue.Peek()
		if ev == nil || ev.At > target {
			break
		}
		c.fire()
	}
	c.now = target
}

// Step fires the next pending event, moving Now() to its deadline. It
// reports false, and leaves the clock unchanged, when nothing is pending. It
// panics when called from a timer callback.
func (c *Clock) Step() bool {
	defer c.drive("Step")()
	if c.queue.Peek() == nil {
		return false
	}
	c.fire()
	return true
}

// Deadline returns when the timer id will fire. It reports false for a timer
// that already fired, was stopped, or the zero TimerID.
func (c *Clock) Deadline(id TimerID) (time.Duration, bool) {
	ev, ok := c.timers[id]
	if !ok {
		return 0, false
	}
	return ev.At, true
}

// NextDeadline returns the deadline of the earliest pending event.
func (c *Clock) NextDeadline() (time.Duration, bool) {
	ev := c.queue.Peek()
	if ev == nil {
		return 0, false
	}
	return ev.At, true
}

// Pending returns the number of pending events: every After call that has
// neither fired nor been stopped. The Clock does not know what an event is
// for, and the Network schedules message deliveries through After too, so a
// node with one election timer and three messages in flight has Pending() ==
// 4. Ask SimNode.ElectionTimerArmed / HeartbeatTimerArmed about Raft timers;
// use Pending() == 0 to detect a quiescent simulation.
func (c *Clock) Pending() int { return c.queue.Len() }

// deadline returns Now()+d, panicking when the sum would overflow. Checked
// here, once, so the Network need not know the clock's position to keep its
// latencies representable.
func (c *Clock) deadline(d time.Duration, op string) time.Duration {
	if d > math.MaxInt64-c.now {
		panic("simulator: Clock." + op + " deadline overflows logical time")
	}
	return c.now + d
}

// drive marks the clock as being driven by op for the duration of the call
// and returns the function that clears the mark. A nested drive panics: the
// callback that issued it runs inside an outer Advance or Step whose final
// "now = target" would move time backwards past whatever the nested call did.
func (c *Clock) drive(op string) func() {
	if c.driving {
		panic("simulator: Clock." + op + " called from a timer callback")
	}
	c.driving = true
	return func() { c.driving = false }
}

// fire pops and runs the earliest event. The queue is ordered and Now() never
// exceeds a pending deadline, so setting now = ev.At never moves time back.
func (c *Clock) fire() {
	ev := c.queue.Pop()
	delete(c.timers, TimerID(ev.Seq))
	c.now = ev.At
	ev.Fn()
}
