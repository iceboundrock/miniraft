package simulator

import (
	"math"
	"slices"
	"testing"
	"time"
)

// TestClockTimersFireInOrder: Advance across several deadlines fires every
// timer in deadline order, ties in creation order, with Now() equal to the
// deadline while each callback runs.
func TestClockTimersFireInOrder(t *testing.T) {
	c := NewClock()
	var fired []string
	var at []time.Duration
	after := func(d time.Duration, name string) {
		c.After(d, func() {
			fired = append(fired, name)
			at = append(at, c.Now())
		})
	}
	after(30*time.Millisecond, "c")
	after(10*time.Millisecond, "a1")
	after(20*time.Millisecond, "b")
	after(10*time.Millisecond, "a2")

	if c.Pending() != 4 {
		t.Fatalf("Pending() = %d, want 4", c.Pending())
	}
	if d, ok := c.NextDeadline(); !ok || d != 10*time.Millisecond {
		t.Fatalf("NextDeadline() = (%v, %v), want (10ms, true)", d, ok)
	}

	c.Advance(25 * time.Millisecond)
	if want := []string{"a1", "a2", "b"}; !slices.Equal(fired, want) {
		t.Fatalf("after Advance(25ms) fired %v, want %v", fired, want)
	}
	if want := []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}; !slices.Equal(at, want) {
		t.Fatalf("callbacks observed Now() = %v, want %v", at, want)
	}
	if c.Now() != 25*time.Millisecond {
		t.Fatalf("Now() = %v, want 25ms", c.Now())
	}

	c.Advance(5 * time.Millisecond)
	if want := []string{"a1", "a2", "b", "c"}; !slices.Equal(fired, want) {
		t.Fatalf("after Advance(30ms) fired %v, want %v", fired, want)
	}
	if c.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", c.Pending())
	}
	if _, ok := c.NextDeadline(); ok {
		t.Fatal("NextDeadline() ok = true with no timers")
	}
}

// TestClockStopTimer: a stopped timer never fires; Stop reports whether the
// timer was pending.
func TestClockStopTimer(t *testing.T) {
	c := NewClock()
	fired := 0
	id := c.After(10*time.Millisecond, func() { fired++ })
	keep := c.After(10*time.Millisecond, func() { fired++ })

	if !c.Stop(id) {
		t.Fatal("Stop(pending) = false, want true")
	}
	if c.Stop(id) {
		t.Fatal("Stop(already stopped) = true, want false")
	}
	if c.Pending() != 1 {
		t.Fatalf("Pending() = %d, want 1", c.Pending())
	}
	c.Advance(time.Second)
	if fired != 1 {
		t.Fatalf("fired = %d, want 1 (only the kept timer)", fired)
	}
	if c.Stop(keep) {
		t.Fatal("Stop(fired) = true, want false")
	}
	if c.Stop(0) {
		t.Fatal("Stop(0) = true, want false: zero is never a timer")
	}
}

// TestClockCallbacksMaySchedule: a callback may arm a new timer; if its
// deadline is within the current Advance window it fires in the same call.
func TestClockCallbacksMaySchedule(t *testing.T) {
	c := NewClock()
	var fired []time.Duration
	c.After(10*time.Millisecond, func() {
		fired = append(fired, c.Now())
		c.After(5*time.Millisecond, func() { fired = append(fired, c.Now()) })
		c.After(100*time.Millisecond, func() { fired = append(fired, c.Now()) })
	})
	c.Advance(20 * time.Millisecond)
	if want := []time.Duration{10 * time.Millisecond, 15 * time.Millisecond}; !slices.Equal(fired, want) {
		t.Fatalf("fired at %v, want %v", fired, want)
	}
	if c.Pending() != 1 {
		t.Fatalf("Pending() = %d, want 1 (the 110ms timer)", c.Pending())
	}
}

// TestClockStep runs exactly one event per call and advances Now() to it.
func TestClockStep(t *testing.T) {
	c := NewClock()
	var fired []string
	c.After(7*time.Millisecond, func() { fired = append(fired, "x") })
	c.After(3*time.Millisecond, func() { fired = append(fired, "y") })

	if !c.Step() {
		t.Fatal("Step() = false with pending events")
	}
	if c.Now() != 3*time.Millisecond || !slices.Equal(fired, []string{"y"}) {
		t.Fatalf("after one Step: Now()=%v fired=%v", c.Now(), fired)
	}
	if !c.Step() {
		t.Fatal("Step() = false with one pending event")
	}
	if c.Now() != 7*time.Millisecond || !slices.Equal(fired, []string{"y", "x"}) {
		t.Fatalf("after two Steps: Now()=%v fired=%v", c.Now(), fired)
	}
	if c.Step() {
		t.Fatal("Step() = true with no events")
	}
	if c.Now() != 7*time.Millisecond {
		t.Fatalf("Step() on empty queue moved Now() to %v", c.Now())
	}
}

// TestClockZeroDelayTimerFiresOnNextAdvance: After(0) fires at the current
// instant, but only once the clock is driven (Advance(0) or Step).
func TestClockZeroDelayTimerFiresOnNextAdvance(t *testing.T) {
	c := NewClock()
	fired := false
	c.After(0, func() { fired = true })
	if fired {
		t.Fatal("After(0) fired synchronously")
	}
	c.Advance(0)
	if !fired {
		t.Fatal("After(0) did not fire on Advance(0)")
	}
}

func TestClockRejectsNegativeDurations(t *testing.T) {
	c := NewClock()
	assertPanics(t, "After(-1)", func() { c.After(-time.Millisecond, func() {}) })
	assertPanics(t, "Advance(-1)", func() { c.Advance(-time.Millisecond) })
}

// TestClockRejectsOverflow: a deadline past the end of logical time would
// wrap negative and let fire() move the clock backwards, so After and Advance
// refuse it and leave the clock untouched. The exact end of time is allowed.
func TestClockRejectsOverflow(t *testing.T) {
	c := NewClock()
	c.Advance(time.Millisecond)
	assertPanics(t, "After(MaxInt64)", func() { c.After(time.Duration(math.MaxInt64), func() {}) })
	assertPanics(t, "Advance(MaxInt64)", func() { c.Advance(time.Duration(math.MaxInt64)) })
	if c.Now() != time.Millisecond || c.Pending() != 0 {
		t.Fatalf("Now() = %v, Pending() = %d after rejected calls; want 1ms, 0", c.Now(), c.Pending())
	}

	end := time.Duration(math.MaxInt64)
	fired := false
	c.After(end-c.Now(), func() { fired = true })
	if d, _ := c.NextDeadline(); d != end {
		t.Fatalf("NextDeadline() = %v, want %v", d, end)
	}
	c.Advance(end - c.Now())
	if !fired || c.Now() != end {
		t.Fatalf("fired = %v, Now() = %v; want true, %v", fired, c.Now(), end)
	}
}

// TestClockRejectsReentrantDriving: a callback may arm and stop timers, but
// it must not drive the clock. Before the guard, an inner Advance moved Now()
// to 3ms and the outer Advance then reset it to its own 1ms target, so time
// went backwards. The clock now panics at the inner call; the panic unwinds
// through the outer call, leaving Now() at the callback's deadline and the
// clock usable afterwards.
func TestClockRejectsReentrantDriving(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inner func(c *Clock)
	}{
		{"Advance", func(c *Clock) { c.Advance(2 * time.Millisecond) }},
		{"Step", func(c *Clock) { c.Step() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClock()
			c.After(time.Millisecond, func() { tc.inner(c) })
			c.After(2*time.Millisecond, func() {}) // gives the inner Step something to fire
			assertPanics(t, "nested "+tc.name, func() { c.Advance(time.Millisecond) })
			if c.Now() != time.Millisecond {
				t.Fatalf("Now() = %v after rejected nested %s, want 1ms", c.Now(), tc.name)
			}
			c.Advance(time.Millisecond)
			if c.Now() != 2*time.Millisecond || c.Pending() != 0 {
				t.Fatalf("Now() = %v, Pending() = %d after recovery; want 2ms, 0", c.Now(), c.Pending())
			}
		})
	}
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", name)
		}
	}()
	fn()
}

func TestClockDeadline(t *testing.T) {
	c := NewClock()
	c.Advance(5 * time.Millisecond)
	id := c.After(10*time.Millisecond, func() {})
	if at, ok := c.Deadline(id); !ok || at != 15*time.Millisecond {
		t.Fatalf("Deadline = %v,%v; want 15ms,true", at, ok)
	}
	if _, ok := c.Deadline(0); ok {
		t.Fatal("Deadline(0) must report no timer")
	}
	c.Stop(id)
	if _, ok := c.Deadline(id); ok {
		t.Fatal("Deadline of a stopped timer must report no timer")
	}
	id = c.After(time.Millisecond, func() {})
	c.Step()
	if _, ok := c.Deadline(id); ok {
		t.Fatal("Deadline of a fired timer must report no timer")
	}
}
