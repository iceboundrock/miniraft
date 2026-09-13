package simulator

import (
	"bytes"
	"testing"
	"time"
)

// TestTimelineFormat pins the line format every other test asserts on:
// fake-clock milliseconds first, the message as event=, then attrs in
// call order (With attrs before per-call attrs), no level, no wall time.
func TestTimelineFormat(t *testing.T) {
	clock := NewClock()
	var buf bytes.Buffer
	logger := newTimelineLogger(clock, &buf)

	logger.Info("cluster", "seed", 42)
	clock.Advance(150 * time.Millisecond)
	logger.With("node", "a").Info("ElectionTimeout", "term", 3)
	clock.Advance(2500 * time.Microsecond) // sub-millisecond precision is truncated in the log
	logger.Debug("send", "from", "a", "to", "b", "latency", 5*time.Millisecond)

	want := "t=0 event=cluster seed=42\n" +
		"t=150 event=ElectionTimeout node=a term=3\n" +
		"t=152 event=send from=a to=b latency=5ms\n"
	if got := buf.String(); got != want {
		t.Fatalf("timeline =\n%s\nwant\n%s", got, want)
	}
}
