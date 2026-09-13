package simulator

import (
	"strings"
	"testing"
)

// newTestCluster builds a cluster for a test, logs its seed (so any failure
// can be replayed) and dumps the timeline when the test fails.
func newTestCluster(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	t.Logf("seed=%d", cfg.Seed)
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() {
		if err := c.AssertInvariants(); err != nil {
			t.Errorf("invariant violation: %v", err)
		}
		if t.Failed() {
			t.Logf("timeline:\n%s", strings.Join(c.Timeline(), "\n"))
		}
	})
	return c
}

// timelineHas reports whether some timeline line contains substr.
func timelineHas(c *Cluster, substr string) bool {
	for _, line := range c.Timeline() {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}
