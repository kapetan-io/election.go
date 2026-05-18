package sim

import "time"

// VirtualClock is a deterministic clock shared across all nodes in a simulation.
// It advances only when the orchestrator explicitly calls Advance.
type VirtualClock struct {
	now time.Time
}

// NewVirtualClock creates a new virtual clock starting at the given time.
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start}
}

// Now returns the current virtual time.
func (c *VirtualClock) Now() time.Time {
	return c.now
}

// Advance moves the clock forward by the given duration.
func (c *VirtualClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}
