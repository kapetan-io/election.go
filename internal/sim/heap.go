package sim

import (
	"container/heap"
	"time"
)

// pendingTimer is a timer waiting to fire in the simulation.
type pendingTimer struct {
	fireAt  time.Time
	timerID int64
	nodeID  string
	index   int // position in the heap (maintained by heap.Interface)
}

// TimerHeap is a min-heap of pending timers sorted by fireAt.
// Same-time tiebreak: lower timerID fires first (monotonic assignment).
type TimerHeap struct {
	items []*pendingTimer
	byID  map[int64]*pendingTimer
}

// newTimerHeap creates an empty TimerHeap.
func newTimerHeap() *TimerHeap {
	return &TimerHeap{
		byID: make(map[int64]*pendingTimer),
	}
}

// Len implements heap.Interface.
func (h *TimerHeap) Len() int { return len(h.items) }

// Less implements heap.Interface — min by fireAt, tiebreak by timerID.
func (h *TimerHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if a.fireAt.Equal(b.fireAt) {
		return a.timerID < b.timerID
	}
	return a.fireAt.Before(b.fireAt)
}

// Swap implements heap.Interface.
func (h *TimerHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

// Push implements heap.Interface (called by container/heap, not directly).
func (h *TimerHeap) Push(x any) {
	t := x.(*pendingTimer)
	t.index = len(h.items)
	h.items = append(h.items, t)
}

// Pop implements heap.Interface (called by container/heap, not directly).
func (h *TimerHeap) Pop() any {
	old := h.items
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	t.index = -1
	return t
}

// Add pushes a timer onto the heap and registers it by ID.
func (h *TimerHeap) Add(t *pendingTimer) {
	h.byID[t.timerID] = t
	heap.Push(h, t)
}

// Peek returns the earliest timer without removing it, or nil if empty.
func (h *TimerHeap) Peek() *pendingTimer {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// PopTimer removes and returns the earliest timer. Returns nil if empty.
func (h *TimerHeap) PopTimer() *pendingTimer {
	if len(h.items) == 0 {
		return nil
	}
	t := heap.Pop(h).(*pendingTimer)
	delete(h.byID, t.timerID)
	return t
}

// Remove cancels a timer by ID. Returns false if not found or already fired.
func (h *TimerHeap) Remove(timerID int64) bool {
	t, ok := h.byID[timerID]
	if !ok {
		return false
	}
	delete(h.byID, timerID)
	heap.Remove(h, t.index)
	return true
}
