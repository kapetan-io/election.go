package sim

import (
	"errors"

	"github.com/thrawn01/election/internal/engine"
)

// SimIO is the per-node IO adapter for simulation. It routes through the
// shared Simulation orchestrator instead of using goroutines or real timers.
type SimIO struct {
	nodeID      string
	sim         *Simulation
	events      []engine.Event
	pendingRecv func(engine.Resp, error)
	nextTimerID int64
}

// Dispatch implements io.IO[engine.Req, engine.Resp] for simulation.
// Each operation is deterministic — no goroutines, no real time.
func (s *SimIO) Dispatch(req engine.Req, callback func(engine.Resp, error)) {
	switch req.Kind {
	case engine.SendRPC:
		// Queue the RPC for delivery by the orchestrator. The callback is
		// stored by the orchestrator and resolved after the target node processes it.
		s.sim.queueRPC(s.nodeID, req.Peer, req.RPCReq, callback)

	case engine.Recv:
		// If events are already queued (e.g., a timer fired before Recv was called),
		// dequeue the first one and deliver inline. Otherwise, park the callback.
		if len(s.events) > 0 {
			event := s.events[0]
			s.events = s.events[1:]
			callback(engine.Resp{Kind: engine.Recv, Event: event}, nil)
		} else {
			s.pendingRecv = callback
		}

	case engine.After:
		// Register the timer on the shared heap. Resolve callback immediately
		// with the assigned TimerID (the timer fires later via the orchestrator).
		s.nextTimerID++
		timerID := s.nextTimerID
		fireAt := s.sim.clock.Now().Add(req.Delay)
		s.sim.timerHeap.Add(&pendingTimer{
			fireAt:  fireAt,
			timerID: timerID,
			nodeID:  s.nodeID,
		})
		callback(engine.Resp{Kind: engine.After, TimerID: timerID}, nil)

	case engine.Cancel:
		// Remove from heap (no-op if already fired or not found).
		s.sim.timerHeap.Remove(req.TimerID)
		callback(engine.Resp{Kind: engine.Cancel}, nil)

	case engine.Now:
		// Return virtual clock time.
		callback(engine.Resp{Kind: engine.Now, Time: s.sim.clock.Now()}, nil)
	}
}

// deliverEvent queues an event for this node. If the node is currently parked
// on a Recv, deliver the event immediately by calling the pending callback.
func (s *SimIO) deliverEvent(event engine.Event) {
	if s.pendingRecv != nil {
		cb := s.pendingRecv
		s.pendingRecv = nil
		cb(engine.Resp{Kind: engine.Recv, Event: event}, nil)
	} else {
		s.events = append(s.events, event)
	}
}

// errDropped is returned to the sender when an RPC is dropped due to a
// partition, drop rate, or other fault injection.
var errDropped = errors.New("RPC dropped")
