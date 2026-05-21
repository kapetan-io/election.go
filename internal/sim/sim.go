package sim

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"time"

	gocoro "github.com/resonatehq/gocoro"

	election "github.com/kapetan-io/election.go"
	"github.com/kapetan-io/election.go/internal/engine"
)

// Config holds configuration for a simulation run.
type Config struct {
	NumNodes      int
	Seed          int64
	DropRate      float64
	MinimumQuorum int
	NodeConfig    map[string]NodeSimConfig
}

// NodeSimConfig overrides per-node election and quorum configuration.
type NodeSimConfig struct {
	ElectionTimeout     time.Duration
	HeartBeatTimeout    time.Duration
	LeaderQuorumTimeout time.Duration
	MinimumQuorum       int
	OnChange            func(election.NodeState)
}

// pendingRPC holds an RPC queued for delivery to a target node.
type pendingRPC struct {
	from     string
	to       string
	req      engine.RPCRequest
	callback func(engine.Resp, error)
	// deliverAt is the virtual time at which this RPC should be delivered.
	// Zero means "deliver as soon as possible" (no artificial delay).
	deliverAt time.Time
}

// simNode holds per-node simulation state.
type simNode struct {
	node  election.Node
	sched gocoro.Scheduler[engine.Req, engine.Resp]
	io    *SimIO
}

// Simulation is the deterministic orchestrator for a cluster of election nodes.
// One goroutine drives all schedulers — no real concurrency.
type Simulation struct {
	nodes      map[string]*simNode
	nodeOrder  []string // stable iteration order
	clock      *VirtualClock
	timerHeap  *TimerHeap
	rpcQueue   []pendingRPC
	partitions map[string]map[string]bool // partitions[a][b] == true means a→b is blocked
	dropRate   float64
	nodeDrops  map[string]float64          // per-node sender drop rate
	delays     map[string]map[string]time.Duration // delays[from][to]
	rng        *rand.Rand
}

const maxSteps = 1_000_000

// New creates a simulation with NumNodes nodes named "n0" through "n{N-1}".
// Each node is initialized with a seeded RNG and started with the simulation IO adapter.
func New(conf Config) *Simulation {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &Simulation{
		nodes:      make(map[string]*simNode, conf.NumNodes),
		nodeOrder:  make([]string, 0, conf.NumNodes),
		clock:      NewVirtualClock(start),
		timerHeap:  newTimerHeap(),
		partitions: make(map[string]map[string]bool),
		nodeDrops:  make(map[string]float64),
		delays:     make(map[string]map[string]time.Duration),
		rng:        rand.New(rand.NewSource(conf.Seed)), //nolint:gosec
		dropRate:   conf.DropRate,
	}

	// Build node IDs
	peers := make([]string, conf.NumNodes)
	for i := range peers {
		peers[i] = fmt.Sprintf("n%d", i)
	}

	// Create and start each node with simulation IO
	for i, id := range peers {
		s.nodeOrder = append(s.nodeOrder, id)

		simio := &SimIO{
			nodeID: id,
			sim:    s,
		}

		sched := gocoro.New(simio, 100)

		// Build election config for this node
		eConf := election.Config{
			UniqueID:      id,
			Peers:         peers,
			MinimumQuorum: conf.MinimumQuorum,
			// SendRPC is never called in simulation (SimIO routes directly),
			// but NewNodeForSim requires a non-nil value.
			SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
				return election.RPCResponse{}, nil
			},
		}

		// Apply per-node config overrides
		if nc, ok := conf.NodeConfig[id]; ok {
			if nc.ElectionTimeout != 0 {
				eConf.ElectionTimeout = nc.ElectionTimeout
			}
			if nc.HeartBeatTimeout != 0 {
				eConf.HeartBeatTimeout = nc.HeartBeatTimeout
			}
			if nc.LeaderQuorumTimeout != 0 {
				eConf.LeaderQuorumTimeout = nc.LeaderQuorumTimeout
			}
			if nc.MinimumQuorum != 0 {
				eConf.MinimumQuorum = nc.MinimumQuorum
			}
			if nc.OnChange != nil {
				eConf.OnChange = nc.OnChange
			}
		}

		// Seed per-node RNG deterministically
		nodeRNG := rand.New(rand.NewSource(conf.Seed + int64(i))) //nolint:gosec

		n, err := election.NewNodeForSim(eConf, nodeRNG)
		if err != nil {
			panic(fmt.Sprintf("sim.New: NewNodeForSim(%s): %v", id, err))
		}

		if err := election.StartNodeForSim(n, sched, simio); err != nil {
			panic(fmt.Sprintf("sim.New: StartNodeForSim(%s): %v", id, err))
		}

		s.nodes[id] = &simNode{
			node:  n,
			sched: sched,
			io:    simio,
		}
	}

	// Step all nodes once so they register their initial timers
	for _, id := range s.nodeOrder {
		s.nodes[id].sched.RunUntilBlocked(s.clock.Now().UnixNano())
	}

	return s
}

// RunUntilLeader drives the orchestrator loop until exactly one node reports
// IsLeader() == true, or until the step limit is exceeded (returns error).
func (s *Simulation) RunUntilLeader() error {
	for step := 0; step < maxSteps; step++ {
		if s.LeaderCount() == 1 {
			return nil
		}
		if !s.step() {
			// No events left — something is wrong
			return fmt.Errorf("no events to process after %d steps; still no leader", step)
		}
	}
	return fmt.Errorf("step limit (%d) exceeded; no leader elected", maxSteps)
}

// RunFor advances virtual time by d and processes all events within that window.
func (s *Simulation) RunFor(d time.Duration) {
	deadline := s.clock.Now().Add(d)
	for {
		next := s.nextEventTime()
		if next.IsZero() || next.After(deadline) {
			// No more events within the window
			s.clock = NewVirtualClock(deadline)
			return
		}
		s.step()
	}
}

// LeaderCount returns the number of nodes that currently report IsLeader().
func (s *Simulation) LeaderCount() int {
	count := 0
	for _, sn := range s.nodes {
		if sn.node.IsLeader() {
			count++
		}
	}
	return count
}

// Leader returns the UniqueID of the current leader, or empty string if none.
func (s *Simulation) Leader() string {
	for _, id := range s.nodeOrder {
		if s.nodes[id].node.IsLeader() {
			return id
		}
	}
	return ""
}

// Node returns the election.Node for the given node ID.
func (s *Simulation) Node(id string) election.Node {
	if sn, ok := s.nodes[id]; ok {
		return sn.node
	}
	return nil
}

// SetPeers delivers a SetPeers event directly to the named node's SimIO and
// steps the scheduler so the coroutine processes it. This bypasses the
// production event channel (which is unused in simulation).
func (s *Simulation) SetPeers(nodeID string, peers []string) {
	sn, ok := s.nodes[nodeID]
	if !ok {
		return
	}
	var errResult error
	done := make(chan error, 1)
	sn.io.deliverEvent(engine.Event{
		Kind:  engine.EventSetPeers,
		Peers: peers,
		Done: func(err error) {
			errResult = err
			done <- err
		},
	})
	sn.sched.RunUntilBlocked(s.clock.Now().UnixNano())
	select {
	case <-done:
	default:
	}
	_ = errResult
}

// Resign delivers a Resign event directly to the named node's SimIO and
// steps the scheduler so the coroutine processes it. Returns ErrNotLeader
// if the node is not the leader.
func (s *Simulation) Resign(nodeID string) error {
	sn, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node: %s", nodeID)
	}
	result := make(chan error, 1)
	sn.io.deliverEvent(engine.Event{
		Kind: engine.EventResign,
		Done: func(err error) {
			result <- err
		},
	})
	sn.sched.RunUntilBlocked(s.clock.Now().UnixNano())
	select {
	case err := <-result:
		return err
	default:
		return fmt.Errorf("resign event not processed")
	}
}

// Partition marks bidirectional isolation between all pairs of groupA × groupB.
func (s *Simulation) Partition(groupA, groupB []string) {
	for _, a := range groupA {
		for _, b := range groupB {
			s.setPartition(a, b, true)
			s.setPartition(b, a, true)
		}
	}
}

// HealAll clears all network partitions.
func (s *Simulation) HealAll() {
	s.partitions = make(map[string]map[string]bool)
}

// SetDropRate sets the global probabilistic RPC drop rate (0.0 = no drops, 1.0 = all dropped).
func (s *Simulation) SetDropRate(rate float64) {
	s.dropRate = rate
}

// SetNodeDropRate sets a per-node (sender) RPC drop rate.
func (s *Simulation) SetNodeDropRate(nodeID string, rate float64) {
	s.nodeDrops[nodeID] = rate
}

// SetDelay sets an artificial delivery delay for RPCs from→to.
func (s *Simulation) SetDelay(from, to string, d time.Duration) {
	if s.delays[from] == nil {
		s.delays[from] = make(map[string]time.Duration)
	}
	s.delays[from][to] = d
}

// -------------------------------------------------------------------------
// Internal orchestration
// -------------------------------------------------------------------------

// queueRPC is called by SimIO.Dispatch for SendRPC. It applies fault injection
// (partitions, drops, delays) and either resolves the callback immediately
// (on drop/partition) or queues the RPC for delivery.
func (s *Simulation) queueRPC(from, to string, req engine.RPCRequest, callback func(engine.Resp, error)) {
	// 1. Check bidirectional partition
	if s.isPartitioned(from, to) {
		callback(engine.Resp{Kind: engine.SendRPC, Err: errDropped}, nil)
		return
	}

	// 2. Check per-node drop rate (sender), then global drop rate
	nodeRate, hasNodeRate := s.nodeDrops[from]
	if hasNodeRate && s.rng.Float64() < nodeRate { //nolint:gosec
		callback(engine.Resp{Kind: engine.SendRPC, Err: errDropped}, nil)
		return
	}
	if !hasNodeRate && s.dropRate > 0 && s.rng.Float64() < s.dropRate { //nolint:gosec
		callback(engine.Resp{Kind: engine.SendRPC, Err: errDropped}, nil)
		return
	}

	// 3. Check artificial delay
	var deliverAt time.Time
	if fromDelays, ok := s.delays[from]; ok {
		if delay, ok := fromDelays[to]; ok && delay > 0 {
			deliverAt = s.clock.Now().Add(delay)
		}
	}

	s.rpcQueue = append(s.rpcQueue, pendingRPC{
		from:      from,
		to:        to,
		req:       req,
		callback:  callback,
		deliverAt: deliverAt,
	})
}

// hasReadyRPC returns true if there is at least one immediately-deliverable RPC.
func (s *Simulation) hasReadyRPC() bool {
	for i := range s.rpcQueue {
		if s.rpcQueue[i].deliverAt.IsZero() || !s.rpcQueue[i].deliverAt.After(s.clock.Now()) {
			return true
		}
	}
	return false
}

// step processes the next event (timer or RPC) in order. Returns false if
// there are no events to process.
func (s *Simulation) step() bool {
	// Per tech spec: RPCs before timers at same timestamp; FIFO among RPCs;
	// lower TimerID among timers at same time.
	hasRPC := s.hasReadyRPC()
	nextTimer := s.timerHeap.Peek()

	if !hasRPC && nextTimer == nil {
		// No immediately-ready RPCs or timers. Check for delayed RPCs.
		nextDelayed := s.nextDelayedRPC()
		if nextDelayed == nil {
			return false
		}
		// Advance clock to when the delayed RPC becomes ready
		s.clock.Advance(nextDelayed.deliverAt.Sub(s.clock.Now()))
		return s.step()
	}

	if hasRPC {
		// RPCs win over timers at the same timestamp.
		// But if a timer is strictly in the past, fire it first.
		if nextTimer != nil && nextTimer.fireAt.Before(s.clock.Now()) {
			return s.fireTimer(nextTimer)
		}
		rpc, ok := s.dequeueReadyRPC()
		if ok {
			return s.deliverRPC(rpc)
		}
	}

	// Only timer available (or no RPC after dequeue) — advance clock and fire it.
	if nextTimer != nil {
		return s.fireTimer(nextTimer)
	}
	return false
}

// nextEventTime returns the time of the next event (the earlier of the next
// ready RPC and the next timer). Returns zero if there are no events.
func (s *Simulation) nextEventTime() time.Time {
	// Check for immediately-ready RPCs
	for i := range s.rpcQueue {
		if s.rpcQueue[i].deliverAt.IsZero() {
			return s.clock.Now()
		}
	}
	// Check for next delayed RPC
	nextDelayed := s.nextDelayedRPC()
	nextTimer := s.timerHeap.Peek()

	if nextDelayed == nil && nextTimer == nil {
		return time.Time{}
	}
	if nextDelayed == nil {
		return nextTimer.fireAt
	}
	if nextTimer == nil {
		return nextDelayed.deliverAt
	}
	if nextDelayed.deliverAt.Before(nextTimer.fireAt) {
		return nextDelayed.deliverAt
	}
	return nextTimer.fireAt
}

// nextDelayedRPC returns the first delayed RPC in the queue, or nil if none.
func (s *Simulation) nextDelayedRPC() *pendingRPC {
	for i := range s.rpcQueue {
		if !s.rpcQueue[i].deliverAt.IsZero() {
			return &s.rpcQueue[i]
		}
	}
	return nil
}

// deliverOneRPC removes and returns the first immediately-ready RPC, or returns
// false if none exists.
func (s *Simulation) dequeueReadyRPC() (pendingRPC, bool) {
	for i := range s.rpcQueue {
		if s.rpcQueue[i].deliverAt.IsZero() {
			rpc := s.rpcQueue[i] // copy before modifying slice
			s.rpcQueue = slices.Delete(s.rpcQueue, i, i+1)
			return rpc, true
		}
	}
	return pendingRPC{}, false
}

// deliverRPC delivers one RPC (by value, already removed from the queue) to
// the target node and resolves the sender's callback.
func (s *Simulation) deliverRPC(rpc pendingRPC) bool {
	target, ok := s.nodes[rpc.to]
	if !ok {
		// Unknown target — resolve sender with error
		rpc.callback(engine.Resp{Kind: engine.SendRPC, Err: fmt.Errorf("unknown node: %s", rpc.to)}, nil)
		return true
	}

	// Set up response capture for the target's Respond callback
	var rpcResp engine.RPCResponse
	responded := false

	// Deliver the RPC as an event to the target node's SimIO
	target.io.deliverEvent(engine.Event{
		Kind:   engine.EventRPC,
		RPCReq: rpc.req,
		Respond: func(resp engine.RPCResponse) {
			rpcResp = resp
			responded = true
		},
	})

	// Step the target node so it processes the event
	target.sched.RunUntilBlocked(s.clock.Now().UnixNano())

	// Resolve the sender's SendRPC callback with the response
	if responded {
		rpc.callback(engine.Resp{Kind: engine.SendRPC, RPCResp: rpcResp}, nil)
	} else {
		rpc.callback(engine.Resp{Kind: engine.SendRPC, Err: fmt.Errorf("no response from %s", rpc.to)}, nil)
	}

	// Step the sender node to resume its awaiting coroutine
	s.stepSender(rpc.from)

	return true
}

// fireTimer pops the earliest timer from the heap, advances the clock, and
// delivers an EventTimer to the owning node.
func (s *Simulation) fireTimer(t *pendingTimer) bool {
	// Advance clock to the timer's fire time
	if t.fireAt.After(s.clock.Now()) {
		s.clock = NewVirtualClock(t.fireAt)
	}

	// Pop from heap
	s.timerHeap.PopTimer()

	target, ok := s.nodes[t.nodeID]
	if !ok {
		return true // node gone, discard
	}

	// Deliver EventTimer to the target node's SimIO
	target.io.deliverEvent(engine.Event{
		Kind:    engine.EventTimer,
		TimerID: t.timerID,
	})

	// Step the target node
	target.sched.RunUntilBlocked(s.clock.Now().UnixNano())

	// Drain any RPCs queued during the step
	s.drainImmediateRPCs()

	return true
}

// drainImmediateRPCs delivers all immediately-ready RPCs (no artificial delay).
// This handles cases where a single step queues multiple outbound RPCs.
func (s *Simulation) drainImmediateRPCs() {
	for {
		rpc, ok := s.dequeueReadyRPC()
		if !ok {
			break
		}
		s.deliverRPC(rpc)
	}
}

// stepSender steps the named node's scheduler so it can resume after a callback.
func (s *Simulation) stepSender(nodeID string) {
	if sn, ok := s.nodes[nodeID]; ok {
		sn.sched.RunUntilBlocked(s.clock.Now().UnixNano())
	}
}

// setPartition sets or clears the partition flag for a directional pair.
func (s *Simulation) setPartition(from, to string, blocked bool) {
	if s.partitions[from] == nil {
		s.partitions[from] = make(map[string]bool)
	}
	s.partitions[from][to] = blocked
}

// isPartitioned returns true if RPCs from→to are blocked by a partition.
func (s *Simulation) isPartitioned(from, to string) bool {
	if m, ok := s.partitions[from]; ok {
		return m[to]
	}
	return false
}
