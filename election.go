package election

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	gocoro "github.com/resonatehq/gocoro"
	gocoroio "github.com/resonatehq/gocoro/pkg/io"

	"github.com/kapetan-io/election.go/internal/engine"
	"github.com/kapetan-io/election.go/internal/prod"
)

// compile-time interface conformance check
var _ Node = (*node)(nil)

const (
	roleFollower  = 0
	roleCandidate = 1
	roleLeader    = 2
	roleShutdown  = 3
)

// voteResult holds the outcome of a single vote request
type voteResult struct {
	granted bool
	term    uint64
}

type node struct {
	conf Config
	self string
	log  *slog.Logger

	// Atomic fields — external readers
	leader   atomic.Value // string
	isLeader atomic.Bool
	term     atomic.Uint64
	role     atomic.Int32
	peers    atomic.Value // []string

	// Atomic counters for Stats
	heartbeatsSent   atomic.Uint64
	heartbeatsRecv   atomic.Uint64
	electionsStarted atomic.Uint64
	electionsWon     atomic.Uint64

	// Coroutine-only state (not safe for concurrent access)
	currentTerm  uint64
	currentRole  int
	vote         struct {
		LastCandidate string
		LastTerm      uint64
	}
	peersLastContact map[string]time.Time
	lastContact      time.Time
	currentPeers     []string
	currentLeader    string
	rng              *rand.Rand

	// Lifecycle — set by Start, used by Stop and public methods
	started      atomic.Bool
	stopped      atomic.Bool // true once forceShutdown has been called
	shutdownOnce sync.Once   // ensures eventCh is closed at most once
	eventCh      chan engine.Event
	completeCh   chan prod.Completion
	doneCh       chan struct{}
	sched        gocoro.Scheduler[engine.Req, engine.Resp]
	io           *prod.ProdIO
}

// NewNode creates a new election node. Call Start() to participate in the election.
func NewNode(conf Config) (Node, error) {
	if conf.UniqueID == "" {
		return nil, errors.New("refusing to spawn a new node with no Config.UniqueID defined")
	}

	if conf.SendRPC == nil {
		return nil, errors.New("refusing to spawn a new node with no Config.SendRPC defined")
	}

	if conf.LeaderQuorumTimeout == 0 {
		conf.LeaderQuorumTimeout = time.Second * 12
	}
	if conf.HeartBeatTimeout == 0 {
		conf.HeartBeatTimeout = time.Second * 6
	}
	if conf.ElectionTimeout == 0 {
		conf.ElectionTimeout = time.Second * 6
	}
	if conf.NetworkTimeout == 0 {
		conf.NetworkTimeout = time.Second * 3
	}
	if conf.Log == nil {
		conf.Log = slog.Default().With("node", conf.UniqueID)
	}

	n := &node{
		conf:         conf,
		self:         conf.UniqueID,
		log:          conf.Log,
		currentPeers: conf.Peers,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
		// Pre-allocate the event channel so it is safe to read in ReceiveRPC,
		// SetPeers, and Resign even before Start is called.
		eventCh:    make(chan engine.Event, 1000),
		completeCh: make(chan prod.Completion, 1000),
		doneCh:     make(chan struct{}),
	}
	n.peers.Store(conf.Peers)
	n.leader.Store("")
	return n, nil
}

// -------------------------------------------------------------------------
// Node interface — atomic read methods (no coroutine hop)
// -------------------------------------------------------------------------

func (n *node) IsLeader() bool {
	return n.isLeader.Load()
}

func (n *node) GetLeader() string {
	v, _ := n.leader.Load().(string)
	return v
}

func (n *node) GetState() NodeState {
	peers, _ := n.peers.Load().([]string)
	return NodeState{
		Leader:   n.GetLeader(),
		IsLeader: n.isLeader.Load(),
		Term:     n.term.Load(),
		Peers:    peers,
		Role:     Role(n.role.Load()),
	}
}

func (n *node) Stats() Stats {
	return Stats{
		Term:             n.term.Load(),
		Role:             Role(n.role.Load()),
		HeartbeatsSent:   n.heartbeatsSent.Load(),
		HeartbeatsRecv:   n.heartbeatsRecv.Load(),
		ElectionsStarted: n.electionsStarted.Load(),
		ElectionsWon:     n.electionsWon.Load(),
	}
}

// -------------------------------------------------------------------------
// Node lifecycle methods
// -------------------------------------------------------------------------

// Start creates the production IO adapter and gocoro scheduler, adds the main
// run coroutine, and launches the main loop in a background goroutine. It is
// non-blocking; the context governs startup only and is not retained.
// Double-start is prevented by an atomic flag.
func (n *node) Start(_ context.Context) error {
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("node already started")
	}

	// The event and completion channels are pre-allocated in NewNode so they
	// are always safe to access. The doneCh is pre-allocated too.
	n.io = prod.NewProdIO(prod.Config{
		SendRPC:        prod.SendRPCFunc(n.conf.SendRPC),
		NetworkTimeout: n.conf.NetworkTimeout,
		Log:            n.conf.Log,
	}, n.eventCh, n.completeCh)
	n.sched = gocoro.New(n.io, 100)

	_, ok := gocoro.Add(n.sched, n.run)
	if !ok {
		return errors.New("failed to add coroutine to scheduler")
	}

	go prod.Run(n.sched, n.completeCh, n.doneCh)
	return nil
}

// Stop pushes an EventShutdown into the event channel and waits for the main
// loop goroutine to exit. If the context expires before a clean shutdown,
// it calls sched.Shutdown(), closes eventCh, and returns ctx.Err().
// Calling Stop on a node that was never started returns nil immediately.
// Calling Stop after a forced shutdown (context-cancelled Stop) returns nil.
func (n *node) Stop(ctx context.Context) error {
	if !n.started.Load() {
		return nil
	}

	// If forceShutdown was already called (e.g., by a prior Stop with cancelled
	// context), the eventCh is closed — skip the send and wait on doneCh.
	if n.stopped.Load() {
		select {
		case <-n.doneCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Signal the coroutine to shut down
	n.io.SetStopped()
	select {
	case n.eventCh <- engine.Event{Kind: engine.EventShutdown}:
	default:
		// If eventCh is full, non-blocking push failed — try with context
		select {
		case n.eventCh <- engine.Event{Kind: engine.EventShutdown}:
		case <-ctx.Done():
			n.forceShutdown()
			return ctx.Err()
		}
	}

	// Wait for clean shutdown
	select {
	case <-n.doneCh:
		return nil
	case <-ctx.Done():
		n.forceShutdown()
		return ctx.Err()
	}
}

// forceShutdown forcefully shuts down the scheduler and closes eventCh to
// unblock any goroutine waiting on it. Safe to call multiple times.
func (n *node) forceShutdown() {
	n.shutdownOnce.Do(func() {
		n.stopped.Store(true)
		n.sched.Shutdown()
		close(n.eventCh)
	})
}

// ReceiveRPC pushes an EventRPC into the event channel and waits for the
// coroutine to process it and invoke the Respond callback. Only peer-to-peer
// RPCs are accepted (HeartBeatRPC, VoteRPC, ResetElectionRPC). Any other RPC
// type returns an error response without entering the event channel.
func (n *node) ReceiveRPC(ctx context.Context, req RPCRequest) (RPCResponse, error) {
	if !n.started.Load() {
		return RPCResponse{Error: "node not started"}, nil
	}

	switch req.RPC {
	case HeartBeatRPC, VoteRPC, ResetElectionRPC:
		// Accepted peer-to-peer RPCs
	default:
		return RPCResponse{Error: "unknown RPC"}, nil
	}

	respCh := make(chan RPCResponse, 1)
	event := engine.Event{
		Kind:   engine.EventRPC,
		RPCReq: req,
		Respond: func(resp RPCResponse) {
			respCh <- resp
		},
	}

	select {
	case n.eventCh <- event:
	case <-ctx.Done():
		return RPCResponse{}, ctx.Err()
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return RPCResponse{}, ctx.Err()
	}
}

// SetPeers updates the list of peers. If the node has not been started yet,
// the update is applied directly (the coroutine is not running). After start,
// the update is pushed through the event channel so the coroutine processes it
// safely.
func (n *node) SetPeers(ctx context.Context, peers []string) error {
	if !n.started.Load() {
		// Node not started — update directly (no coroutine running yet)
		n.currentPeers = peers
		n.peers.Store(peers)
		return nil
	}

	doneCh := make(chan error, 1)
	event := engine.Event{
		Kind:  engine.EventSetPeers,
		Peers: peers,
		Done: func(err error) {
			doneCh <- err
		},
	}

	select {
	case n.eventCh <- event:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resign pushes an EventResign into the event channel and waits for the
// coroutine to process it. Returns ErrNotLeader if the node is not the leader.
func (n *node) Resign(ctx context.Context) error {
	if !n.started.Load() {
		return ErrNotLeader
	}

	doneCh := make(chan error, 1)
	event := engine.Event{
		Kind: engine.EventResign,
		Done: func(err error) {
			doneCh <- err
		},
	}

	select {
	case n.eventCh <- event:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// -------------------------------------------------------------------------
// Coroutine state machine
// -------------------------------------------------------------------------

func (n *node) run(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) (struct{}, error) {
	n.currentRole = roleFollower
	n.role.Store(int32(roleFollower))

	for {
		switch n.currentRole {
		case roleFollower:
			n.runFollower(c)
		case roleCandidate:
			n.runCandidate(c)
		case roleLeader:
			n.runLeader(c)
		case roleShutdown:
			return struct{}{}, nil
		}
	}
}

func (n *node) runFollower(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) {
	n.log.Debug("entering follower state", "leader", n.currentLeader)

	// Register heartbeat timer (randomized so nodes don't all fire at once)
	heartbeatDelay := n.randomDuration(n.conf.HeartBeatTimeout)
	hbResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: heartbeatDelay})
	heartbeatTimerID := hbResp.TimerID

	// No-peers timer fires sooner — 1/5 of heartbeat timeout
	noPeersDelay := heartbeatDelay / 5
	npResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: noPeersDelay})
	noPeersTimerID := npResp.TimerID

	for n.currentRole == roleFollower {
		resp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Recv})
		event := resp.Event

		switch event.Kind {
		case engine.EventTimer:
			switch event.TimerID {
			case heartbeatTimerID:
				// Check if we have had successful contact with the leader recently
				nowResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Now})
				now := nowResp.Time
				if now.Sub(n.lastContact) < n.conf.HeartBeatTimeout {
					// Re-register heartbeat timer
					hbResp2, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: heartbeatDelay})
					heartbeatTimerID = hbResp2.TimerID
					continue
				}
				// Heartbeat failed! Transition to candidate state
				n.log.Debug("heartbeat timeout, starting election", "previous_leader", n.currentLeader)
				n.setLeader("")
				n.currentRole = roleCandidate
				// Cancel the no-peers timer before returning
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: noPeersTimerID}) //nolint:errcheck
				return

			case noPeersTimerID:
				// If we already have a leader, skip
				if n.currentLeader != "" {
					// Re-register noPeers timer
					npResp2, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: noPeersDelay})
					noPeersTimerID = npResp2.TimerID
					continue
				}
				// If we have no peers, or are the only peer, become candidate immediately
				if len(n.currentPeers) == 0 || (len(n.currentPeers) == 1 && n.currentPeers[0] == n.self) {
					n.currentRole = roleCandidate
					// Cancel the heartbeat timer before returning
					gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
					return
				}
				// Re-register noPeers timer
				npResp2, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: noPeersDelay})
				noPeersTimerID = npResp2.TimerID
			}

		case engine.EventRPC:
			n.processRPC(c, event)

		case engine.EventSetPeers:
			n.handleSetPeers(c, event, SetPeersReq{Peers: event.Peers})

		case engine.EventResign:
			if event.Done != nil {
				event.Done(ErrNotLeader)
			}

		case engine.EventShutdown:
			n.currentRole = roleShutdown
			// Cancel timers before returning
			gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
			gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: noPeersTimerID})   //nolint:errcheck
			return
		}
	}
}

func (n *node) runCandidate(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) {
	n.log.Debug("entering candidate state", "term", n.currentTerm+1)
	n.electionsStarted.Add(1)

	// Vote timer: randomized delay before sending vote requests
	voteDelay := n.randomDuration(n.conf.HeartBeatTimeout / 10)
	vtResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: voteDelay})
	voteTimerID := vtResp.TimerID

	// Election timer: if no winner before this, restart election
	electionDelay := n.randomDuration(n.conf.ElectionTimeout)
	etResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: electionDelay})
	electionTimerID := etResp.TimerID

	grantedVotes := 0
	votesNeeded := n.quorumSize()
	n.log.Debug("votes needed", "count", votesNeeded)

	var pendingVotes []voteResult

	for n.currentRole == roleCandidate {
		resp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Recv})
		event := resp.Event

		switch event.Kind {
		case engine.EventTimer:
			switch event.TimerID {
			case voteTimerID:
				// Do not start a vote if below minimum quorum
				if len(n.currentPeers) < n.conf.MinimumQuorum {
					n.log.Warn("peer count below minimum quorum; sleeping",
						"peers", len(n.currentPeers),
						"minimum", n.conf.MinimumQuorum)
					// Re-register vote timer and wait
					vtResp2, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: voteDelay})
					voteTimerID = vtResp2.TimerID
					continue
				}
				// Start election: collect votes
				pendingVotes = n.electSelf(c)
				// Process all collected votes immediately
				for _, v := range pendingVotes {
					if v.term > n.currentTerm {
						n.log.Debug("newer term discovered, falling back to follower")
						n.currentRole = roleFollower
						n.currentTerm = v.term
						n.term.Store(n.currentTerm)
						n.role.Store(int32(roleFollower))
						gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: electionTimerID}) //nolint:errcheck
						return
					}
					if v.granted {
						grantedVotes++
						n.log.Debug("vote granted", "tally", grantedVotes)
					}
					if grantedVotes >= votesNeeded {
						n.log.Debug("election won!", "tally", grantedVotes)
						n.currentRole = roleLeader
						n.role.Store(int32(roleLeader))
						n.electionsWon.Add(1)
						n.setLeader(n.self)
						gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: voteTimerID})    //nolint:errcheck
						gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: electionTimerID}) //nolint:errcheck
						return
					}
				}

			case electionTimerID:
				// Election timed out — return to re-enter runCandidate
				n.log.Debug("election timeout, restarting election")
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: voteTimerID}) //nolint:errcheck
				return
			}

		case engine.EventRPC:
			n.processRPC(c, event)
			// If we transitioned out of candidate state due to an RPC, exit
			if n.currentRole != roleCandidate {
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: voteTimerID})    //nolint:errcheck
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: electionTimerID}) //nolint:errcheck
				return
			}

		case engine.EventSetPeers:
			n.handleSetPeers(c, event, SetPeersReq{Peers: event.Peers})

		case engine.EventResign:
			if event.Done != nil {
				event.Done(ErrNotLeader)
			}

		case engine.EventShutdown:
			n.currentRole = roleShutdown
			n.role.Store(int32(roleShutdown))
			gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: voteTimerID})    //nolint:errcheck
			gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: electionTimerID}) //nolint:errcheck
			return
		}
	}
}

// electSelf increments the current term, votes for itself, and sends vote requests
// to all peers. Returns a slice of vote results (including our own self-vote).
// This fixes the double-send bug in the original by always returning a VoteResp on error.
func (n *node) electSelf(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) []voteResult {
	// Increment the term
	n.currentTerm++
	n.term.Store(n.currentTerm)

	// Record our vote for ourselves
	n.vote.LastCandidate = n.self
	n.vote.LastTerm = n.currentTerm

	term := n.currentTerm
	results := make([]voteResult, 0, len(n.currentPeers))

	// Vote for ourselves first
	results = append(results, voteResult{granted: true, term: term})

	// Spawn one coroutine per peer and collect promises
	type voteAwaitable = interface {
		Value() VoteResp
		Error() error
		Pending() bool
		Completed() bool
	}

	awaitables := make([]voteAwaitable, 0, len(n.currentPeers))

	for _, peer := range n.currentPeers {
		if peer == n.self {
			continue
		}
		peerCopy := peer
		termCopy := term
		selfCopy := n.self

		p := gocoro.Spawn(c, func(cc gocoro.Coroutine[engine.Req, engine.Resp, VoteResp]) (VoteResp, error) {
			resp, _ := gocoro.YieldAndAwait(cc, engine.Req{
				Kind: engine.SendRPC,
				Peer: peerCopy,
				RPCReq: engine.RPCRequest{
					RPC: engine.VoteRPC,
					Request: engine.VoteReq{
						Term:      termCopy,
						Candidate: selfCopy,
					},
				},
			})
			if resp.Err != nil {
				return VoteResp{Term: termCopy, Granted: false}, nil
			}
			vResp, ok := resp.RPCResp.Response.(VoteResp)
			if !ok {
				return VoteResp{Term: termCopy, Granted: false}, nil
			}
			return vResp, nil
		})

		awaitables = append(awaitables, p)
	}

	// Await each promise sequentially (all SendRPCs are in-flight concurrently)
	for _, p := range awaitables {
		vResp, _ := gocoro.Await(c, p)
		results = append(results, voteResult{granted: vResp.Granted, term: vResp.Term})
	}

	return results
}

func (n *node) runLeader(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) {
	n.log.Debug("entering leader state", "term", n.currentTerm)
	n.peersLastContact = make(map[string]time.Time, len(n.currentPeers))

	// Register heartbeat ticker (re-registered on each fire)
	heartbeatDelay := n.randomDuration(n.conf.HeartBeatTimeout / 3)
	hbResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: heartbeatDelay})
	heartbeatTimerID := hbResp.TimerID

	// Register quorum check timer
	quorumResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: n.conf.LeaderQuorumTimeout})
	quorumTimerID := quorumResp.TimerID

	for n.currentRole == roleLeader {
		resp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Recv})
		event := resp.Event

		switch event.Kind {
		case engine.EventTimer:
			switch event.TimerID {
			case heartbeatTimerID:
				// Send heartbeats to all peers (fire-and-forget spawns)
				n.sendHeartBeats(c)
				// Re-register heartbeat timer
				hbResp2, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: heartbeatDelay})
				heartbeatTimerID = hbResp2.TimerID

			case quorumTimerID:
				// Check minimum quorum
				if len(n.currentPeers) < n.conf.MinimumQuorum {
					n.log.Warn("peer count below minimum quorum; stepping down",
						"peers", len(n.currentPeers),
						"minimum", n.conf.MinimumQuorum)
					n.stepDownFromLeader(c)
					gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
					return
				}

				// Check if we've received heartbeat responses from a quorum of peers
				nowResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Now})
				now := nowResp.Time
				contacted := 0
				for _, peer := range n.currentPeers {
					if peer == n.self {
						continue
					}
					lc, ok := n.peersLastContact[peer]
					if !ok {
						continue
					}
					if now.Sub(lc) < n.conf.HeartBeatTimeout {
						contacted++
					}
				}

				quorum := n.quorumSize()
				n.log.Debug("quorum check", "quorum", quorum-1, "contacted", contacted)
				if contacted < (quorum - 1) {
					n.log.Warn("failed to receive heartbeats from a quorum of peers; stepping down")
					n.stepDownFromLeader(c)
					gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
					return
				}

				// Re-register quorum timer
				quorumResp2, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.After, Delay: n.conf.LeaderQuorumTimeout})
				quorumTimerID = quorumResp2.TimerID
			}

		case engine.EventRPC:
			n.processRPC(c, event)
			// If we transitioned out of leader state, exit
			if n.currentRole != roleLeader {
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: quorumTimerID})    //nolint:errcheck
				return
			}

		case engine.EventSetPeers:
			n.handleSetPeers(c, event, SetPeersReq{Peers: event.Peers})
			// If leader, immediately send heartbeats to new peers
			if n.currentRole == roleLeader {
				n.sendHeartBeats(c)
			}

		case engine.EventResign:
			n.handleResign(c, event)
			if n.currentRole != roleLeader {
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
				gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: quorumTimerID})    //nolint:errcheck
				return
			}

		case engine.EventShutdown:
			n.currentRole = roleShutdown
			n.role.Store(int32(roleShutdown))
			// Notify all peers we are stepping down
			n.sendElectionResets(c)
			gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: heartbeatTimerID}) //nolint:errcheck
			gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Cancel, TimerID: quorumTimerID})    //nolint:errcheck
			return
		}
	}

	// Exiting leader state (e.g. due to receiving a heartbeat from another leader)
	// Reset lastContact so the node gives itself grace time as a new follower
	nowResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Now})
	n.lastContact = nowResp.Time
	if n.currentLeader == n.self {
		n.setLeader("")
	}
}

// sendHeartBeats spawns a fire-and-forget coroutine per peer to send heartbeats.
// Spawned coroutines update peersLastContact directly (safe because coroutines are cooperative).
func (n *node) sendHeartBeats(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) {
	term := n.currentTerm
	for _, peer := range n.currentPeers {
		if peer == n.self {
			continue
		}
		peerCopy := peer
		selfCopy := n.self
		gocoro.Spawn(c, func(cc gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) (struct{}, error) {
			resp, _ := gocoro.YieldAndAwait(cc, engine.Req{
				Kind: engine.SendRPC,
				Peer: peerCopy,
				RPCReq: engine.RPCRequest{
					RPC: engine.HeartBeatRPC,
					Request: engine.HeartBeatReq{
						Term:   term,
						Leader: selfCopy,
					},
				},
			})
			n.heartbeatsSent.Add(1)
			if resp.Err != nil {
				return struct{}{}, nil
			}
			hResp, ok := resp.RPCResp.Response.(HeartBeatResp)
			if !ok {
				return struct{}{}, nil
			}
			// Update term if peer has a newer one
			if hResp.Term > term {
				n.currentTerm = hResp.Term
				n.term.Store(n.currentTerm)
			}
			nowResp, _ := gocoro.YieldAndAwait(cc, engine.Req{Kind: engine.Now})
			n.peersLastContact[peerCopy] = nowResp.Time
			return struct{}{}, nil
		})
	}
}

// sendElectionResets spawns fire-and-forget coroutines to notify peers we are stepping down.
func (n *node) sendElectionResets(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) {
	for _, peer := range n.currentPeers {
		if peer == n.self {
			continue
		}
		peerCopy := peer
		gocoro.Spawn(c, func(cc gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) (struct{}, error) {
			gocoro.YieldAndAwait(cc, engine.Req{ //nolint:errcheck
				Kind: engine.SendRPC,
				Peer: peerCopy,
				RPCReq: engine.RPCRequest{
					RPC:     engine.ResetElectionRPC,
					Request: engine.ResetElectionReq{},
				},
			})
			return struct{}{}, nil
		})
	}
}

// stepDownFromLeader transitions the leader to follower and notifies peers.
func (n *node) stepDownFromLeader(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) {
	n.currentRole = roleFollower
	n.role.Store(int32(roleFollower))
	n.setLeader("")
	n.sendElectionResets(c)
}

// -------------------------------------------------------------------------
// RPC Handlers
// -------------------------------------------------------------------------

func (n *node) processRPC(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}], event engine.Event) {
	switch cmd := event.RPCReq.Request.(type) {
	case VoteReq:
		n.handleVote(c, event, cmd)
	case ResetElectionReq:
		n.handleResetElection(event)
	case HeartBeatReq:
		n.handleHeartBeat(c, event, cmd)
	case ResignReq:
		n.handleResign(c, event)
	case SetPeersReq:
		n.handleSetPeers(c, event, cmd)
	default:
		n.log.Error("unexpected RPC command", "request", event.RPCReq.Request)
		if event.Respond != nil {
			event.Respond(RPCResponse{RPC: event.RPCReq.RPC, Error: "unexpected command"})
		}
	}
}

func (n *node) handleVote(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}], event engine.Event, req VoteReq) {
	n.log.Debug("RPC: VoteReq", "req", req)
	resp := VoteResp{
		Term:    n.currentTerm,
		Granted: false,
	}

	defer func() {
		if event.Respond != nil {
			event.Respond(RPCResponse{RPC: VoteRPC, Response: resp})
		}
	}()

	// Reject vote if we have an existing leader who is not the candidate
	if n.currentLeader != "" && n.currentLeader != req.Candidate {
		n.log.Debug("rejecting vote; already have leader",
			"candidate", req.Candidate, "leader", n.currentLeader)
		return
	}

	// Ignore older terms
	if req.Term < n.currentTerm {
		return
	}

	// Update to newer term
	if req.Term > n.currentTerm {
		n.log.Debug("received vote request with newer term", "term", req.Term)
		n.currentRole = roleFollower
		n.role.Store(int32(roleFollower))
		n.currentTerm = req.Term
		n.term.Store(n.currentTerm)
		resp.Term = req.Term
	}

	// Check if we already voted in this term
	if n.vote.LastTerm == req.Term && n.vote.LastCandidate != "" {
		n.log.Debug("already voted in this term",
			"candidate", req.Candidate, "voted_for", n.vote.LastCandidate)
		if n.vote.LastCandidate == req.Candidate {
			n.log.Debug("duplicate vote request from same candidate", "candidate", req.Candidate)
			resp.Granted = true
		}
		return
	}

	// Vote for the first candidate we hear from in this term
	n.vote.LastTerm = req.Term
	n.vote.LastCandidate = req.Candidate
	resp.Granted = true

	// Record last contact via Now yield (enables virtual clock in simulation)
	nowResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Now})
	n.lastContact = nowResp.Time
}

func (n *node) handleHeartBeat(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}], event engine.Event, req HeartBeatReq) {
	n.log.Debug("RPC: HeartBeatReq", "req", req)

	resp := HeartBeatResp{
		Term: n.currentTerm,
	}

	defer func() {
		if event.Respond != nil {
			event.Respond(RPCResponse{RPC: HeartBeatRPC, Response: resp})
		}
	}()

	// If we are not a follower (e.g., we think we are leader or candidate),
	// step down to follower — another node has legitimately won the election.
	if n.currentRole != roleFollower {
		n.currentRole = roleFollower
		n.role.Store(int32(roleFollower))
		resp.Term = req.Term
	}

	// Always update to the most current term
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.term.Store(n.currentTerm)
	}

	// Record the leader from the heartbeat
	n.setLeader(req.Leader)
	n.heartbeatsRecv.Add(1)

	// Update last contact time via Now yield
	nowResp, _ := gocoro.YieldAndAwait(c, engine.Req{Kind: engine.Now})
	n.lastContact = nowResp.Time
}

func (n *node) handleResetElection(event engine.Event) {
	n.log.Debug("RPC: ResetElectionReq")
	n.setLeader("")
	n.currentRole = roleCandidate
	n.role.Store(int32(roleCandidate))
	if event.Respond != nil {
		event.Respond(RPCResponse{RPC: ResetElectionRPC, Response: ResetElectionResp{}})
	}
}

func (n *node) handleResign(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}], event engine.Event) {
	n.log.Debug("RPC: ResignReq or EventResign")

	if n.currentRole != roleLeader {
		if event.Done != nil {
			event.Done(ErrNotLeader)
		}
		return
	}

	// Step down as leader
	n.currentRole = roleFollower
	n.role.Store(int32(roleFollower))
	n.setLeader("")

	// Notify all peers
	n.sendElectionResets(c)

	if event.Done != nil {
		event.Done(nil)
	}
}

func (n *node) handleSetPeers(_ gocoro.Coroutine[engine.Req, engine.Resp, struct{}], event engine.Event, req SetPeersReq) {
	n.log.Debug("RPC: SetPeersReq", "peers", req.Peers)
	n.currentPeers = req.Peers
	n.peers.Store(req.Peers)
	if event.Done != nil {
		event.Done(nil)
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func (n *node) setLeader(leader string) {
	if n.currentLeader != leader {
		n.log.Debug("leader changed", "leader", leader)
		n.currentLeader = leader
		n.leader.Store(leader)
		if leader == n.self {
			n.isLeader.Store(true)
		} else {
			n.isLeader.Store(false)
		}
		if n.conf.OnLeaderChange != nil {
			n.conf.OnLeaderChange(leader)
		}
	}
}

func (n *node) quorumSize() int {
	size := len(n.currentPeers)
	if size == 0 {
		return 1
	}
	return size/2 + 1
}

func (n *node) randomDuration(minDur time.Duration) time.Duration {
	return minDur + time.Duration(n.rng.Int63())%minDur //nolint:gosec
}

// -------------------------------------------------------------------------
// Simulation helpers (internal use only — not part of the public Node API)
// -------------------------------------------------------------------------

// NewNodeForSim creates a node with a caller-provided *rand.Rand instead of
// seeding from wall-clock time. Used by internal/sim to inject a deterministic
// RNG seeded from the simulation master seed.
func NewNodeForSim(conf Config, rng *rand.Rand) (Node, error) {
	if conf.UniqueID == "" {
		return nil, errors.New("refusing to spawn a new node with no Config.UniqueID defined")
	}
	if conf.SendRPC == nil {
		return nil, errors.New("refusing to spawn a new node with no Config.SendRPC defined")
	}

	if conf.LeaderQuorumTimeout == 0 {
		conf.LeaderQuorumTimeout = time.Second * 12
	}
	if conf.HeartBeatTimeout == 0 {
		conf.HeartBeatTimeout = time.Second * 6
	}
	if conf.ElectionTimeout == 0 {
		conf.ElectionTimeout = time.Second * 6
	}
	if conf.NetworkTimeout == 0 {
		conf.NetworkTimeout = time.Second * 3
	}
	if conf.Log == nil {
		conf.Log = slog.Default().With("node", conf.UniqueID)
	}

	n := &node{
		conf:         conf,
		self:         conf.UniqueID,
		log:          conf.Log,
		currentPeers: conf.Peers,
		rng:          rng,
		eventCh:      make(chan engine.Event, 1000),
		completeCh:   make(chan prod.Completion, 1000),
		doneCh:       make(chan struct{}),
	}
	n.peers.Store(conf.Peers)
	n.leader.Store("")
	return n, nil
}

// StartNodeForSim wires a node to a caller-provided scheduler and IO adapter
// instead of creating the production ones. Used by internal/sim to inject
// simulation IO. This is not part of the public Node interface.
func StartNodeForSim(nd Node, sched gocoro.Scheduler[engine.Req, engine.Resp], io gocoroio.IO[engine.Req, engine.Resp]) error {
	n, ok := nd.(*node)
	if !ok {
		return errors.New("StartNodeForSim: node must be a *node")
	}
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("node already started")
	}

	n.sched = sched
	_, ok = gocoro.Add(n.sched, n.run)
	if !ok {
		return errors.New("failed to add coroutine to scheduler")
	}

	// The IO adapter is not stored on the node because the simulation drives
	// the scheduler directly — there is no prod.Run goroutine to invoke.
	_ = io
	return nil
}

