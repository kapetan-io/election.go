package election

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/kapetan-io/election.go/internal/engine"
)

// Re-exports from internal/engine (type aliases — transparent to callers)
type RPC = engine.RPC
type RPCRequest = engine.RPCRequest
type RPCResponse = engine.RPCResponse
type VoteReq = engine.VoteReq
type VoteResp = engine.VoteResp
type HeartBeatReq = engine.HeartBeatReq
type HeartBeatResp = engine.HeartBeatResp
type ResetElectionReq = engine.ResetElectionReq
type ResetElectionResp = engine.ResetElectionResp
type ResignReq = engine.ResignReq
type ResignResp = engine.ResignResp
type SetPeersReq = engine.SetPeersReq
type SetPeersResp = engine.SetPeersResp
type Peer = engine.Peer

const (
	HeartBeatRPC     = engine.HeartBeatRPC
	VoteRPC          = engine.VoteRPC
	ResetElectionRPC = engine.ResetElectionRPC
	ResignRPC        = engine.ResignRPC
	SetPeersRPC      = engine.SetPeersRPC
)

// Role represents the role of a node in the election
type Role int

const (
	Follower  Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// NodeState is the current state of a node
type NodeState struct {
	Leader   string
	IsLeader bool
	Term     uint64
	Peers    []Peer
	Role     Role
}

// Stats holds counters for node activity
type Stats struct {
	Term             uint64
	Role             Role
	HeartbeatsSent   uint64
	HeartbeatsRecv   uint64
	ElectionsStarted uint64
	ElectionsWon     uint64
}

// SendRPCFunc sends an RPC request to a peer and returns the response
type SendRPCFunc func(ctx context.Context, peer string, req RPCRequest) (RPCResponse, error)

// Node is the public interface for an election node
type Node interface {
	// Start begins participating in the election
	Start(ctx context.Context) error

	// Stop ceases participation in the election
	Stop(ctx context.Context) error

	// SetPeers updates the list of peers considered in the election
	SetPeers(ctx context.Context, peers []string) error

	// Resign steps down as leader; returns ErrNotLeader if not currently leader
	Resign(ctx context.Context) error

	// IsLeader returns true if this node is currently the leader
	IsLeader() bool

	// GetLeader returns the unique ID of the current leader
	GetLeader() string

	// GetState returns the current state of this node
	GetState() NodeState

	// ReceiveRPC handles an incoming RPC request from a peer
	ReceiveRPC(ctx context.Context, req RPCRequest) (RPCResponse, error)

	// Stats returns current activity counters
	Stats() Stats
}

// Config holds the configuration for a node
type Config struct {
	// UniqueID is the identifier this node uses to identify itself among peers
	UniqueID string

	// Peers is the initial list of peers to consider in the election, including this node
	Peers []string

	// SendRPC sends an RPC request to a peer
	SendRPC SendRPCFunc

	// OnChange is called whenever the leader changes or peer metadata is updated.
	// It runs synchronously in the election goroutine; do not call
	// Resign, SetPeers, Stop, or ReceiveRPC from this callback. Must not block.
	OnChange func(NodeState)

	// HeartBeatTimeout is how long followers wait before starting a new election
	HeartBeatTimeout time.Duration

	// ElectionTimeout is how long candidates wait for an election to complete before restarting
	ElectionTimeout time.Duration

	// NetworkTimeout is how long to wait for a single network operation
	NetworkTimeout time.Duration

	// LeaderQuorumTimeout is how long the leader waits on heartbeat responses before stepping down
	LeaderQuorumTimeout time.Duration

	// MinimumQuorum is the minimum number of peers required to elect a leader
	MinimumQuorum int

	// Log is the logger to use; defaults to slog.Default() with node ID attribute
	Log *slog.Logger
}

// ErrNotLeader is returned when an operation requires leadership but the node is not leader
var ErrNotLeader = errors.New("not the leader")
