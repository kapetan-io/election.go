package election

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// compile-time interface conformance check
var _ Node = (*node)(nil)

type node struct {
	conf Config
	self string
	log  *slog.Logger
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

	return &node{
		conf: conf,
		self: conf.UniqueID,
		log:  conf.Log,
	}, nil
}

func (n *node) Start(_ context.Context) error {
	return nil
}

func (n *node) Stop(_ context.Context) error {
	return nil
}

func (n *node) SetPeers(_ context.Context, _ []string) error {
	return nil
}

func (n *node) Resign(_ context.Context) error {
	return ErrNotLeader
}

func (n *node) IsLeader() bool {
	return false
}

func (n *node) GetLeader() string {
	return ""
}

func (n *node) GetState() NodeState {
	return NodeState{}
}

func (n *node) ReceiveRPC(_ context.Context, _ RPCRequest) (RPCResponse, error) {
	return RPCResponse{}, nil
}

func (n *node) Stats() Stats {
	return Stats{}
}

