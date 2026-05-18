package election_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thrawn01/election"
)

// TestProductionSmokeTest validates the production IO adapter wiring by
// creating a 3-node cluster with in-process RPC delivery, starting all nodes,
// and verifying that exactly one node becomes leader. This is the only test
// that uses real wall-clock time — subsequent tests use simulation.
func TestProductionSmokeTest(t *testing.T) {
	const timeout = 30 * time.Second

	// Create 3 nodes; we will wire them together via in-process RPC below.
	type nodeEntry struct {
		node election.Node
		mu   sync.RWMutex
	}

	nodes := make(map[string]*nodeEntry)
	var nodesMu sync.RWMutex

	sendRPC := func(ctx context.Context, peer string, req election.RPCRequest) (election.RPCResponse, error) {
		nodesMu.RLock()
		entry, ok := nodes[peer]
		nodesMu.RUnlock()
		if !ok {
			return election.RPCResponse{}, context.DeadlineExceeded
		}
		entry.mu.RLock()
		n := entry.node
		entry.mu.RUnlock()
		return n.ReceiveRPC(ctx, req)
	}

	peers := []string{"n0", "n1", "n2"}

	nodesMu.Lock()
	for _, id := range peers {
		n, err := election.NewNode(election.Config{
			UniqueID:            id,
			Peers:               peers,
			SendRPC:             sendRPC,
			HeartBeatTimeout:    time.Second,
			ElectionTimeout:     time.Second * 2,
			NetworkTimeout:      time.Second,
			LeaderQuorumTimeout: time.Second * 4,
		})
		require.NoError(t, err)
		nodes[id] = &nodeEntry{node: n}
	}
	nodesMu.Unlock()

	ctx := context.Background()

	// Start all nodes
	for _, id := range peers {
		nodesMu.RLock()
		n := nodes[id].node
		nodesMu.RUnlock()
		require.NoError(t, n.Start(ctx))
	}

	// Verify that exactly one leader is elected
	require.Eventually(t, func() bool {
		nodesMu.RLock()
		defer nodesMu.RUnlock()
		leaders := 0
		for _, entry := range nodes {
			if entry.node.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}, timeout, 100*time.Millisecond)

	// Stop all nodes cleanly
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, id := range peers {
		nodesMu.RLock()
		n := nodes[id].node
		nodesMu.RUnlock()
		require.NoError(t, n.Stop(stopCtx))
	}
}
