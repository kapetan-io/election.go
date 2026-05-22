package election_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kapetan-io/election.go"
	"github.com/kapetan-io/election.go/internal/sim"
)

// TestSingleNodeLeader verifies that a single-node cluster becomes its own leader.
func TestSingleNodeLeader(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 1, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	assert.Equal(t, "n0", s.Leader())
	assert.True(t, s.Node("n0").IsLeader())
	assert.Equal(t, 1, s.LeaderCount())
}

// TestSimpleElection verifies a 5-node cluster elects a leader, and a new
// leader is elected after the current one resigns.
func TestSimpleElection(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)
	assert.Equal(t, 1, s.LeaderCount())

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Resign the current leader
	err = s.Resign(leader)
	require.NoError(t, err)

	// Run until a new leader is elected
	err = s.RunUntilLeader()
	require.NoError(t, err)

	newLeader := s.Leader()
	require.NotEmpty(t, newLeader)
	assert.NotEqual(t, leader, newLeader)
}

// TestLeaderDisconnect verifies that when the leader is isolated (all outgoing
// RPCs dropped), a new leader is elected from the remaining nodes.
func TestLeaderDisconnect(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Partition the leader away from all other nodes
	others := make([]string, 0, 4)
	for _, id := range []string{"n0", "n1", "n2", "n3", "n4"} {
		if id != leader {
			others = append(others, id)
		}
	}
	s.Partition([]string{leader}, others)

	// Run for enough virtual time to detect heartbeat timeout and elect a new leader
	s.RunFor(60 * time.Second)

	// The old leader must have stepped down
	assert.False(t, s.Node(leader).IsLeader())

	// A new leader should exist among the majority
	newLeader := ""
	for _, id := range others {
		if s.Node(id).IsLeader() {
			newLeader = id
			break
		}
	}
	assert.NotEmpty(t, newLeader)
}

// TestFollowerDisconnect verifies that when a follower is partitioned, it loses
// its leader reference. After healing, it should follow the leader again.
func TestFollowerDisconnect(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Partition n4 away from the rest of the cluster
	s.Partition([]string{"n4"}, []string{"n0", "n1", "n2", "n3"})

	// Run for enough virtual time that n4 times out and loses its leader
	s.RunFor(30 * time.Second)

	// n4 should no longer follow the leader
	assert.Equal(t, "", s.Node("n4").GetLeader())

	// Heal the partition
	s.HealAll()

	// Run more virtual time so n4 receives heartbeats and re-joins
	s.RunFor(30 * time.Second)

	// n4 should now follow the cluster leader
	assert.NotEmpty(t, s.Node("n4").GetLeader())
	assert.Equal(t, s.Leader(), s.Node("n4").GetLeader())
}

// TestSplitBrain verifies that after partitioning a 5-node cluster into
// minority {n0,n1} and majority {n2,n3,n4}, only the majority can elect a
// leader. After healing the partition, a single leader is established.
func TestSplitBrain(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Partition: minority {n0,n1} vs majority {n2,n3,n4}
	s.Partition([]string{"n0", "n1"}, []string{"n2", "n3", "n4"})

	// Run for enough virtual time for the partition to take effect
	s.RunFor(60 * time.Second)

	// The minority cannot achieve quorum (2 < 3), so at most 1 leader total
	// from the majority. The minority nodes should not be leader.
	assert.False(t, s.Node("n0").IsLeader())
	assert.False(t, s.Node("n1").IsLeader())

	// Heal the partition and run until a single leader is established
	s.HealAll()
	s.RunFor(60 * time.Second)

	// Exactly one leader should exist across all nodes
	assert.Equal(t, 1, s.LeaderCount())
}

// TestOmissionFaults verifies that the leader (n0) retains leadership in the
// face of selective omission faults in the cluster. The fault pattern ensures
// n0 can still reach a quorum (n1 and n2) while other connectivity is impaired.
func TestOmissionFaults(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Find which nodes can still form quorum with the leader.
	// The omission pattern: n3↔n4, leader↔n4, leader↔n3, n2↔n4, n1↔n3 blocked.
	// With n0 as leader, n0 can reach n1 and n2 → quorum of 3 is satisfied.
	// We replicate the original test's topology with the actual leader.
	var node3, node4 string
	available := []string{}
	for _, id := range []string{"n0", "n1", "n2", "n3", "n4"} {
		if id != leader {
			available = append(available, id)
		}
	}
	// Use the last two nodes as the "isolated" pair
	node3 = available[len(available)-2]
	node4 = available[len(available)-1]
	node1 := available[0]
	node2 := available[1]

	// Bidirectional omissions matching the original test topology
	s.Partition([]string{node3}, []string{node4})
	s.Partition([]string{leader}, []string{node4})
	s.Partition([]string{leader}, []string{node3})
	s.Partition([]string{node2}, []string{node4})
	s.Partition([]string{node1}, []string{node3})

	// Run for enough virtual time to detect stability
	s.RunFor(30 * time.Second)

	// The leader should have retained leadership because it can still reach quorum
	assert.True(t, s.Node(leader).IsLeader())
	assert.Equal(t, leader, s.Leader())
}

// TestIsolatedLeader verifies that when the leader is cut off from a majority
// of followers, it steps down and the majority elects a new leader.
func TestIsolatedLeader(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Build the majority group (excluding the leader)
	majority := make([]string, 0, 4)
	for _, id := range []string{"n0", "n1", "n2", "n3", "n4"} {
		if id != leader {
			majority = append(majority, id)
		}
	}

	// Isolate the leader from all but one follower
	// Partition leader from all followers except the first one
	isolated := majority[1:] // leader can only reach majority[0]
	s.Partition([]string{leader}, isolated)

	// Run for enough virtual time for the quorum check to fail and a new leader to emerge
	s.RunFor(120 * time.Second)

	// The old leader must have stepped down
	assert.False(t, s.Node(leader).IsLeader())

	// A new leader should exist among the majority
	newLeader := s.Leader()
	assert.NotEmpty(t, newLeader)
	assert.NotEqual(t, leader, newLeader)
	assert.Equal(t, election.Follower, s.Node(leader).GetState().Role)
}

// TestMinimumQuorum verifies that a node enforces MinimumQuorum by refusing to
// lead when its peer list shrinks below the configured minimum.
func TestMinimumQuorum(t *testing.T) {
	// Create a 2-node cluster where both require at least 2 peers to elect a leader
	nodeConf := sim.NodeSimConfig{MinimumQuorum: 2}
	s := sim.New(sim.Config{
		NumNodes: 2,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": nodeConf,
			"n1": nodeConf,
		},
	})

	// Both nodes should be able to elect a leader (2 peers, quorum=2)
	err := s.RunUntilLeader()
	require.NoError(t, err)
	assert.Equal(t, 1, s.LeaderCount())

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Reduce n0's peer list to only itself, simulating n1 leaving the cluster
	// This drops len(currentPeers) below MinimumQuorum for n0
	s.SetPeers("n0", []string{"n0"})

	// Run for long enough that the quorum check timer fires
	s.RunFor(60 * time.Second)

	// n0 should have stepped down since it no longer meets MinimumQuorum
	assert.False(t, s.Node("n0").IsLeader())
}

// TestResign verifies that only the leader can resign, and that a new leader is
// elected after resignation.
func TestResign(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Find a follower
	follower := ""
	for _, id := range []string{"n0", "n1", "n2", "n3", "n4"} {
		if id != leader {
			follower = id
			break
		}
	}
	require.NotEmpty(t, follower)

	// Resigning a follower must return ErrNotLeader
	err = s.Resign(follower)
	require.ErrorContains(t, err, "not the leader")

	// Leader should be unchanged
	assert.Equal(t, leader, s.Leader())

	// Resign the actual leader
	err = s.Resign(leader)
	require.NoError(t, err)

	// Run until a new leader is elected
	err = s.RunUntilLeader()
	require.NoError(t, err)

	newLeader := s.Leader()
	require.NotEmpty(t, newLeader)
	assert.NotEqual(t, leader, newLeader)
}

// TestResignSingleNode verifies that a single-node cluster re-elects itself
// after resigning.
func TestResignSingleNode(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 1, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)
	assert.Equal(t, "n0", s.Leader())

	err = s.Resign("n0")
	require.NoError(t, err)

	// n0 should re-elect itself since it is the only node
	err = s.RunUntilLeader()
	require.NoError(t, err)
	assert.Equal(t, "n0", s.Leader())
}

// TestRPCDrops verifies that a leader is eventually elected even when 50% of
// all RPCs are dropped globally. This demonstrates the simulation's fault
// injection capability under high packet loss.
func TestRPCDrops(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42, DropRate: 0.5})

	// Even with 50% packet loss a leader should eventually emerge
	err := s.RunUntilLeader()
	require.NoError(t, err)

	assert.Equal(t, 1, s.LeaderCount())
	require.NotEmpty(t, s.Leader())

	// Verify the cluster remains stable under continued packet loss
	s.RunFor(30 * time.Second)
	assert.Equal(t, 1, s.LeaderCount())
}

// TestPartitionedMinorityCantElect verifies that a partitioned minority of one
// node cannot elect itself as leader when it lacks quorum.
func TestPartitionedMinorityCantElect(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Partition n0 away from the majority
	s.Partition([]string{"n0"}, []string{"n1", "n2", "n3", "n4"})

	// Run for long enough that n0 would attempt re-election
	s.RunFor(60 * time.Second)

	// n0 cannot achieve quorum (1 < 3) so it must not be leader
	assert.False(t, s.Node("n0").IsLeader())

	// Exactly one leader must exist (in the majority)
	assert.Equal(t, 1, s.LeaderCount())

	// The leader must not be n0
	assert.NotEqual(t, "n0", s.Leader())
}

// TestOnLeaderChangeCallback verifies that the OnChange callback fires
// when a new leader is elected and again when the leader changes.
func TestOnLeaderChangeCallback(t *testing.T) {
	type leaderChange struct {
		leader string
		term   uint64
	}

	var mu sync.Mutex
	changes := make(map[string][]leaderChange) // nodeID → list of leader changes reported

	collect := func(nodeID string) func(election.NodeState) {
		return func(state election.NodeState) {
			mu.Lock()
			defer mu.Unlock()
			changes[nodeID] = append(changes[nodeID], leaderChange{leader: state.Leader, term: state.Term})
		}
	}

	s := sim.New(sim.Config{
		NumNodes: 3,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": {OnChange: collect("n0")},
			"n1": {OnChange: collect("n1")},
			"n2": {OnChange: collect("n2")},
		},
	})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// At least one node must have received the leader change notification
	mu.Lock()
	totalChanges := 0
	for _, v := range changes {
		totalChanges += len(v)
	}
	mu.Unlock()
	assert.Greater(t, totalChanges, 0)

	// Term delivered in the callback must be non-zero when a leader is set,
	// and must match GetState().Term on the leader node at that point.
	mu.Lock()
	var firstElectionTerm uint64
	for _, nodeChanges := range changes {
		for _, c := range nodeChanges {
			if c.leader != "" {
				assert.NotZero(t, c.term)
				if firstElectionTerm == 0 {
					firstElectionTerm = c.term
				}
				assert.Equal(t, firstElectionTerm, c.term)
			}
		}
	}
	mu.Unlock()

	leaderState := s.Node(leader).GetState()
	assert.Equal(t, firstElectionTerm, leaderState.Term)

	// Resign the leader and verify a second callback fires
	err = s.Resign(leader)
	require.NoError(t, err)

	err = s.RunUntilLeader()
	require.NoError(t, err)

	newLeader := s.Leader()
	require.NotEmpty(t, newLeader)
	assert.NotEqual(t, leader, newLeader)

	// More changes should have been recorded after leadership transition
	mu.Lock()
	totalChanges2 := 0
	for _, v := range changes {
		totalChanges2 += len(v)
	}
	mu.Unlock()
	assert.Greater(t, totalChanges2, totalChanges)

	// When a new leader is elected, the Term must be strictly greater than
	// the previous election's Term.
	mu.Lock()
	var secondElectionTerm uint64
	for _, nodeChanges := range changes {
		for _, c := range nodeChanges {
			if c.leader == newLeader {
				assert.NotZero(t, c.term)
				if secondElectionTerm == 0 {
					secondElectionTerm = c.term
				}
				assert.Equal(t, secondElectionTerm, c.term)
			}
		}
	}
	mu.Unlock()

	assert.Greater(t, secondElectionTerm, firstElectionTerm)

	newLeaderState := s.Node(newLeader).GetState()
	assert.Equal(t, secondElectionTerm, newLeaderState.Term)
}

// TestUnknownPeerCanWinElection verifies that a node not in other nodes'
// currentPeers can still win an election. This simulates DNS propagation delay:
// a 5-node cluster starts normally, then n0-n3's DNS view is updated to exclude
// n4 (simulating n4 disappearing from DNS). n4 still knows about everyone.
//
// After the leader is partitioned and the remaining followers are isolated
// from each other, n4 is the only node that can achieve quorum:
//   - n0-n3 only know about [n0, n1, n2, n3] (quorum=3). When isolated from
//     each other, none can gather enough votes.
//   - n4 knows about [n0, n1, n2, n3, n4] and can reach all non-leader nodes,
//     collecting enough votes to win despite not being in their peer lists.
func TestUnknownPeerCanWinElection(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 5, Seed: 42})

	// Elect a leader normally with all 5 nodes participating.
	// This ensures all nodes are at a consistent term.
	err := s.RunUntilLeader()
	require.NoError(t, err)
	assert.Equal(t, 1, s.LeaderCount())

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Simulate DNS update: n0-n3 re-resolve and no longer see n4.
	// n4's DNS still includes all 5 nodes.
	peersWithoutN4 := []string{"n0", "n1", "n2", "n3"}
	for _, id := range peersWithoutN4 {
		s.SetPeers(id, peersWithoutN4)
	}

	// Let the cluster stabilize — the leader heartbeats n0-n3 only,
	// n4 receives no heartbeats but its term is current.
	s.RunFor(10 * time.Second)

	// Identify the non-leader followers (excluding n4)
	followers := []string{}
	for _, id := range peersWithoutN4 {
		if id != leader {
			followers = append(followers, id)
		}
	}

	// Partition the leader away from everyone
	s.Partition([]string{leader}, []string{"n0", "n1", "n2", "n3", "n4"})

	// Isolate the remaining followers from each other so they can't form
	// quorum among themselves (each needs 3 votes from [n0,n1,n2,n3] but
	// can only reach itself).
	for i := 0; i < len(followers); i++ {
		for j := i + 1; j < len(followers); j++ {
			s.Partition([]string{followers[i]}, []string{followers[j]})
		}
	}

	// n4 is NOT partitioned from any follower — it can reach all of them.
	// When n4 starts an election, followers will vote for it despite n4
	// not being in their currentPeers — handleVote has no peer whitelist.
	s.RunFor(60 * time.Second)

	assert.False(t, s.Node(leader).IsLeader())
	assert.Equal(t, 1, s.LeaderCount())
	assert.Equal(t, "n4", s.Leader())
}

// TestReceiveRPCUnknownType verifies that ReceiveRPC returns an error response
// (without blocking) when called with an RPC type that is not accepted from peers.
func TestReceiveRPCUnknownType(t *testing.T) {
	ctx := context.Background()

	n, err := election.NewNode(election.Config{
		UniqueID: "n0",
		Peers:    []string{"n0"},
		SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
			return election.RPCResponse{}, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, n.Start(ctx))
	defer func() { _ = n.Stop(ctx) }()

	// SetPeersRPC is not a peer-to-peer RPC and must be rejected inline
	resp, err := n.ReceiveRPC(ctx, election.RPCRequest{RPC: election.SetPeersRPC})
	require.NoError(t, err)
	assert.Equal(t, "unknown RPC", resp.Error)

	// ResignRPC is also not a peer-to-peer RPC and must be rejected inline
	resp, err = n.ReceiveRPC(ctx, election.RPCRequest{RPC: election.ResignRPC})
	require.NoError(t, err)
	assert.Equal(t, "unknown RPC", resp.Error)
}

// TestStopWithContextTimeout verifies that Stop returns context.Canceled when
// the context is already cancelled, without panicking (no send-to-closed-channel).
func TestStopWithContextTimeout(t *testing.T) {
	startCtx := context.Background()

	n, err := election.NewNode(election.Config{
		UniqueID: "n0",
		Peers:    []string{"n0"},
		SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
			return election.RPCResponse{}, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, n.Start(startCtx))

	// Stop with an already-cancelled context; must return context.Canceled
	cancelledCtx, cancel := context.WithCancel(startCtx)
	cancel()

	err = n.Stop(cancelledCtx)
	assert.ErrorIs(t, err, context.Canceled)

	// A clean Stop on the same node after forced shutdown must not panic.
	// The node may already be partially shut down; use a fresh background context.
	cleanCtx, cleanCancel := context.WithTimeout(startCtx, 2*time.Second)
	defer cleanCancel()
	_ = n.Stop(cleanCtx)
}

// TestSetMetadataSizeValidation verifies that metadata size limits are enforced
// on SetMetadata, Config.Metadata at Start(), and that pre-start SetMetadata works.
func TestSetMetadataSizeValidation(t *testing.T) {
	ctx := context.Background()

	// Table-driven size boundary tests
	for _, test := range []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "zero-bytes", size: 0, wantErr: false},
		{name: "exactly-1024-bytes", size: 1024, wantErr: false},
		{name: "1025-bytes", size: 1025, wantErr: true},
		{name: "large-blob", size: 2048, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, err := election.NewNode(election.Config{
				UniqueID: "n0",
				Peers:    []string{"n0"},
				SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
					return election.RPCResponse{}, nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, n.Start(ctx))
			defer func() { _ = n.Stop(ctx) }()

			blob := bytes.Repeat([]byte("x"), test.size)
			err = n.SetMetadata(ctx, blob)
			if test.wantErr {
				require.ErrorIs(t, err, election.ErrMetadataTooLarge)
			} else {
				require.NoError(t, err)
			}
		})
	}

	// Config.Metadata > 1024 bytes causes Start() to return ErrMetadataTooLarge
	t.Run("config-metadata-too-large", func(t *testing.T) {
		n, err := election.NewNode(election.Config{
			UniqueID: "n0",
			Peers:    []string{"n0"},
			Metadata: bytes.Repeat([]byte("x"), 1025),
			SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
				return election.RPCResponse{}, nil
			},
		})
		require.NoError(t, err)
		err = n.Start(ctx)
		require.ErrorIs(t, err, election.ErrMetadataTooLarge)
	})

	// SetMetadata before Start() sets metadata directly; self entry in GetState().Peers has it
	t.Run("set-metadata-before-start", func(t *testing.T) {
		const meta = "pre-start-metadata"
		n, err := election.NewNode(election.Config{
			UniqueID: "n0",
			Peers:    []string{"n0"},
			SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
				return election.RPCResponse{}, nil
			},
		})
		require.NoError(t, err)

		require.NoError(t, n.SetMetadata(ctx, []byte(meta)))

		// Self entry in GetState().Peers must have the metadata populated
		state := n.GetState()
		var found bool
		for _, p := range state.Peers {
			if p.Address == "n0" {
				assert.Equal(t, []byte(meta), p.Metadata)
				found = true
			}
		}
		assert.True(t, found)
	})
}

// TestSetPeersPreStartPreservesMetadata verifies that calling SetMetadata then
// SetPeers before Start preserves the node's own metadata in the peer list.
func TestSetPeersPreStartPreservesMetadata(t *testing.T) {
	n, err := election.NewNode(election.Config{
		UniqueID: "n0",
		Peers:    []string{"n0"},
		SendRPC: func(_ context.Context, _ string, _ election.RPCRequest) (election.RPCResponse, error) {
			return election.RPCResponse{}, nil
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, n.SetMetadata(ctx, []byte("my-metadata")))

	// SetPeers with a new list should preserve the self-entry's metadata
	require.NoError(t, n.SetPeers(ctx, []string{"n0", "n1", "n2"}))

	state := n.GetState()
	require.Len(t, state.Peers, 3)
	for _, p := range state.Peers {
		if p.Address == "n0" {
			assert.Equal(t, []byte("my-metadata"), p.Metadata)
			return
		}
	}
	t.Fatal("self entry not found in peers")
}

// TestLeaderSeesAllPeerMetadata verifies that after a heartbeat cycle the leader's
// GetState().Peers contains metadata for every peer in the cluster.
func TestLeaderSeesAllPeerMetadata(t *testing.T) {
	s := sim.New(sim.Config{
		NumNodes: 3,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": {Metadata: []byte("meta-n0")},
			"n1": {Metadata: []byte("meta-n1")},
			"n2": {Metadata: []byte("meta-n2")},
		},
	})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Allow heartbeat cycles to propagate follower metadata back to the leader
	s.RunFor(30 * time.Second)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	state := s.Node(leader).GetState()
	metadataByAddress := make(map[string][]byte, len(state.Peers))
	for _, p := range state.Peers {
		metadataByAddress[p.Address] = p.Metadata
	}

	assert.Equal(t, []byte("meta-n0"), metadataByAddress["n0"])
	assert.Equal(t, []byte("meta-n1"), metadataByAddress["n1"])
	assert.Equal(t, []byte("meta-n2"), metadataByAddress["n2"])
}

// TestFollowerSeesLeaderMetadata verifies that a follower sees the leader's metadata
// and its own metadata, but not other followers' metadata.
func TestFollowerSeesLeaderMetadata(t *testing.T) {
	s := sim.New(sim.Config{
		NumNodes: 3,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": {Metadata: []byte("meta-n0")},
			"n1": {Metadata: []byte("meta-n1")},
			"n2": {Metadata: []byte("meta-n2")},
		},
	})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Allow heartbeat cycles to propagate metadata
	s.RunFor(30 * time.Second)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Pick a follower
	var follower string
	for _, id := range []string{"n0", "n1", "n2"} {
		if id != leader {
			follower = id
			break
		}
	}
	require.NotEmpty(t, follower)

	// Find the third node (neither leader nor the chosen follower)
	var otherFollower string
	for _, id := range []string{"n0", "n1", "n2"} {
		if id != leader && id != follower {
			otherFollower = id
			break
		}
	}

	state := s.Node(follower).GetState()
	metadataByAddress := make(map[string][]byte, len(state.Peers))
	for _, p := range state.Peers {
		metadataByAddress[p.Address] = p.Metadata
	}

	// Follower sees leader's metadata
	assert.Equal(t, []byte("meta-"+leader), metadataByAddress[leader])
	// Follower sees its own metadata
	assert.Equal(t, []byte("meta-"+follower), metadataByAddress[follower])
	// Follower does NOT see the other follower's metadata (nil/empty)
	assert.Empty(t, metadataByAddress[otherFollower])
}

// TestOnChangeFiresOnMetadataChange verifies that the OnChange callback fires when
// a follower's metadata changes (leader receives it) but not on every heartbeat.
func TestOnChangeFiresOnMetadataChange(t *testing.T) {
	var leaderChanges, n1Changes, n2Changes atomic.Int32

	makeCounter := func(c *atomic.Int32) func(election.NodeState) {
		return func(_ election.NodeState) {
			c.Add(1)
		}
	}

	s := sim.New(sim.Config{
		NumNodes: 3,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": {OnChange: makeCounter(&leaderChanges)},
			"n1": {OnChange: makeCounter(&n1Changes)},
			"n2": {OnChange: makeCounter(&n2Changes)},
		},
	})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Allow heartbeat cycles to stabilize so all followers know their leader
	s.RunFor(30 * time.Second)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Pick a follower
	var follower string
	for _, id := range []string{"n0", "n1", "n2"} {
		if id != leader {
			follower = id
			break
		}
	}

	// Map node IDs to their counters
	counters := map[string]*atomic.Int32{
		"n0": &leaderChanges,
		"n1": &n1Changes,
		"n2": &n2Changes,
	}
	leaderCounter := counters[leader]
	followerCounter := counters[follower]

	// Record baseline counts after the cluster has fully stabilized
	leaderBaseline := leaderCounter.Load()
	followerBaseline := followerCounter.Load()

	// Set new metadata on the follower
	s.SetMetadata(follower, []byte("new-follower-meta"))

	// Run for enough time for heartbeat to carry the new metadata back to the leader
	s.RunFor(30 * time.Second)

	leaderAfterMetadata := leaderCounter.Load()
	followerAfterMetadata := followerCounter.Load()

	// Leader's OnChange must have fired at least once (it received updated peer metadata)
	assert.Greater(t, leaderAfterMetadata, leaderBaseline)

	// Follower's OnChange must NOT have fired due to its own metadata change
	// (the follower already knows its own metadata; the leader does not send it back)
	assert.Equal(t, followerBaseline, followerAfterMetadata)

	// Run more virtual time with no metadata changes — counters must be stable
	leaderStable := leaderCounter.Load()
	s.RunFor(60 * time.Second)
	assert.Equal(t, leaderStable, leaderCounter.Load())
}

// TestOnChangeLeaderTransitionIncludesMetadata verifies that the NodeState delivered
// to OnChange during a leader transition includes populated peer metadata.
func TestOnChangeLeaderTransitionIncludesMetadata(t *testing.T) {
	var mu sync.Mutex
	// statesOnBecoming maps nodeID → last NodeState received when that node became leader
	statesOnBecoming := make(map[string]election.NodeState)

	makeCallback := func(nodeID string) func(election.NodeState) {
		return func(state election.NodeState) {
			if state.IsLeader {
				mu.Lock()
				statesOnBecoming[nodeID] = state
				mu.Unlock()
			}
		}
	}

	s := sim.New(sim.Config{
		NumNodes: 3,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": {Metadata: []byte("meta-n0"), OnChange: makeCallback("n0")},
			"n1": {Metadata: []byte("meta-n1"), OnChange: makeCallback("n1")},
			"n2": {Metadata: []byte("meta-n2"), OnChange: makeCallback("n2")},
		},
	})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Allow heartbeat cycles to propagate metadata before resign
	s.RunFor(30 * time.Second)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	err = s.Resign(leader)
	require.NoError(t, err)

	err = s.RunUntilLeader()
	require.NoError(t, err)

	// Allow the new leader to receive peer metadata via heartbeats
	s.RunFor(30 * time.Second)

	newLeader := s.Leader()
	require.NotEmpty(t, newLeader)
	require.NotEqual(t, leader, newLeader)

	// The new leader must have received an OnChange callback when it became leader
	mu.Lock()
	newLeaderState, ok := statesOnBecoming[newLeader]
	mu.Unlock()
	require.True(t, ok)

	// The NodeState at the time of becoming leader should identify itself as leader
	assert.True(t, newLeaderState.IsLeader)
	assert.Equal(t, newLeader, newLeaderState.Leader)
	assert.NotZero(t, newLeaderState.Term)
	// Peers list must be populated with the new leader's own metadata
	assert.NotEmpty(t, newLeaderState.Peers)
	for _, p := range newLeaderState.Peers {
		if p.Address == newLeader {
			assert.Equal(t, []byte("meta-"+newLeader), p.Metadata)
		}
	}
}

// TestSetMetadataMidRun verifies that metadata set after the cluster is running
// propagates correctly via heartbeat cycles.
func TestSetMetadataMidRun(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 3, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	// Pick a follower
	var follower string
	for _, id := range []string{"n0", "n1", "n2"} {
		if id != leader {
			follower = id
			break
		}
	}

	// Set metadata on both leader and one follower mid-run
	s.SetMetadata(leader, []byte("leader-meta"))
	s.SetMetadata(follower, []byte("follower-meta"))

	// Allow heartbeat cycles to propagate
	s.RunFor(30 * time.Second)

	// Leader should see both its own metadata and the follower's metadata
	leaderState := s.Node(leader).GetState()
	leaderMeta := make(map[string][]byte, len(leaderState.Peers))
	for _, p := range leaderState.Peers {
		leaderMeta[p.Address] = p.Metadata
	}
	assert.Equal(t, []byte("leader-meta"), leaderMeta[leader])
	assert.Equal(t, []byte("follower-meta"), leaderMeta[follower])

	// Follower should see the leader's metadata
	followerState := s.Node(follower).GetState()
	followerMeta := make(map[string][]byte, len(followerState.Peers))
	for _, p := range followerState.Peers {
		followerMeta[p.Address] = p.Metadata
	}
	assert.Equal(t, []byte("leader-meta"), followerMeta[leader])
}

// TestMetadataSurvivesLeaderTransition verifies that metadata set before a leadership
// transition is still visible after a new leader is elected and heartbeats propagate.
func TestMetadataSurvivesLeaderTransition(t *testing.T) {
	s := sim.New(sim.Config{
		NumNodes: 3,
		Seed:     42,
		NodeConfig: map[string]sim.NodeSimConfig{
			"n0": {Metadata: []byte("meta-n0")},
			"n1": {Metadata: []byte("meta-n1")},
			"n2": {Metadata: []byte("meta-n2")},
		},
	})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	// Let metadata propagate to the leader
	s.RunFor(30 * time.Second)

	leader := s.Leader()
	require.NotEmpty(t, leader)

	err = s.Resign(leader)
	require.NoError(t, err)

	err = s.RunUntilLeader()
	require.NoError(t, err)

	// Let the new leader propagate metadata via heartbeats
	s.RunFor(60 * time.Second)

	newLeader := s.Leader()
	require.NotEmpty(t, newLeader)
	require.NotEqual(t, leader, newLeader)

	// New leader should have received metadata from remaining peers via heartbeat responses
	state := s.Node(newLeader).GetState()
	metadataByAddress := make(map[string][]byte, len(state.Peers))
	for _, p := range state.Peers {
		metadataByAddress[p.Address] = p.Metadata
	}

	// The new leader's own metadata must be present
	assert.Equal(t, []byte("meta-"+newLeader), metadataByAddress[newLeader])

	// All peers' metadata must have propagated after 60s of heartbeats
	for _, id := range []string{"n0", "n1", "n2"} {
		assert.Equal(t, []byte("meta-"+id), metadataByAddress[id])
	}
}

// TestSimRejectsOversizedMetadata verifies that creating a sim node with metadata
// exceeding 1KB panics, matching the production Start() behavior.
func TestSimRejectsOversizedMetadata(t *testing.T) {
	require.Panics(t, func() {
		sim.New(sim.Config{
			NumNodes: 3,
			Seed:     42,
			NodeConfig: map[string]sim.NodeSimConfig{
				"n0": {Metadata: bytes.Repeat([]byte("x"), 1025)},
			},
		})
	})
}
