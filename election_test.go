package election_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrawn01/election"
)

var cfg = &election.Config{
	NetworkTimeout:      time.Second,
	HeartBeatTimeout:    time.Second,
	LeaderQuorumTimeout: time.Second * 2,
	ElectionTimeout:     time.Second * 2,
}

func createCluster(t *testing.T, c *TestCluster) {
	t.Helper()

	// Start with a known leader
	err := c.SpawnNode("n0", cfg)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n0"] == "n0"
	}, 10*time.Second, 100*time.Millisecond)

	// Added nodes should become followers
	err = c.SpawnNode("n1", cfg)
	require.NoError(t, err)
	err = c.SpawnNode("n2", cfg)
	require.NoError(t, err)
	err = c.SpawnNode("n3", cfg)
	require.NoError(t, err)
	err = c.SpawnNode("n4", cfg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n0"] == "n0" &&
			status["n1"] == "n0" &&
			status["n2"] == "n0" &&
			status["n3"] == "n0" &&
			status["n4"] == "n0"
	}, 10*time.Second, 100*time.Millisecond)
}

func TestSingleNodeLeader(t *testing.T) {
	c := NewTestCluster(t)
	err := c.SpawnNode("n0", cfg)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n0"] == "n0"
	}, 10*time.Second, 100*time.Millisecond)

	// Consume first leader election event
	event := <-c.OnChangeCh
	assert.Equal(t, "n0", event.Leader)
	assert.Equal(t, "n0", event.From)

	assert.True(t, c.Nodes["n0"].Node.IsLeader())

	select {
	// Should NOT receive a leadership change as we are the only node
	case <-c.OnChangeCh:
		t.Log("received un-expected leader change")
		t.FailNow()
	case <-time.After(cfg.HeartBeatTimeout * 3):
	}
}

func TestSimpleElection(t *testing.T) {
	c := NewTestCluster(t)
	createCluster(t, c)
	defer c.Close()

	err := c.Nodes["n0"].Node.Resign(context.Background())
	require.NoError(t, err)

	// Wait until n0 is no longer leader
	require.Eventually(t, func() bool {
		candidate := c.GetLeader()
		return candidate != nil && candidate.GetLeader() != "n0"
	}, 30*time.Second, 100*time.Millisecond)
}

func TestLeaderDisconnect(t *testing.T) {
	c := NewTestCluster(t)
	createCluster(t, c)
	defer c.Close()

	errConnRefused := errConnRefused()
	c.AddNetworkError("n0", errConnRefused)
	defer c.DelNetworkError("n0")

	// Should lose leadership
	require.Eventually(t, func() bool {
		node := c.Nodes["n0"]
		return node != nil && node.Node.GetLeader() != "n0"
	}, 30*time.Second, 100*time.Millisecond)
}

func TestFollowerDisconnect(t *testing.T) {
	c := NewTestCluster(t)
	createCluster(t, c)
	defer c.Close()

	errConnRefused := errConnRefused()
	c.AddNetworkError("n4", errConnRefused)
	defer c.DelNetworkError("n4")

	// Wait until n4 loses leader
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n4"] != "n0"
	}, 5*time.Second, 100*time.Millisecond)

	c.DelNetworkError("n4")

	// Follower should resume being a follower without forcing a new election.
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n4"] == "n0"
	}, 60*time.Second, 100*time.Millisecond)
}

func TestSplitBrain(t *testing.T) {
	c1 := NewTestCluster(t)
	createCluster(t, c1)
	defer c1.Close()

	c2 := NewTestCluster(t)

	c2.Add("n0", c1.Remove("n0"))
	c2.Add("n1", c1.Remove("n1"))

	// Cluster 1 should elect a new leader
	require.Eventually(t, func() bool {
		return c1.GetLeader() != nil
	}, 30*time.Second, 100*time.Millisecond)

	// Cluster 2 should elect a new leader
	require.Eventually(t, func() bool {
		return c2.GetLeader() != nil
	}, 30*time.Second, 100*time.Millisecond)

	// Move the nodes in cluster2 back to cluster1
	c1.Add("n0", c2.Remove("n0"))
	c1.Add("n1", c2.Remove("n1"))

	// The nodes should detect 2 leaders and start a new vote.
	require.Eventually(t, func() bool {
		status := c1.GetClusterStatus()
		leaders := make(map[string]struct{})
		for _, v := range status {
			if v != "" {
				leaders[v] = struct{}{}
			}
		}
		return len(leaders) == 1
	}, 10*time.Second, 100*time.Millisecond)
}

func TestOmissionFaults(t *testing.T) {
	c1 := NewTestCluster(t)
	createCluster(t, c1)
	defer c1.Close()

	errConnRefused := errConnRefused()
	c1.Disconnect("n3", "n4", errConnRefused)
	c1.Disconnect("n4", "n3", errConnRefused)
	c1.Disconnect("n0", "n4", errConnRefused)
	c1.Disconnect("n4", "n0", errConnRefused)
	c1.Disconnect("n0", "n3", errConnRefused)
	c1.Disconnect("n3", "n0", errConnRefused)
	c1.Disconnect("n2", "n4", errConnRefused)
	c1.Disconnect("n4", "n2", errConnRefused)
	c1.Disconnect("n1", "n3", errConnRefused)
	c1.Disconnect("n3", "n1", errConnRefused)

	// Cluster should retain n0 as leader in the face of an unstable cluster
	for i := 0; i < 12; i++ {
		leader := c1.GetLeader()
		require.NotNil(t, leader)
		require.Equal(t, leader.GetLeader(), "n0")
		time.Sleep(time.Millisecond * 400)
	}

	// Should retain leader once communication is restored
	c1.ClearErrors()

	for i := 0; i < 12; i++ {
		leader := c1.GetLeader()
		require.NotNil(t, leader)
		require.Equal(t, leader.GetLeader(), "n0")
		time.Sleep(time.Millisecond * 400)
	}
}

func TestIsolatedLeader(t *testing.T) {
	c1 := NewTestCluster(t)
	createCluster(t, c1)
	defer c1.Close()

	require.Equal(t, c1.GetLeader().GetLeader(), "n0")

	errConnRefused := errConnRefused()
	c1.Disconnect("n0", "n2", errConnRefused)
	c1.Disconnect("n2", "n0", errConnRefused)
	c1.Disconnect("n0", "n3", errConnRefused)
	c1.Disconnect("n3", "n0", errConnRefused)
	c1.Disconnect("n0", "n4", errConnRefused)
	c1.Disconnect("n4", "n0", errConnRefused)

	// Leader should realize it doesn't have a quorum of
	// heartbeats and step down; remaining cluster should elect a new leader
	require.Eventually(t, func() bool {
		leader := c1.GetLeader()
		if leader == nil {
			return false
		}
		return leader.GetLeader() != "n0"
	}, 20*time.Second, 500*time.Millisecond)

	require.NotEqual(t, c1.GetLeader().GetLeader(), "n0")

	// Should persist new leader once communication is restored
	c1.ClearErrors()

	require.Eventually(t, func() bool {
		return c1.Nodes["n0"].Node.GetLeader() != ""
	}, 10*time.Second, 100*time.Millisecond)

	s := c1.Nodes["n0"].Node.GetState()
	assert.Equal(t, election.Follower, s.Role)
}

func TestMinimumQuorum(t *testing.T) {
	c := NewTestCluster(t)

	minCfg := &election.Config{
		NetworkTimeout:      time.Second,
		HeartBeatTimeout:    time.Second,
		LeaderQuorumTimeout: time.Second * 2,
		ElectionTimeout:     time.Second * 2,
		MinimumQuorum:       2,
	}

	err := c.SpawnNode("n0", minCfg)
	require.NoError(t, err)

	time.Sleep(time.Second * 5)

	// Ensure n0 is not leader
	status := c.GetClusterStatus()
	require.NotEqual(t, "n0", status["n0"])

	err = c.SpawnNode("n1", minCfg)
	require.NoError(t, err)

	// Should elect a leader
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n0"] != ""
	}, 10*time.Second, 100*time.Millisecond)

	status = c.GetClusterStatus()
	var leader string

	// Shutdown the follower
	if status["n0"] == "n0" {
		err = c.Remove("n1").Node.Stop(context.Background())
		require.NoError(t, err)
		leader = "n0"
	} else {
		err = c.Remove("n0").Node.Stop(context.Background())
		require.NoError(t, err)
		leader = "n1"
	}

	// The leader should detect it no longer has MinimumQuorum and step down
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status[leader] == ""
	}, 10*time.Second, 100*time.Millisecond)
}

func TestResign(t *testing.T) {
	c1 := NewTestCluster(t)
	createCluster(t, c1)
	defer c1.Close()

	require.Eventually(t, func() bool {
		return c1.GetLeader() != nil
	}, 30*time.Second, 100*time.Millisecond)

	leader := c1.GetLeader()

	// Calling resign on a follower should return ErrNotLeader
	err := c1.Nodes["n1"].Node.Resign(context.Background())
	assert.ErrorContains(t, err, "not the leader")

	for i := 0; i < 10; i++ {
		if c1.GetLeader() != leader {
			require.FailNow(t, "leader should not have changed")
		}
		time.Sleep(time.Millisecond * 500)
	}

	// Calling resign on the leader should give up leadership
	err = c1.Nodes["n0"].Node.Resign(context.Background())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return c1.GetLeader() != leader
	}, 30*time.Second, 100*time.Millisecond)
}

func TestResignSingleNode(t *testing.T) {
	c := NewTestCluster(t)
	err := c.SpawnNode("n0", cfg)
	require.NoError(t, err)
	defer c.Close()

	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n0"] == "n0"
	}, 10*time.Second, 100*time.Millisecond)

	err = c.Nodes["n0"].Node.Resign(context.Background())
	require.NoError(t, err)

	// n0 will eventually become leader again
	require.Eventually(t, func() bool {
		status := c.GetClusterStatus()
		return status["n0"] == "n0"
	}, 10*time.Second, 100*time.Millisecond)
}

// errConnRefused returns a sentinel error for simulating connection refused
func errConnRefused() error {
	return fmt.Errorf("connection refused")
}
