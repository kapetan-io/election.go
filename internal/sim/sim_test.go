package sim_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrawn01/election/internal/sim"
)

// TestSimulationBasicLeaderElection verifies that a 3-node simulation converges
// to exactly one leader using a deterministic seed.
func TestSimulationBasicLeaderElection(t *testing.T) {
	s := sim.New(sim.Config{NumNodes: 3, Seed: 42})

	err := s.RunUntilLeader()
	require.NoError(t, err)

	assert.Equal(t, 1, s.LeaderCount())
	require.NotEmpty(t, s.Leader())
}

// TestSimulationDeterminism verifies that the same seed always produces the
// same leader, confirming deterministic event ordering.
func TestSimulationDeterminism(t *testing.T) {
	s1 := sim.New(sim.Config{NumNodes: 3, Seed: 42})
	err := s1.RunUntilLeader()
	require.NoError(t, err)
	leader1 := s1.Leader()
	require.NotEmpty(t, leader1)

	s2 := sim.New(sim.Config{NumNodes: 3, Seed: 42})
	err = s2.RunUntilLeader()
	require.NoError(t, err)
	leader2 := s2.Leader()
	require.NotEmpty(t, leader2)

	assert.Equal(t, leader1, leader2)
}
