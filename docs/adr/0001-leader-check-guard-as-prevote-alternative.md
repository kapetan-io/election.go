# 1. Leader-Check Guard as PreVote Alternative for Partitioned Node Rejoin

Date: 2026-05-17

## Status

Accepted

## Context

In Raft, a node that becomes partitioned from the cluster will repeatedly increment its term while trying to elect itself. When it rejoins, its inflated term can force the entire cluster into a new election — the "term bomb" problem. The standard Raft solution is the [PreVote protocol](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf), where a candidate must first confirm it can win before incrementing its term. PreVote adds a round of RPCs before every election.

This library needs to prevent partitioned-node disruption without the complexity of an additional RPC round.

## Decision

We will not implement PreVote. Instead, we prevent partitioned-node disruption through four complementary mechanisms:

- **Leader-check guard in `handleVote`**: Followers with a known current leader reject vote requests from other candidates, preventing a partitioned node with an inflated term from winning.
- **Leader quorum self-demotion**: A leader that loses contact with a quorum of peers steps down and sends `ResetElectionRPC` to all reachable peers, freeing them to elect a new leader. The leader detects its own partition rather than waiting for a returning node to force the issue.
- **Follower-first initial state**: Every node starts in `followerState` and waits for a heartbeat timeout before transitioning to `candidateState`.
- **`MinimumQuorum` config**: If a node's peer list shrinks below `MinimumQuorum`, it refuses to run a candidate election, preventing an isolated minority from electing a leader.

## Consequences

- No additional RPC round per election — simpler and lower latency than PreVote.
- A candidate can only win votes from nodes that have lost their leader (via heartbeat timeout or explicit `ResetElection`).
- If the leader crashes without sending `ResetElection`, followers eventually time out their heartbeat and clear their leader reference, becoming available to vote. Election availability depends on the heartbeat timeout, not on an explicit failure-detection protocol.
- Nodes rejoin as fresh members via `SetPeers`, so there is no risk of term/vote state persisting across restarts.
