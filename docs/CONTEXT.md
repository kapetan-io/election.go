# Election Library Domain

Domain glossary for the `election` library — a transport-agnostic RAFT leader election implementation in Go.

## Language

### Roles

**Node**:
A single participant in a leader election cluster, identified by a unique string (typically `ip:port`).
_Avoid_: member, instance, server

**Leader**:
The node that has won the current election term and sends heartbeats to all peers.
_Avoid_: master, primary

**Follower**:
A node that recognizes a current leader and responds to heartbeats and vote requests.
_Avoid_: replica, secondary

**Candidate**:
A node that has started an election by requesting votes from its peers. A node transitions from follower to candidate when the heartbeat timeout expires.
_Avoid_: proposer, nominee

**Peer**:
Any other node in the cluster known to this node. Peers are opaque strings; the library never interprets them.
_Avoid_: neighbor, endpoint

### Election Mechanics

**Term**:
A monotonically increasing integer that identifies an election cycle. Each new election increments the term. A node with a higher term supersedes one with a lower term.
_Avoid_: epoch, round, generation

**Quorum**:
The minimum number of votes required to win an election — strictly more than half the cluster size.
_Avoid_: majority (acceptable in prose, but quorum is the precise concept)

**MinimumQuorum**:
A configuration threshold — the minimum number of peers required before a node will attempt an election. Prevents isolated minorities from electing a leader.

**Heartbeat**:
A periodic RPC sent by the leader to all followers to assert liveness and maintain leadership.
_Avoid_: ping, keepalive, health check

**HeartbeatTimeout**:
The duration a follower waits without receiving a heartbeat before transitioning to candidate and starting a new election.

**LeaderQuorumTimeout**:
The duration a leader waits for heartbeat responses from a quorum of followers before stepping down. This is how the leader detects its own partition.

**Leader-Check Guard**:
A rule in vote handling: a follower with a known current leader rejects vote requests from other candidates. Prevents partitioned nodes with inflated terms from disrupting the cluster on rejoin. This library's alternative to the PreVote protocol.

**ResetElection**:
An RPC sent by a stepping-down leader to its reachable peers, clearing their leader reference so they become available to vote in the next election.

**Resign**:
A voluntary leadership step-down initiated by the current leader. Triggers a new election that the resigning node does not participate in.

### Architecture (post-refactor)

**Coroutine**:
A cooperatively scheduled unit of execution managed by `gocoro`. Replaces Go goroutines and channels inside the node, making execution order deterministic.

**IOReq / IOResp**:
Typed values yielded by the coroutine to request IO from the scheduler — `SendRPC`, `Recv`, `After`, `Cancel`, `Now`. The scheduler fulfills these with real or simulated IO.

**DST (Deterministic Simulation Testing)**:
A testing approach where the same state-machine code runs against simulated IO (virtual clock, in-memory RPC routing, seeded RNG) so that test execution is fully reproducible.

**Virtual Clock**:
A simulated clock used in DST that advances only when explicitly stepped, replacing wall-clock time.

### Transport

**SendRPC**:
A function provided by the consumer at configuration time. The library calls it to send an outbound RPC to a peer. Signature: `func(ctx, peer, request) (response, error)`.
_Avoid_: transport, client, send

**ReceiveRPC**:
A method on the node called by the consumer when an inbound RPC arrives over whatever transport they use.
_Avoid_: handler, dispatch, accept

## Relationships

- A **Node** is always in exactly one role: **Follower**, **Candidate**, or **Leader**
- A **Leader** sends **Heartbeats** to all **Peers** on a regular interval
- A **Follower** starts a new election (becomes **Candidate**) when **HeartbeatTimeout** expires without a **Heartbeat**
- A **Candidate** requests votes from all **Peers**; receiving a **Quorum** of votes makes it **Leader**
- A **Leader** that loses contact with a **Quorum** of **Peers** (via **LeaderQuorumTimeout**) steps down and sends **ResetElection** to reachable peers
- A **Follower** with a known **Leader** rejects vote requests from **Candidates** (**Leader-Check Guard**)
- The library never calls **SendRPC** directly — it yields an **IOReq** to the **Coroutine** scheduler, which dispatches through the consumer-provided **SendRPC** function

## Example dialogue

> **Dev:** "A **Node** got partitioned and its **Term** is way ahead of the cluster. What happens when it rejoins?"
> **Domain expert:** "It becomes a **Candidate** and sends vote requests, but every **Follower** still has a known **Leader**, so the **Leader-Check Guard** rejects the votes. The cluster is undisturbed."

> **Dev:** "What if the actual **Leader** crashes?"
> **Domain expert:** "**Followers** stop receiving **Heartbeats**. After the **HeartbeatTimeout**, they clear their **Leader** reference and transition to **Candidate**. Now the **Leader-Check Guard** no longer blocks votes, and a new election proceeds."

> **Dev:** "How does the **Leader** know it's been partitioned?"
> **Domain expert:** "It tracks **Heartbeat** responses. If fewer than a **Quorum** of **Peers** respond within the **LeaderQuorumTimeout**, it steps down and sends **ResetElection** to the peers it can still reach."

## Flagged ambiguities

- "state" is used for both the node's role (`followerState`, `candidateState`, `leaderState`) and the full snapshot returned by `GetState()` (`NodeState`). Resolved: **role** refers to the follower/candidate/leader enum; **node state** refers to the full snapshot (leader, isLeader, term, peers, current role).
- "send" / "receive" — in the current codebase `send()` is an internal method that pushes onto `rpcCh`, not the consumer-provided `SendRPC`. Post-refactor this ambiguity disappears: **SendRPC** is always the consumer-provided function; inbound delivery is always **ReceiveRPC**.
