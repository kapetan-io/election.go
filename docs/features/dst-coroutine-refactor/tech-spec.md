# DST Coroutine Refactor Tech Spec

_PRD: docs/features/dst-coroutine-refactor/prd.md_

## Overview

Replace the channel/goroutine/select internals of the election library with a coroutine-based architecture using `github.com/resonatehq/gocoro`. The coroutine state machine is identical in production and simulation — only the IO adapter differs. Production IO uses real network calls, wall-clock time, and Go channels. Simulation IO uses a virtual clock, in-memory RPC routing, and deterministic stepping.

The same rewrite cleans up the public API (context support, explicit errors, no pointer out-params), replaces proto-generated types with plain Go structs, migrates logging from logrus to `log/slog`, and removes all `holster/v4` dependencies.

## Component Design

### Package Structure

| Package | Role |
|---|---|
| `election` | Public API: `Node` interface, `Config`, RPC types, `NewNode` |
| `internal/engine` | Coroutine IO vocabulary: `Req`, `Resp`, `Kind`, `Event`, `EventKind` |
| `internal/prod` | Production IO adapter and main loop |
| `internal/sim` | Simulation orchestrator, simulation IO adapter, virtual clock, timer heap, fault injection |

### gocoro Integration Model

gocoro provides a cooperative coroutine scheduler parameterized as `Scheduler[I, O]`. It does not run its own event loop — the caller drives it by repeatedly calling `RunUntilBlocked(time)`. When a coroutine yields a value, the scheduler calls `io.Dispatch(value, callback)` on a caller-provided `io.IO[I, O]` implementation. The callback resolves the coroutine's promise and allows the scheduler to resume it.

The election library provides two `io.IO[engine.Req, engine.Resp]` implementations:

- **Production IO** (`internal/prod`): `SendRPC` dispatches via goroutine to the consumer-provided `SendRPCFunc`. `Recv` reads from a Go channel fed by `ReceiveRPC`, timer fires, `SetPeers`, and `Resign`. `After` uses `time.AfterFunc`. `Cancel` and `Now` resolve synchronously.
- **Simulation IO** (`internal/sim`): `SendRPC` routes to the orchestrator's `queueRPC`, which delivers to the target node's event queue. `Recv` reads from an in-memory slice. `After` pushes onto a shared timer heap. `Cancel` and `Now` resolve synchronously using the virtual clock.

### IO Vocabulary (`internal/engine`)

All coroutine-to-scheduler communication uses tagged-struct sum types. Living in `internal/engine` avoids naming collisions with the public `RPCRequest`, `RPCResponse`, `SendRPCFunc`, and `ReceiveRPC`.

```go
type Kind int

const (
    SendRPC Kind = iota + 1
    Recv
    After
    Cancel
    Now
)

type Req struct {
    Kind    Kind
    Peer    string           // SendRPC
    RPCReq  RPCRequest       // SendRPC
    Delay   time.Duration    // After
    TimerID int64            // Cancel
}

type Resp struct {
    Kind    Kind
    RPCResp RPCResponse      // SendRPC
    Err     error            // SendRPC
    Event   Event            // Recv
    TimerID int64            // After (assigned handle)
    Time    time.Time        // Now
}
```

### Event Types (`internal/engine`)

Events are the inbound messages delivered to the coroutine via `Recv`. All external input — inbound RPCs, timer fires, `SetPeers`, `Resign`, shutdown — arrives as an `Event`.

```go
type EventKind int

const (
    EventRPC EventKind = iota + 1
    EventTimer
    EventSetPeers
    EventResign
    EventShutdown
)

type Event struct {
    Kind    EventKind
    RPCReq  RPCRequest           // EventRPC
    TimerID int64                // EventTimer
    Peers   []string             // EventSetPeers
    Respond func(RPCResponse)    // EventRPC — response callback
    Done    func(error)          // EventSetPeers, EventResign — completion callback
}
```

`Respond` and `Done` are plain function calls invoked by the coroutine between yield points. The implementation is provided by the IO adapter — production writes to a channel, simulation stores the result in the orchestrator's state.

### Coroutine Type Parameters

The scheduler is `gocoro.Scheduler[engine.Req, engine.Resp]`. Coroutine functions share `T=engine.Req`, `TNext=engine.Resp`, with `TReturn` varying by role:

| Coroutine | `TReturn` |
|---|---|
| Main `run` loop | `struct{}` |
| Per-peer vote request | `VoteResp` |
| Per-peer heartbeat | `HeartBeatResp` |
| Per-peer election reset | `struct{}` |

`runFollower`, `runCandidate`, and `runLeader` are regular methods on `*node` that accept the `gocoro.Coroutine` handle — not separate coroutines. They share `*node` state directly and return control to the outer `run` loop.

### State Machine Structure

```go
func (n *node) run(c gocoro.Coroutine[engine.Req, engine.Resp, struct{}]) (struct{}, error) {
    for {
        switch n.role {
        case follower:
            n.runFollower(c)
        case candidate:
            n.runCandidate(c)
        case leader:
            n.runLeader(c)
        case shutdown:
            return struct{}{}, nil
        }
    }
}
```

Each `runX` method:
1. Registers timers via `YieldAndAwait(c, engine.Req{Kind: engine.After, ...})` — returns `TimerID` immediately
2. Enters a loop calling `YieldAndAwait(c, engine.Req{Kind: engine.Recv})` to receive the next event
3. Dispatches on `event.Kind` — processes RPCs, handles timer fires, responds to `SetPeers`/`Resign`
4. Cancels outstanding timers via `YieldAndAwait(c, engine.Req{Kind: engine.Cancel, ...})` before returning

`Recv` is the universal blocking point. The coroutine never blocks on anything else except `SendRPC` (in spawned children).

### Timer Model

Timers are non-blocking: `After` registers a timer and returns a `TimerID` immediately. The fire arrives later as an `EventTimer` through `Recv`.

- `TimerID` is `int64`, monotonically assigned by the IO adapter.
- Cancelling an already-fired or already-cancelled timer is a no-op.
- All five IO operations (`SendRPC`, `Recv`, `After`, `Cancel`, `Now`) go through `YieldAndAwait` for uniform scheduler mediation — even `Cancel` and `Now` which resolve instantly.

### Fan-Out Pattern

**Vote collection (sequential `Await`):** `electSelf` spawns one coroutine per peer via `gocoro.Spawn`, collects all promises, then awaits each sequentially. All `SendRPC` IO operations are dispatched concurrently by the IO adapter — sequential `Await` only affects when the main coroutine sees results, not when RPCs are in flight. On RPC failure, the spawned coroutine returns `VoteResp{Granted: false}` instead of silently dropping (fixes the existing `electSelf` double-send bug).

**Heartbeats (fire-and-forget `Spawn`):** `runLeader` spawns one coroutine per peer on each heartbeat timer fire. Spawned coroutines are not awaited — they run when the main coroutine next yields to `Recv`, send the RPC, update `peersLastContact` on the node struct directly (safe because coroutines are cooperative — only one runs at a time), and exit.

**Election resets (fire-and-forget `Spawn`):** Same pattern as heartbeats. Spawned, not awaited.

## Data Model

### Node State (atomic fields)

The coroutine updates these atomics whenever state changes. `IsLeader()`, `GetLeader()`, and `GetState()` read them without entering the coroutine.

| Field | Type | Updated when |
|---|---|---|
| `leader` | `atomic.Value` (string) | Leader changes (heartbeat received, election won, resign) |
| `isLeader` | `atomic.Bool` | Role transitions to/from leader |
| `term` | `atomic.Uint64` | Term increments (new election, higher-term RPC received) |
| `role` | `atomic.Int32` | Any role transition |
| `peers` | `atomic.Value` ([]string) | `SetPeers` processed |

### Node Internal State (coroutine-only, not atomic)

| Field | Purpose |
|---|---|
| `currentTerm uint64` | Authoritative term counter (atomics are copies for readers) |
| `vote.LastCandidate string` | Who this node voted for in the current term |
| `vote.LastTerm uint64` | Term in which this node last cast a vote; paired with `LastCandidate` for duplicate-vote detection |
| `peersLastContact map[string]time.Time` | Leader-only: last heartbeat response time per peer |
| `lastContact time.Time` | Follower-only: last heartbeat received |
| `rng *rand.Rand` | Per-node seeded RNG for `randomDuration` (`math/rand`, not `math/rand/v2` — Go 1.21 minimum) |

## API Design

### Node Interface

```go
type Node interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    SetPeers(ctx context.Context, peers []string) error
    Resign(ctx context.Context) error
    IsLeader() bool
    GetLeader() string
    GetState() NodeState
    ReceiveRPC(ctx context.Context, req RPCRequest) (RPCResponse, error)
    Stats() Stats
}
```

### NewNode

```go
func NewNode(conf Config) (Node, error)
```

Validates `UniqueID`, `SendRPC`, and timeout fields, applies defaults to zero-valued durations, and returns an initialized node. The node does not participate in elections until `Start` is called.

### Config

```go
type Config struct {
    UniqueID            string
    Peers               []string
    SendRPC             SendRPCFunc
    OnLeaderChange      func(leader string)
    HeartBeatTimeout    time.Duration
    ElectionTimeout     time.Duration
    NetworkTimeout      time.Duration
    LeaderQuorumTimeout time.Duration
    MinimumQuorum       int
    Log                 *slog.Logger
}
```

- `OnLeaderChange` replaces `OnUpdate` (rename, same semantics). Invoked synchronously from the coroutine event loop — must not block or re-enter the node.
- `Log` defaults to `slog.Default()` when nil.
- Zero-valued durations get defaults applied inline (replaces `setter.SetDefault`).

### SendRPCFunc

```go
type SendRPCFunc func(ctx context.Context, peer string, req RPCRequest) (RPCResponse, error)
```

Pointer out-param removed. Context carries `NetworkTimeout` deadline (applied by the production IO adapter, not the coroutine).

### NodeState

```go
type NodeState struct {
    Leader   string
    IsLeader bool
    Term     uint64
    Peers    []string
    Role     Role
}

type Role int

const (
    Follower Role = iota
    Candidate
    Leader
)
```

### Stats

```go
type Stats struct {
    Term             uint64
    Role             Role
    HeartbeatsSent   uint64
    HeartbeatsRecv   uint64
    ElectionsStarted uint64
    ElectionsWon     uint64
}
```

Counters are atomics updated by the coroutine. `Stats()` reads them without entering the coroutine.

### RPC Types

```go
type RPC string

const (
    HeartBeatRPC      RPC = "heartbeat"
    VoteRPC           RPC = "vote"
    ResetElectionRPC  RPC = "reset_election"
    ResignRPC         RPC = "resign"
    SetPeersRPC       RPC = "set_peers"
)

type RPCRequest struct {
    RPC     RPC
    Request any
}

type RPCResponse struct {
    RPC      RPC
    Response any
    Error    string
}
```

`GetStateRPC` removed — `GetState()` is an atomic read, no longer an RPC.

### RPC Message Types

Plain Go structs replacing proto-generated types. `structs.proto` and `structs.pb.go` are deleted.

```go
type VoteReq struct {
    Term      uint64
    Candidate string
}

type VoteResp struct {
    Term    uint64
    Granted bool
}

type HeartBeatReq struct {
    Term   uint64
    Leader string
}

type HeartBeatResp struct {
    Term uint64
}

type ResetElectionReq struct{}

type ResetElectionResp struct{}

type ResignReq struct{}

type ResignResp struct {
    Success bool
}

type SetPeersReq struct {
    Peers []string
}

type SetPeersResp struct{}
```

JSON marshaling in `rpc.go` (`RPCPayload`, `EncodeRPC`, `DecodeRPC`) follows the same structure — it type-switches on `Request`/`Response` and marshals via `json.RawMessage`. All struct fields use lowercase `json` tags matching the field name (e.g., `Term uint64 \`json:"term"\``, `Granted bool \`json:"granted"\``, `Leader string \`json:"leader"\``). This is the wire format; changing a tag is a breaking change. Wire values for all RPC constants use underscores (e.g., `"reset_election"`, `"set_peers"`), replacing the old hyphenated forms. Three struct fields also change on the wire: `HeartBeatReq.From` is renamed to `Leader`, `HeartBeatResp.From` is removed, and `VoteResp.Candidate` is removed — peer identity in vote fan-out is tracked in each spawned coroutine's closure rather than echoed in the response. A fourth breaking wire-format change: all struct fields drop `omitempty`. Fields with zero values (e.g., `Granted: false`, `Term: 0`) will now appear on the wire where they were previously omitted. All of these are breaking wire-format changes; there is no backward-compatibility shim.

### Error Sentinel

```go
var ErrNotLeader = errors.New("not the leader")
```

Unchanged.

## Node Lifecycle

### Start

`Start(ctx context.Context) error` is non-blocking. It creates the production IO adapter and gocoro scheduler, adds the main `run` coroutine, and launches the production main loop in a background goroutine. Config validation occurs in `NewNode`. The context governs startup only and is not retained. Double-start prevented by an atomic flag.

### Stop

`Stop(ctx context.Context) error` is idempotent: calling it on a never-started or already-stopped node returns `nil` immediately. Otherwise, it pushes an `EventShutdown` into the event channel. The coroutine sees it via `Recv`, transitions to shutdown state, and returns. If the current role is leader, `runLeader` sends `ResetElectionRPC` to all peers before returning (bounded by `NetworkTimeout`). Any fire-and-forget coroutines (heartbeats, election resets) that are in-flight when shutdown is received are allowed to complete normally — their `SendRPC` calls will either return or time out under `NetworkTimeout`. The main loop goroutine exits when `sched.Size()` reaches 0, meaning all spawned coroutines have also finished. `Stop` waits on `doneCh` or context expiry — on expiry, calls `sched.Shutdown()` and stops the main loop. `sched.Shutdown()` prevents new coroutines from being added but does not signal in-flight ones — coroutines blocked in `YieldAndAwait` are silently abandoned (goroutine leak). On the forced-shutdown path, `eventCh` is closed after `sched.Shutdown()` is called. Closing `eventCh` unblocks the `Recv` goroutine in the IO adapter, which exits upon receiving the zero value. Timer `AfterFunc` callbacks that fire after shutdown must check a `stopped` flag before writing to `eventCh` to avoid a send-to-closed-channel panic. The total leak on forced shutdown is bounded to spawned coroutines stuck in `YieldAndAwait` (main coroutine + at most one per peer), all of which are abandoned as the node is torn down. Returns `nil` on clean shutdown; returns `ctx.Err()` on context expiry.

### ReceiveRPC

`ReceiveRPC(ctx context.Context, req RPCRequest) (RPCResponse, error)` pushes an `Event{Kind: EventRPC, RPCReq: req, Respond: cb}` into the production IO's event channel. The callback `cb` writes to a per-call response channel. `ReceiveRPC` blocks on that response channel or context cancellation. `ReceiveRPC` only accepts peer-to-peer RPCs (`HeartBeatRPC`, `VoteRPC`, `ResetElectionRPC`); any other `RPC` value returns `(RPCResponse{Error: "unknown RPC"}, nil)` without entering the event channel. The Go `error` return is `nil` for all non-context cases; protocol-level failures are reported in `RPCResponse.Error`. `SetPeersRPC` and `ResignRPC` constants exist for consumers' transport-layer encoding but are never sent or received as peer-to-peer RPCs — `SetPeers` and `Resign` use dedicated event types via their `Node` interface methods.

### SetPeers / Resign

Same event+callback bridge as `ReceiveRPC`. Push `EventSetPeers` or `EventResign` with a `Done func(error)` callback. Block on the response or context cancellation.

## Production IO and Main Loop (`internal/prod`)

### IO Adapter

Implements `io.IO[engine.Req, engine.Resp]`:

| `Req.Kind` | Dispatch behavior | Blocking? |
|---|---|---|
| `SendRPC` | Goroutine calls `conf.SendRPC` with `NetworkTimeout` context, pushes completion to `completeCh` | Async |
| `Recv` | Goroutine reads from `eventCh`, pushes completion to `completeCh` | Async |
| `After` | Assigns `TimerID`, registers `time.AfterFunc` that pushes `EventTimer` to `eventCh`. Calls callback inline with `TimerID` | Sync |
| `Cancel` | Stops timer, removes from map. Calls callback inline | Sync |
| `Now` | Calls callback inline with `time.Now()` | Sync |

Sync callbacks resolve the promise within the same `RunUntilBlocked` call — the coroutine continues past those yield points without the main loop iterating.

### Main Loop

```
for sched.Size() > 0:
    sched.RunUntilBlocked(now)
    wait for at least one completion on completeCh
    invoke all ready completion callbacks
    loop
```

A `completion` struct carries the gocoro callback + response. The main loop invokes the callback (which calls `promise.Complete`), then calls `RunUntilBlocked` again so the scheduler can resume unblocked coroutines.

Shutdown: `EventShutdown` enters `eventCh` → `Recv` goroutine reads it → pushes to `completeCh` → main loop invokes callback → `RunUntilBlocked` → coroutine exits → `sched.Size() == 0` → loop ends → `doneCh` closes.

## Simulation Design (`internal/sim`)

### Architecture

One gocoro scheduler per node. A single `Simulation` orchestrator controls all nodes, the shared virtual clock, a global timer heap, and the RPC routing layer. Only one node's coroutine runs at a time (single-stepper model).

### Virtual Clock

```go
type VirtualClock struct {
    now time.Time
}
```

Shared across all nodes. Advances only when the orchestrator explicitly calls `Advance(d)`. All `Now` yields return this clock's value.

### Timer Heap

A min-heap of pending timers sorted by fire time:

```go
type pendingTimer struct {
    fireAt  time.Time
    timerID int64
    nodeID  string
}
```

When a coroutine yields `After{Delay: d}`, the simulation IO computes `fireAt = clock.Now() + d`, pushes onto the heap, and resolves the callback immediately with the `TimerID`.

### Simulation IO Adapter (per node)

| `Req.Kind` | Dispatch behavior |
|---|---|
| `SendRPC` | Calls `sim.queueRPC(nodeID, req.Peer, req.RPCReq, cb)` — does not resolve callback inline |
| `Recv` | If events queued: dequeue and call callback inline. Otherwise: park callback as `pendingRecv` |
| `After` | Register on timer heap. Call callback inline with `TimerID` |
| `Cancel` | Remove from timer heap. Call callback inline |
| `Now` | Call callback inline with `clock.Now()` |

### Cross-Node RPC Delivery

When Node A yields `SendRPC` to Node B:

1. A's coroutine suspends (awaiting `SendRPC` response)
2. Orchestrator delivers the RPC as an event to B's `simIO` event queue
3. Orchestrator steps B's scheduler — B processes the RPC, calls `event.Respond(response)`
4. Orchestrator takes B's response and resolves A's `SendRPC` callback
5. Orchestrator steps A's scheduler — A resumes with the response

### Orchestrator Loop

```
for !sim.done():
    find next event (earliest timer fire or queued RPC)
    advance clock to that event's time
    deliver event to target node
    step target node's scheduler via RunUntilBlocked
    drain any outbound RPCs queued during the step
```

No goroutines, no channels, no real time. Fully deterministic given the same seed.

**Event ordering for same-timestamp events:** When a queued RPC and a pending timer share the same virtual timestamp, the queued RPC is processed first. Among multiple queued RPCs at the same timestamp, FIFO order (insertion order) is used. Among multiple timers at the same timestamp, the timer with the lower `TimerID` is processed first (TimerIDs are assigned monotonically). This ordering is fixed and must not change — tests depend on it for reproducibility.

`sim.done()` returns true when the virtual clock has advanced past the simulation's configured end time or when `RunUntilLeader` detects exactly one leader across all nodes. `RunUntilLeader` drives the loop until one node reports `IsLeader() == true` (or a configurable step limit is exceeded, returning an error). `RunFor(d time.Duration)` advances the virtual clock by `d` and drives the loop until all events within that window are processed.

### Seeded RNG

Each node receives a `*rand.Rand` seeded from `masterSeed + nodeIndex`. Used for `randomDuration` in election/heartbeat timeouts. Reproducible across runs.

### Fault Injection

All fault injection hooks live in `queueRPC`, checked in this order:

1. **Network partitions** — bidirectional isolation between named node pairs. Implemented as 100% RPC drops in both directions. `sim.Partition(groupA, groupB)` / `sim.HealAll()`.
2. **RPC drops** — probabilistic, using the simulation's seeded RNG. `sim.SetDropRate(float64)` for global, `sim.SetNodeDropRate(nodeID, float64)` per-node.
3. **RPC delays** — deliver after a virtual-clock offset. `sim.SetDelay(from, to, duration)`. Delayed RPCs go onto the timer heap.

When a drop or partition triggers, the sender's callback is resolved immediately with an error — no delivery to the target.

### Simulation Config

```go
type Config struct {
    NumNodes   int
    Seed       int64
    DropRate   float64
    NodeConfig map[string]NodeSimConfig
}

type NodeSimConfig struct {
    ElectionTimeout  time.Duration
    HeartBeatTimeout time.Duration
}
```

### Simulation API

The `Simulation` type exposes the following methods for test assertions and fault injection:

```go
// Execution control
func (s *Simulation) RunUntilLeader() error   // drives the loop until one leader; error on step limit
func (s *Simulation) RunFor(d time.Duration)  // drives the loop for d of virtual time

// Assertions
func (s *Simulation) LeaderCount() int        // number of nodes currently reporting IsLeader() == true
func (s *Simulation) Leader() string          // UniqueID of the current leader; empty if none
func (s *Simulation) Node(id string) Node     // returns the Node interface for the named node

// Fault injection
func (s *Simulation) Partition(groupA, groupB []string)
func (s *Simulation) HealAll()
func (s *Simulation) SetDropRate(rate float64)
func (s *Simulation) SetNodeDropRate(nodeID string, rate float64)
func (s *Simulation) SetDelay(from, to string, d time.Duration)
```

## Dependencies

### Added

| Dependency | Purpose |
|---|---|
| `github.com/resonatehq/gocoro` | Coroutine scheduler |

### Removed

| Dependency | Replacement |
|---|---|
| `github.com/mailgun/holster/v4/setter` | Inline zero-check |
| `github.com/mailgun/holster/v4/slice` | `slices.Contains` (Go 1.21 stdlib) |
| `github.com/mailgun/holster/v4/syncutil` | Removed — coroutine scheduler replaces `WaitGroup` usage |
| `github.com/mailgun/holster/v4/testutil` | Removed — deterministic simulation replaces `UntilPass` |
| `github.com/sirupsen/logrus` | `log/slog` (stdlib) |
| `github.com/golang/protobuf` | Plain Go structs |

### Minimum Go Version

Go 1.21 — required for `slices.Contains` and `log/slog`.

## Error Handling

| Error | Condition |
|---|---|
| `ErrNotLeader` | `Resign` called when not leader |
| `context.DeadlineExceeded` / `context.Canceled` | `ReceiveRPC`, `SetPeers`, `Resign`, `Stop` context expires before completion |
| `NewNode` validation errors | Missing `UniqueID`, missing `SendRPC`, invalid timeouts |

RPC-level errors propagate through `RPCResponse.Error` (string field, unchanged from current design). `SendRPCFunc` errors in fan-out coroutines are handled by the spawned coroutine — vote requests return `VoteResp{Granted: false}`, heartbeats and election resets log and exit.

## Security

No changes to the security model. The library never touches the network directly — all transport security is the consumer's responsibility via their `SendRPCFunc` and `ReceiveRPC` handler.

## Observability

- **`Stats()` method**: Atomic counters for heartbeats sent/received, elections started/won. Readable at any time without entering the coroutine. Serves both production debugging and test assertions.
- **`GetState()` method**: Atomic snapshot of leader, isLeader, term, peers, role.
- **`OnLeaderChange` callback**: Synchronous notification on leader transitions.
- **Structured logging via `slog`**: Node ID always present via `slog.With("node", id)`. Key events: role transitions, elections started/won, heartbeat timeouts, leader quorum loss, RPC errors.

## Testing

Testing follows the `surface-testing` skill.

**Surface:** The `Node` interface — `NewNode` + all methods. Tests interact exclusively through the public API. The simulation harness drives schedulers internally but asserts through `GetState()`, `IsLeader()`, `GetLeader()`, and `Stats()`.

**External dependencies:** None to fake. `SendRPCFunc` is replaced by simulation IO routing. Wall clock is replaced by virtual clock. RNG is seeded per-node. The architecture eliminates the need for any test fakes.

**Observability for async behavior:** `Stats()` exposes counters for internal operations. `GetState()` exposes full node state. The deterministic simulation makes all behavior synchronously observable after each step — no `require.Eventually` needed.

**Time handling:** All time access goes through `YieldAndAwait` (`After` and `Now`). The IO adapter provides real or virtual time. The coroutine never calls `time.Now()` directly.

**Test structure:** All tests use the `Simulation` type from `internal/sim`. The existing `TestCluster` in `cluster_test.go` is replaced entirely.

```go
func TestLeaderElection(t *testing.T) {
    sim := sim.New(sim.Config{NumNodes: 3, Seed: 42})
    sim.RunUntilLeader()

    assert.Equal(t, 1, sim.LeaderCount())
    require.NotEmpty(t, sim.Leader())
}

func TestPartitionedMinorityCantElect(t *testing.T) {
    sim := sim.New(sim.Config{NumNodes: 5, Seed: 42})
    sim.RunUntilLeader()

    sim.Partition([]string{"n0"}, []string{"n1", "n2", "n3", "n4"})
    sim.RunFor(10 * time.Second) // virtual time

    assert.Equal(t, 1, sim.LeaderCount())
    assert.False(t, sim.Node("n0").IsLeader())
}
```

**Migrated scenarios:** Every existing test in `election_test.go` gets a deterministic equivalent — no `testutil.UntilPass`, no wall-clock sleeps. `rpc_test.go` is rewritten to match the new wire format: updated RPC constant values (underscores replacing hyphens), renamed/removed struct fields (`HeartBeatReq.Leader` replaces `From`, `HeartBeatResp.From` removed, `VoteResp.Candidate` removed), and `omitempty` removal. The PRD requires at least two new fault-injection scenarios (RPC drops, partitions).

## Files Deleted

| File | Reason |
|---|---|
| `structs.proto` | Replaced by plain Go structs |
| `structs.pb.go` | Replaced by plain Go structs |

## Open Questions

None — all technical questions were resolved during the design discussion.
