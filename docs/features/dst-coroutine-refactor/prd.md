# Election Library: Production Readiness via DST Coroutine Refactor

## Problem

The election library (`github.com/thrawn01/election`) is a transport-agnostic implementation of RAFT leader election that works but is not production-ready. The internal architecture — built on Go channels, goroutines, and `select` — makes the codebase difficult to test deterministically. Tests rely on wall-clock polling (`testutil.UntilPass`) and real timers, making them slow and non-reproducible. The public API has rough edges (no context support, silent failures, pointer out-params). The dependency tree pulls in the entire `holster/v4` transitive graph (consul, memberlist, serf) for three trivial utility functions.

This refactor replaces the channel/goroutine internals with a coroutine-based architecture using `github.com/resonatehq/gocoro`, enabling deterministic simulation testing (DST). The same rewrite is the opportunity to clean up the public API surface and remove unnecessary dependencies.

## Users

Go developers who need leader election without being tied to a specific network transport or service discovery system. They provide their own `SendRPC` implementation and peer discovery (consul, k8s, memberlist, etc.).

The deterministic simulation harness is an internal development tool for testing the library itself. It is not part of the public API.

## Core Design Principles

1. **Same code path in production and simulation.** The node's state machine is identical in both modes. Only the IO implementation passed to the coroutine scheduler differs — production IO uses real network calls and wall-clock time; simulation IO uses a virtual clock and in-memory RPC routing.

2. **No `select`, no goroutines inside the node.** All concurrency is cooperative via the coroutine scheduler. Go's `select` is non-deterministic when multiple cases are ready, which makes simulation impossible. Eliminating it is what makes deterministic testing work.

3. **Transport-agnostic.** The library never touches the network directly. Consumers provide a `SendRPC` function at configuration time with signature `func(ctx context.Context, peer string, req RPCRequest) (RPCResponse, error)` and call `ReceiveRPC` when inbound RPCs arrive over whatever transport they use. Peer identifiers are opaque strings; the library never interprets them.

4. **Parallel fan-out via spawned coroutines.** Outbound RPCs to peers (heartbeats, vote requests) are sent in parallel by spawning a coroutine per peer. The scheduler controls stepping order, so parallelism is deterministic in simulation and performant in production.

## Scope

### In Scope

**State machine rewrite (single release)**
- Replace all channel/goroutine/select-based internals (`run`, `runFollower`, `runCandidate`, `runLeader`, `electSelf`, `sendHeartBeat`, `sendElectionReset`, `processRPC`) with coroutine yield points (`gocoro.YieldAndAwait`)
- Define `IOReq` and `IOResp` types for yield points: `SendRPC` (send RPC and suspend until response), `Recv` (suspend until next inbound event), `After` (register a timer; fires by delivering an event via `Recv`), `Cancel` (remove a pending timer by handle), `Now` (read current clock without suspending)
- Implement production IO (real network, real timers, real clock)
- Remove `rpcCh`, `shutdownCh`, `running` atomic, `syncutil.WaitGroup` — replaced by coroutine scheduler lifecycle

**Public API cleanup**
- `ReceiveRPC(ctx context.Context, req RPCRequest) (RPCResponse, error)` — context-aware, explicit error return, no pointer out-param
- Callback-based `OnLeaderChange` — synchronous callback invoked from the coroutine event loop (must not block or re-enter the node), no channel subscription (avoids forcing goroutines on consumers who may also use DST). `Config.OnUpdate func(string)` is removed; `OnLeaderChange` is its replacement and is set on `Config` in the same way.
- `IsLeader()` / `GetLeader()` / `GetState() NodeState` via atomic reads — truly non-blocking, remove channel hop. `GetState` returns the full node state (leader, isLeader, term, peers, current role) for callers that do not want to track callback state. The old blocking signature `GetState(ctx context.Context) (NodeState, error)` is removed.
- `SetPeers(ctx context.Context, peers []string) error` / `Resign(ctx context.Context) error` — routed through the coroutine scheduler. Both accept a context so callers can set a deadline; a cancelled or timed-out context returns an error. There are no silent drops.
- Context propagation into `electSelf` / `sendHeartBeat` (currently `context.Background()`)
- Node lifecycle: `Start(ctx context.Context) error` is non-blocking — it launches the coroutine scheduler in a background goroutine and returns; the context governs only the startup phase and is not retained after `Start` returns. `Stop(ctx context.Context) error` is the sole stop mechanism — it stops the scheduler gracefully, waiting for in-flight RPCs to drain or the context to expire. The old `shutdownCh` / `running` atomic / `syncutil.WaitGroup` are removed.

**Simulation harness (internal)**
- Virtual clock with deterministic time advancement
- Deterministic timer heap
- Cross-node RPC delivery via single-stepper model — only one node's coroutine runs at a time. When Node A sends an RPC, the scheduler suspends A, steps B to process the request, queues B's response back to A, then resumes A. This serialization makes all inter-node interactions deterministic and eliminates reentrancy.
- Fault injection: (a) RPC drops — a configured percentage of `SendRPC` calls return an error without delivery; (b) RPC delays — a `SendRPC` call delivers after a configurable virtual-clock offset; (c) network partitions — bidirectional isolation between a named set of node pairs, implemented as 100% RPC drops in both directions for those pairs. Unidirectional partitions are out of scope.
- Seeded `*rand.Rand` per node for reproducible election timeouts

**Test migration**
- Port all existing test scenarios to the deterministic simulation harness, including split-brain scenarios
- No test coverage regression — every scenario currently tested must have a deterministic equivalent
- Remove all `testutil.UntilPass` polling and wall-clock sleeps

**Bug fixes (resolved by rewrite)**
- `electSelf` double-send (`election.go:481-496`): missing `return` after error-path send — goes away when channels are removed
- `send()` silent drop (`election.go:852-857`): returns empty `RPCResponse{}` with no error when `rpcCh` is full — goes away when `rpcCh` is removed

**Dependency cleanup**
- Replace `holster/v4/slice.ContainsString` with `slices.Contains`
- Replace `holster/v4/setter.SetDefault` with inline zero-check
- Replace `holster/v4/syncutil.WaitGroup` with `sync.WaitGroup` or `errgroup.Group`
- Drop `github.com/golang/protobuf` — replace with plain Go structs defined in the library package. The `google.golang.org/protobuf` dependency is not added; consumers do not need a proto toolchain. `RPCRequest` and `RPCResponse` are ordinary exported Go structs. Field definitions are deferred to the tech spec.
- Replace `logrus` with `log/slog` (stdlib)
- Remove `WaitForConnect` — unrelated to leader election, belongs in the caller's integration layer
- Add `github.com/resonatehq/gocoro` as a normal `go.mod` dependency

### Out of Scope / Non-Goals

- **Metrics/observability** (elections started, won, heartbeats sent/missed, step-downs, dropped RPCs) — valuable but additive, can layer on after refactor. Tracked in `docs/known-issues.md`.
- **Fencing token / monotonic lease epoch** — safety enhancement for leadership, separate feature. Tracked in `docs/known-issues.md`.
- **PreVote protocol** — not a gap. The library already prevents partitioned-node disruption via the leader-check guard in `handleVote`: followers with a known leader reject all vote requests from other candidates. See ADR for details.
- **Storage interface for term/vote persistence** — not a gap. Nodes rejoin as fresh members via `SetPeers`, so there is no double-voting risk from restarts.
- **Module version bump** — staying on current `github.com/thrawn01/election` import path. No known external consumers.

## Success Metrics

All verifiable at merge time:

1. **All existing test scenarios pass deterministically** — no `testutil.UntilPass` polling, no wall-clock sleeps, no flaky runs
2. **Test suite completes in under 1 second** — virtual time makes tests near-instant
3. **Zero `holster/v4` dependencies remain** — transitive dependency tree drastically reduced
4. **At least two new fault-injection test scenarios** that were impractical with the old harness: one RPC-drop scenario (e.g., majority of vote requests dropped) and one partition scenario (e.g., minority partition cannot elect a leader). Demonstrates DST value beyond porting existing tests.

## Dependencies and Constraints

- **gocoro** (`github.com/resonatehq/gocoro`): last commit Sept 2024, 13 stars, one real user (Resonate). Small enough to vendor or fork if abandoned. Taken as a normal dependency for now.
- **Go 1.21 minimum** — required for `slices.Contains` (added in 1.21) and `log/slog` (added in 1.21)
- **Breaking API changes**: this is a breaking release with no semver version bump. Acceptable because there are no known external consumers.

## Open Questions

- **Internal IOReq naming**: the `Recv` yield type must have an internal name distinct from the public `ReceiveRPC` method to avoid confusion. Naming deferred to tech spec.
