# 2. Recv as Universal Blocking Point with Non-Blocking Timers

Date: 2026-05-17

## Status

Accepted

## Context

The coroutine-based state machine needs a model for how timers interact with the event loop. Two approaches were considered:

- **Blocking timers**: `After` suspends the coroutine until the timer fires. The coroutine must use `Spawn` to run a timer concurrently with other event handling, then `Await` or `select`-equivalent to multiplex. This reintroduces the complexity of Go's `select` — the very thing the refactor eliminates.
- **Non-blocking timers**: `After` registers a timer and returns a handle (`TimerID`) immediately. The timer fire arrives later as an `EventTimer` through `Recv`. A single `Recv` loop handles all event types uniformly.

The state machine's `runFollower`, `runCandidate`, and `runLeader` methods each wait on multiple concurrent signals (inbound RPCs, multiple timers, shutdown). The current implementation uses Go `select` to multiplex these — any replacement must handle the same multiplexing without reintroducing non-determinism.

## Decision

`After` is non-blocking: it registers a timer via `YieldAndAwait` and returns a `TimerID` immediately. Timer fires are delivered as `EventTimer` events through `Recv`. `Cancel` removes a pending timer by handle (no-op if already fired or cancelled).

`Recv` is the sole blocking yield point in the state machine. All external input — inbound RPCs, timer fires, `SetPeers`, `Resign`, shutdown — arrives as a tagged `Event` through `Recv`. The coroutine event loop is a single `for` loop that yields `Recv` and dispatches on `Event.Kind`.

All five IO operations (`SendRPC`, `Recv`, `After`, `Cancel`, `Now`) go through `YieldAndAwait` for uniform scheduler mediation, even though `After`, `Cancel`, and `Now` resolve instantly.

## Consequences

- The state machine has exactly one multiplexing point (`Recv`), replacing the four `select` statements in the current code. No coroutine-level multiplexing or concurrent timer spawning needed.
- Timer registration and cancellation are pure bookkeeping — no coroutine suspension, no scheduler interaction beyond the yield.
- In simulation, the timer heap is a simple data structure that the orchestrator inspects to decide when to advance the virtual clock. No goroutines or channels involved.
- The uniform `YieldAndAwait` path means the simulation IO adapter sees every operation, even trivial ones like `Now`. This is the cost of full determinism — every interaction with the outside world is mediated by the scheduler.
