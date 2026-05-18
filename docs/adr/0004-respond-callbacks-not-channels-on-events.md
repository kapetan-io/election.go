# 4. Respond Callbacks on Events Instead of Channels

Date: 2026-05-17

## Status

Accepted

## Context

When the coroutine processes an inbound RPC via `Recv`, it needs to send a response back to the caller (the goroutine blocked in `ReceiveRPC`). Two approaches were considered:

- **Channel on event**: The `Event` struct carries a `chan RPCResponse`. The coroutine writes the response to the channel. The `ReceiveRPC` caller reads from the channel. This mirrors the current `respChan` pattern on `RPCRequest`.
- **Callback on event**: The `Event` struct carries a `Respond func(RPCResponse)`. The coroutine calls the function. The IO adapter provides the implementation — production writes to a channel, simulation stores the result in the orchestrator's state.

The project's first design principle is "same code path in production and simulation." Channels are Go concurrency primitives — writing to a channel inside the coroutine introduces a dependency on Go's runtime scheduler, which is non-deterministic when multiple goroutines are involved. The coroutine state machine must be free of channels, goroutines, and `select` to be fully deterministic under simulation.

## Decision

Events carry response callbacks (`Respond func(RPCResponse)` for RPCs, `Done func(error)` for `SetPeers`/`Resign`), not channels. The coroutine calls these as plain function calls between yield points. The IO adapter provides the callback implementation:

- **Production**: The callback writes to a per-call `chan` that `ReceiveRPC`/`SetPeers`/`Resign` blocks on. The channel exists in the IO adapter, never in the coroutine.
- **Simulation**: The callback stores the response in the orchestrator's state for the calling node to pick up. No channels involved.

## Consequences

- The coroutine state machine contains zero Go concurrency primitives — no channels, no goroutines, no `select`. All concurrency is cooperative via the gocoro scheduler.
- The same coroutine code runs identically in production and simulation. The `event.Respond(resp)` call is a plain function invocation regardless of mode.
- Channels are pushed to the IO adapter boundary, where non-determinism is acceptable (production) or absent (simulation).
- The `Event` struct is slightly less obvious than a channel-based design — a reader must understand that `Respond` is an IO-adapter-provided callback, not a method on the event. The naming and documentation must make this clear.
