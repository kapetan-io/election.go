# 3. One Scheduler Per Node in Simulation

Date: 2026-05-17

## Status

Accepted

## Context

The deterministic simulation testing (DST) harness runs multiple election nodes in a single process. Each node's state machine is a coroutine on a gocoro scheduler. Two architectures were considered:

- **Global scheduler**: All nodes' coroutines run on a single `Scheduler[Req, Resp]` with a single IO adapter. The IO adapter must demultiplex requests by node, and the scheduler's stepping order determines which node runs next. Simpler setup, but the IO adapter becomes complex — it must track per-node timer heaps, per-node event queues, and per-node RNG state within a single `Dispatch` implementation.
- **Per-node scheduler**: Each node gets its own `Scheduler[Req, Resp]` with its own IO adapter. The simulation orchestrator controls which node's scheduler to step and when. Each IO adapter is simple — it manages one node's timers, events, and RNG.

## Decision

One gocoro scheduler per node. A single `Simulation` orchestrator owns all node schedulers, the shared virtual clock, and the RPC routing layer. The orchestrator calls `RunUntilBlocked` on individual node schedulers in a controlled order — only one node's coroutine runs at a time (single-stepper model).

Cross-node RPC delivery is orchestrator-mediated: when Node A yields `SendRPC` to Node B, A suspends, the orchestrator delivers the RPC to B's event queue, steps B's scheduler, collects B's response, resolves A's callback, then steps A.

## Consequences

- Each simulation IO adapter is trivial — it handles one node's state with no demultiplexing logic.
- The orchestrator has full control over execution order, making cross-node interactions deterministic and reproducible.
- The per-node scheduler matches the production model (each `node` struct owns its scheduler), so the simulation exercises the same lifecycle code paths.
- Adding or removing nodes mid-simulation is straightforward — create or destroy an individual scheduler without affecting others.
- The orchestrator loop is more complex than a global-scheduler approach — it must explicitly manage cross-node RPC delivery and decide stepping order. This complexity is acceptable because it lives in test infrastructure, not production code.
