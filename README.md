
<h2 align="center">
Election.go<br />
A network-agnostic leader election library for Go.
</h2>

[![GitHub tag](https://img.shields.io/github/tag/kapetan-io/election.go?include_prereleases=&sort=semver&color=blue)](https://github.com/kapetan-io/election.go/releases/)
[![CI](https://github.com/kapetan-io/election.go/workflows/CI/badge.svg)](https://github.com/kapetan-io/election.go/actions?query=workflow:"CI")
[![License](https://img.shields.io/badge/License-Apache-blue)](#license)

This is a network-agnostic implementation of the leader election portion of the RAFT protocol. The library
provides no peer discovery mechanism — the user must call `SetPeers()` when the list of peers changes.
Any service discovery mechanism will work: consul, etcd, k8s, memberlist, DNS, etc.

Internally the state machine is implemented as a [gocoro](https://github.com/resonatehq/gocoro) coroutine.
The same state machine code runs in both production and in the deterministic simulation test harness,
where virtual time, network partitions, and RPC drops can be injected with full reproducibility.

## Usage
The user must provide a `SendRPC()` function at initialization time. This function is called whenever
RPC communication between nodes is needed. A node that wishes to receive an RPC call must call
`Node.ReceiveRPC()` when the RPC request arrives over whatever network protocol the user implements.

```go
package main

import (
    "context"
    "log"
    "log/slog"

    "github.com/kapetan-io/election.go"
)

func main() {
    node, err := election.NewNode(election.Config{
        UniqueID: "node-1",
        Peers:    []string{"node-1", "node-2", "node-3"},
        SendRPC:  sendRPC,  // your network transport
        OnLeaderChange: func(leader string, term uint64) {
            slog.Info("leader changed", "leader", leader)
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Start participating in the election
    if err := node.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer node.Stop(context.Background())

    // Query state at any time (non-blocking atomic reads)
    if node.IsLeader() {
        slog.Info("I am the leader")
    }
}
```

### HTTP Transport Example
See [`example_test.go`](example_test.go) for a complete working example using HTTP as the transport.

```go
func sendRPC(ctx context.Context, peer string, req election.RPCRequest) (election.RPCResponse, error) {
    b, err := json.Marshal(req)
    if err != nil {
        return election.RPCResponse{}, fmt.Errorf("while encoding request: %w", err)
    }

    hr, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/rpc", peer), bytes.NewBuffer(b))
    if err != nil {
        return election.RPCResponse{}, fmt.Errorf("while creating request: %w", err)
    }

    hp, err := http.DefaultClient.Do(hr)
    if err != nil {
        return election.RPCResponse{}, fmt.Errorf("while sending http request: %w", err)
    }
    defer hp.Body.Close()

    var resp election.RPCResponse
    if err := json.NewDecoder(hp.Body).Decode(&resp); err != nil {
        return election.RPCResponse{}, fmt.Errorf("while decoding response: %w", err)
    }
    return resp, nil
}
```

The receiving side calls `ReceiveRPC`:
```go
mux.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
    var req election.RPCRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    resp, err := node.ReceiveRPC(r.Context(), req)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    json.NewEncoder(w).Encode(resp)
})
```

## Node API
- `NewNode(Config) (Node, error)` - Create a new election node
- `Node.Start(ctx) error` - Begin participating in the election
- `Node.Stop(ctx) error` - Cease participation; blocks until shutdown completes or ctx expires
- `Node.SetPeers(ctx, peers) error` - Update the peer list dynamically
- `Node.Resign(ctx) error` - Step down as leader; returns `ErrNotLeader` if not the leader
- `Node.IsLeader() bool` - Non-blocking atomic read
- `Node.GetLeader() string` - Non-blocking atomic read; returns the leader's UniqueID
- `Node.GetState() NodeState` - Non-blocking snapshot of role, term, leader, and peers
- `Node.ReceiveRPC(ctx, req) (RPCResponse, error)` - Handle an incoming peer RPC
- `Node.Stats() Stats` - Activity counters (heartbeats sent/recv, elections started/won)

## Deterministic Simulation Testing
The library includes a deterministic simulation harness (`internal/sim`) that runs the same coroutine
state machine with virtual time. The entire test suite runs in under 2 seconds with `-race` enabled.

Fault injection capabilities:
- **Network partitions** - Bidirectional isolation between groups of nodes
- **RPC drops** - Global or per-node drop rates
- **RPC delays** - Per-link artificial latency
- **Deterministic replay** - Same seed produces identical execution
