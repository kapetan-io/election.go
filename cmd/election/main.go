package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kapetan-io/election.go"
)

func sendRPC(ctx context.Context, peer string, req election.RPCRequest) (election.RPCResponse, error) {
	// Marshall the RPC request to json
	b, err := json.Marshal(req)
	if err != nil {
		return election.RPCResponse{}, fmt.Errorf("while encoding request: %w", err)
	}

	// Create a new http request with context
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/rpc", peer), bytes.NewBuffer(b))
	if err != nil {
		return election.RPCResponse{}, fmt.Errorf("while creating request: %w", err)
	}

	// Send the request
	hp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return election.RPCResponse{}, fmt.Errorf("while sending http request: %w", err)
	}
	defer func() {
		_ = hp.Body.Close()
	}()

	// Decode the response from JSON
	var resp election.RPCResponse
	dec := json.NewDecoder(hp.Body)
	if err := dec.Decode(&resp); err != nil {
		return election.RPCResponse{}, fmt.Errorf("while decoding response: %w", err)
	}
	return resp, nil
}

func newHandler(node election.Node) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		dec := json.NewDecoder(r.Body)
		var req election.RPCRequest
		if err := dec.Decode(&req); err != nil {
			status := http.StatusBadRequest
			w.WriteHeader(status)
			if _, err := w.Write([]byte(err.Error())); err != nil {
				slog.Warn("while writing response", "status", status, "err", err)
			}
			return
		}

		// Example of how a peer might exclude RPC commands it doesn't want made.
		if req.RPC == election.SetPeersRPC {
			status := http.StatusBadRequest
			w.WriteHeader(status)
			if _, err := fmt.Fprintf(w, "RPC request '%s' not allowed", req.RPC); err != nil {
				slog.Warn("while writing response", "status", status, "err", err)
			}
			return
		}

		resp, err := node.ReceiveRPC(r.Context(), req)
		if err != nil {
			status := http.StatusInternalServerError
			w.WriteHeader(status)
			if _, err = w.Write([]byte(err.Error())); err != nil {
				slog.Warn("while writing response", "status", status, "err", err)
			}
			return
		}

		enc := json.NewEncoder(w)
		if err := enc.Encode(resp); err != nil {
			status := http.StatusInternalServerError
			w.WriteHeader(status)
			if _, err = w.Write([]byte(err.Error())); err != nil {
				slog.Warn("while writing response", "status", status, "err", err)
			}
		}
	}
}

// waitForServer dials addr in a retry loop until it succeeds or the attempt
// limit is reached. It replaces the old election.WaitForConnect helper.
func waitForServer(addr string, attempts int, delay time.Duration) error {
	for i := 0; i < attempts; i++ {
		resp, err := http.Get(fmt.Sprintf("http://%s/rpc", addr)) //nolint:noctx
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("server at %s did not become ready after %d attempts", addr, attempts)
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		slog.Error("usage: election <election-address:port> [peer1,peer2,...]")
		os.Exit(1)
	}

	electionAddr := os.Args[1]

	// Parse optional comma-separated peer list
	var peers []string
	if len(os.Args) == 3 && os.Args[2] != "" {
		for _, p := range strings.Split(os.Args[2], ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}
	// Always include self
	found := false
	for _, p := range peers {
		if p == electionAddr {
			found = true
			break
		}
	}
	if !found {
		peers = append([]string{electionAddr}, peers...)
	}

	node, err := election.NewNode(election.Config{
		// A unique identifier used to identify us in a list of peers
		UniqueID: electionAddr,
		Peers:    peers,
		// Called whenever the library detects a change in leadership
		OnChange: func(state election.NodeState) {
			slog.Info("leader changed", "leader", state.Leader, "term", state.Term)
		},
		// Called when the library wants to contact other peers
		SendRPC: sendRPC,
	})
	if err != nil {
		slog.Error("failed to create node", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", newHandler(node))
	go func() {
		server := &http.Server{
			Addr:              electionAddr,
			Handler:           mux,
			ReadHeaderTimeout: 1 * time.Minute,
		}
		if err := server.ListenAndServe(); err != nil {
			slog.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	// Wait until the http server is up and can receive RPC requests
	if err := waitForServer(electionAddr, 10, 100*time.Millisecond); err != nil {
		slog.Error("server readiness check failed", "err", err)
		os.Exit(1)
	}

	// Now that our http handler is listening for requests we
	// can safely start the election.
	if err := node.Start(context.Background()); err != nil {
		slog.Error("failed to start node", "err", err)
		os.Exit(1)
	}

	slog.Info("election node started", "addr", electionAddr, "peers", peers)

	// Wait here for signals to clean up our mess
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	for range c {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		_ = node.Stop(ctx)
		cancel()
		os.Exit(0)
	}
}
