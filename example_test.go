package election_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/kapetan-io/election.go"
)

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
	defer func() {
		_ = hp.Body.Close()
	}()

	var resp election.RPCResponse
	dec := json.NewDecoder(hp.Body)
	if err := dec.Decode(&resp); err != nil {
		return election.RPCResponse{}, fmt.Errorf("while decoding response: %w", err)
	}
	return resp, nil
}

func newHandler(t *testing.T, node election.Node) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		dec := json.NewDecoder(r.Body)
		var req election.RPCRequest
		if err := dec.Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, err = w.Write([]byte(err.Error()))
			require.NoError(t, err)
			return
		}
		resp, err := node.ReceiveRPC(r.Context(), req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte(err.Error()))
			require.NoError(t, err)
			return
		}

		enc := json.NewEncoder(w)
		if err := enc.Encode(resp); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte(err.Error()))
			require.NoError(t, err)
		}
	}
}

// SimpleExample spawns 2 nodes. In a real application you would
// only spawn a single node which would represent your application
// in the election.
func SimpleExample(t *testing.T) {
	node1, err := election.NewNode(election.Config{
		Peers:    []string{"localhost:7080", "localhost:7081"},
		UniqueID: "localhost:7080",
		OnLeaderChange: func(leader string) {
			log.Printf("Current Leader: %s\n", leader)
		},
		SendRPC: sendRPC,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := node1.Stop(context.Background())
		require.NoError(t, err)
	}()

	node2, err := election.NewNode(election.Config{
		Peers:    []string{"localhost:7080", "localhost:7081"},
		UniqueID: "localhost:7081",
		SendRPC:  sendRPC,
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/rpc", newHandler(t, node1))
		server := &http.Server{
			Addr:              ":7080",
			Handler:           mux,
			ReadHeaderTimeout: 1 * time.Minute,
		}
		log.Fatal(server.ListenAndServe())
	}()

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/rpc", newHandler(t, node2))
		server := &http.Server{
			Addr:              ":7081",
			Handler:           mux,
			ReadHeaderTimeout: 1 * time.Minute,
		}
		log.Fatal(server.ListenAndServe())
	}()

	// Now that both http handlers are listening for requests we
	// can safely start the election.
	err = node1.Start(context.Background())
	require.NoError(t, err)
	err = node2.Start(context.Background())
	require.NoError(t, err)

	// Wait here for signals to clean up our mess
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	for range c {
		err = node1.Stop(context.Background())
		require.NoError(t, err)
		err = node2.Stop(context.Background())
		require.NoError(t, err)
		break
	}
}
