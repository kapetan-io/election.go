package election_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrawn01/election"
)

func TestRPCRequest(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   election.RPCRequest
		out  string
	}{
		{
			name: "heartbeat",
			in: election.RPCRequest{
				RPC: election.HeartBeatRPC,
				Request: election.HeartBeatReq{
					Leader: "node1",
					Term:   1,
				},
			},
			out: `{"rpc":"heartbeat","request":{"term":1,"leader":"node1"}}`,
		},
		{
			name: "vote",
			in: election.RPCRequest{
				RPC: election.VoteRPC,
				Request: election.VoteReq{
					Candidate: "node1",
					Term:      1,
				},
			},
			out: `{"rpc":"vote","request":{"term":1,"candidate":"node1"}}`,
		},
		{
			name: "reset",
			in: election.RPCRequest{
				RPC:     election.ResetElectionRPC,
				Request: election.ResetElectionReq{},
			},
			out: `{"rpc":"reset_election","request":{}}`,
		},
		{
			name: "resign",
			in: election.RPCRequest{
				RPC:     election.ResignRPC,
				Request: election.ResignReq{},
			},
			out: `{"rpc":"resign","request":{}}`,
		},
		{
			name: "set-peers",
			in: election.RPCRequest{
				RPC:     election.SetPeersRPC,
				Request: election.SetPeersReq{Peers: []string{"n0", "n1"}},
			},
			out: `{"rpc":"set_peers","request":{"peers":["n0","n1"]}}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.out, string(b))

			var in election.RPCRequest
			err = json.Unmarshal(b, &in)
			require.NoError(t, err)
			assert.Equal(t, tt.in, in)
		})
	}
}

func TestRPCResponse(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   election.RPCResponse
		out  string
	}{
		{
			name: "heartbeat",
			in: election.RPCResponse{
				RPC: election.HeartBeatRPC,
				Response: election.HeartBeatResp{
					Term: 1,
				},
			},
			out: `{"rpc":"heartbeat","response":{"term":1}}`,
		},
		{
			name: "heartbeat-zero-term",
			in: election.RPCResponse{
				RPC:      election.HeartBeatRPC,
				Response: election.HeartBeatResp{},
			},
			out: `{"rpc":"heartbeat","response":{"term":0}}`,
		},
		{
			name: "vote",
			in: election.RPCResponse{
				RPC: election.VoteRPC,
				Response: election.VoteResp{
					Term:    1,
					Granted: true,
				},
			},
			out: `{"rpc":"vote","response":{"term":1,"granted":true}}`,
		},
		{
			name: "vote-not-granted",
			in: election.RPCResponse{
				RPC: election.VoteRPC,
				Response: election.VoteResp{
					Granted: false,
					Term:    0,
				},
			},
			out: `{"rpc":"vote","response":{"term":0,"granted":false}}`,
		},
		{
			name: "reset",
			in: election.RPCResponse{
				RPC:      election.ResetElectionRPC,
				Response: election.ResetElectionResp{},
			},
			out: `{"rpc":"reset_election","response":{}}`,
		},
		{
			name: "resign",
			in: election.RPCResponse{
				RPC: election.ResignRPC,
				Response: election.ResignResp{
					Success: true,
				},
			},
			out: `{"rpc":"resign","response":{"success":true}}`,
		},
		{
			name: "set-peers",
			in: election.RPCResponse{
				RPC:      election.SetPeersRPC,
				Response: election.SetPeersResp{},
			},
			out: `{"rpc":"set_peers","response":{}}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.out, string(b))

			var in election.RPCResponse
			err = json.Unmarshal(b, &in)
			require.NoError(t, err)
			assert.Equal(t, tt.in, in)
		})
	}
}

func TestHTTPServer(t *testing.T) {
	ctx := context.Background()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in election.RPCRequest

		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(b, &in))

		var resp election.RPCResponse
		switch in.RPC {
		case election.HeartBeatRPC:
			resp = election.RPCResponse{
				RPC: election.HeartBeatRPC,
				Response: election.HeartBeatResp{
					Term: 10,
				},
			}
		default:
			resp = election.RPCResponse{
				Error: fmt.Sprintf("invalid rpc request '%s'", in.RPC),
			}
		}
		out, err := json.Marshal(resp)
		require.NoError(t, err)
		_, err = w.Write(out)
		require.NoError(t, err)
	}))
	defer ts.Close()

	// Marshall our request
	b, err := json.Marshal(election.RPCRequest{
		RPC: election.HeartBeatRPC,
		Request: election.HeartBeatReq{
			Term:   10,
			Leader: "node10",
		},
	})
	require.NoError(t, err)

	// Send the request to the server
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL, bytes.NewBuffer(b))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		err := resp.Body.Close()
		require.NoError(t, err)
	}()

	// Unmarshall the response
	var rpcResp election.RPCResponse
	b, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(b, &rpcResp)
	require.NoError(t, err)

	// Should have the response we expect
	hb := rpcResp.Response.(election.HeartBeatResp)
	assert.Equal(t, uint64(10), hb.Term)

	// Send an unknown rpc request to the server
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, ts.URL, bytes.NewBuffer([]byte(`{"rpc":"unknown"}`)))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		err := resp.Body.Close()
		require.NoError(t, err)
	}()

	// Unmarshall the response
	b, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(b, &rpcResp)
	require.NoError(t, err)

	// Should have the response we expect
	assert.Equal(t, "invalid rpc request 'unknown'", rpcResp.Error)
}
