package engine

import "encoding/json"

// RPC is the type of RPC request
type RPC string

const (
	HeartBeatRPC     RPC = "heartbeat"
	VoteRPC          RPC = "vote"
	ResetElectionRPC RPC = "reset_election"
	ResignRPC        RPC = "resign"
	SetPeersRPC      RPC = "set_peers"
	SetMetadataRPC   RPC = "set_metadata"
)

// Peer represents a node in the election cluster
type Peer struct {
	Address  string `json:"address"`
	Metadata []byte `json:"metadata,omitempty"`
}

// rpcPayload is the JSON envelope for RPC requests and responses
type rpcPayload struct {
	RPC      RPC             `json:"rpc"`
	Request  json.RawMessage `json:"request,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// RPCRequest is a request to a peer
type RPCRequest struct {
	RPC     RPC
	Request any
}

func (r RPCRequest) MarshalJSON() ([]byte, error) {
	out, err := json.Marshal(r.Request)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rpcPayload{
		RPC:     r.RPC,
		Request: out,
	})
}

func (r *RPCRequest) UnmarshalJSON(s []byte) error {
	var in rpcPayload
	if err := json.Unmarshal(s, &in); err != nil {
		return err
	}
	r.RPC = in.RPC

	switch in.RPC {
	case HeartBeatRPC:
		var req HeartBeatReq
		if err := json.Unmarshal(in.Request, &req); err != nil {
			return err
		}
		r.Request = req
	case VoteRPC:
		var req VoteReq
		if err := json.Unmarshal(in.Request, &req); err != nil {
			return err
		}
		r.Request = req
	case ResetElectionRPC:
		var req ResetElectionReq
		if err := json.Unmarshal(in.Request, &req); err != nil {
			return err
		}
		r.Request = req
	case ResignRPC:
		var req ResignReq
		if err := json.Unmarshal(in.Request, &req); err != nil {
			return err
		}
		r.Request = req
	case SetPeersRPC:
		var req SetPeersReq
		if err := json.Unmarshal(in.Request, &req); err != nil {
			return err
		}
		r.Request = req
	case SetMetadataRPC:
		var req SetMetadataReq
		if err := json.Unmarshal(in.Request, &req); err != nil {
			return err
		}
		r.Request = req
	}
	return nil
}

// RPCResponse is a response from a peer
type RPCResponse struct {
	RPC      RPC
	Response any
	Error    string
}

func (r RPCResponse) MarshalJSON() ([]byte, error) {
	out, err := json.Marshal(r.Response)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rpcPayload{
		RPC:      r.RPC,
		Response: out,
		Error:    r.Error,
	})
}

func (r *RPCResponse) UnmarshalJSON(s []byte) error {
	var in rpcPayload
	if err := json.Unmarshal(s, &in); err != nil {
		return err
	}
	r.Error = in.Error
	r.RPC = in.RPC

	switch in.RPC {
	case HeartBeatRPC:
		var resp HeartBeatResp
		if err := json.Unmarshal(in.Response, &resp); err != nil {
			return err
		}
		r.Response = resp
	case VoteRPC:
		var resp VoteResp
		if err := json.Unmarshal(in.Response, &resp); err != nil {
			return err
		}
		r.Response = resp
	case ResetElectionRPC:
		var resp ResetElectionResp
		if err := json.Unmarshal(in.Response, &resp); err != nil {
			return err
		}
		r.Response = resp
	case ResignRPC:
		var resp ResignResp
		if err := json.Unmarshal(in.Response, &resp); err != nil {
			return err
		}
		r.Response = resp
	case SetPeersRPC:
		var resp SetPeersResp
		if err := json.Unmarshal(in.Response, &resp); err != nil {
			return err
		}
		r.Response = resp
	case SetMetadataRPC:
		var resp SetMetadataResp
		if err := json.Unmarshal(in.Response, &resp); err != nil {
			return err
		}
		r.Response = resp
	}
	return nil
}

// VoteReq is sent to all peers at the start of an election
type VoteReq struct {
	Term      uint64 `json:"term"`
	Candidate string `json:"candidate"`
}

// VoteResp is the response to a VoteReq
type VoteResp struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

// HeartBeatReq is sent by the leader to all followers
type HeartBeatReq struct {
	Term     uint64 `json:"term"`
	Leader   string `json:"leader"`
	Metadata []byte `json:"metadata,omitempty"`
}

// HeartBeatResp is the response to a HeartBeatReq
type HeartBeatResp struct {
	Term     uint64 `json:"term"`
	Metadata []byte `json:"metadata,omitempty"`
}

// ResetElectionReq resets the current state of a node to 'candidate'
type ResetElectionReq struct{}

// ResetElectionResp is the response to a ResetElectionReq
type ResetElectionResp struct{}

// ResignReq asks the node to resign as leader
type ResignReq struct{}

// ResignResp is the response to a ResignReq
type ResignResp struct {
	Success bool `json:"success"`
}

// SetPeersReq sets the peers this node will consider during the election
type SetPeersReq struct {
	Peers []string `json:"peers"`
}

// SetPeersResp is the response to a SetPeersReq
type SetPeersResp struct{}

// SetMetadataReq sets the metadata blob for this node
type SetMetadataReq struct {
	Metadata []byte `json:"metadata"`
}

// SetMetadataResp is the response to a SetMetadataReq
type SetMetadataResp struct{}
