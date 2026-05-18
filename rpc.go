package election

import "encoding/json"

// RPCPayload is the JSON wire envelope for RPC requests and responses.
// MarshalJSON and UnmarshalJSON on RPCRequest and RPCResponse use this format.
type RPCPayload struct {
	RPC      RPC             `json:"rpc"`
	Request  json.RawMessage `json:"request,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
	Error    string          `json:"error,omitempty"`
}
