package engine

import "time"

// Kind identifies the type of IO request a coroutine is making
type Kind int

const (
	SendRPC Kind = iota + 1
	Recv
	After
	Cancel
	Now
)

// Req is a request from the coroutine to the IO scheduler
type Req struct {
	Kind    Kind
	Peer    string
	RPCReq  RPCRequest
	Delay   time.Duration
	TimerID int64
}

// Resp is a response from the IO scheduler to the coroutine
type Resp struct {
	Kind    Kind
	RPCResp RPCResponse
	Err     error
	Event   Event
	TimerID int64
	Time    time.Time
}

// EventKind identifies the type of event delivered to the coroutine
type EventKind int

const (
	EventRPC      EventKind = iota + 1
	EventTimer
	EventSetPeers
	EventResign
	EventShutdown
)

// Event carries inbound messages delivered via Recv
type Event struct {
	Kind    EventKind
	RPCReq  RPCRequest
	TimerID int64
	Peers   []string
	Respond func(RPCResponse)
	Done    func(error)
}
