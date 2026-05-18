package prod

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/kapetan-io/election.go/internal/engine"
)

// SendRPCFunc is the function type for sending an RPC to a peer.
// It mirrors election.SendRPCFunc without importing the election package.
type SendRPCFunc func(ctx context.Context, peer string, req engine.RPCRequest) (engine.RPCResponse, error)

// Config holds the subset of election.Config needed by the production IO adapter.
type Config struct {
	SendRPC        SendRPCFunc
	NetworkTimeout time.Duration
	Log            *slog.Logger
}

// Completion pairs a gocoro callback with the response that satisfies it.
type Completion struct {
	callback func(engine.Resp, error)
	resp     engine.Resp
}

// ProdIO implements io.IO[engine.Req, engine.Resp] for production use.
// It drives the coroutine state machine using real goroutines, wall-clock
// timers, and channels.
type ProdIO struct {
	conf       Config
	eventCh    chan engine.Event
	completeCh chan Completion
	timers     map[int64]*time.Timer
	nextTimer  int64
	stopped    atomic.Bool
	log        *slog.Logger
}

// NewProdIO creates a new production IO adapter.
func NewProdIO(conf Config, eventCh chan engine.Event, completeCh chan Completion) *ProdIO {
	return &ProdIO{
		conf:       conf,
		eventCh:    eventCh,
		completeCh: completeCh,
		timers:     make(map[int64]*time.Timer),
		log:        conf.Log,
	}
}

// SetStopped marks the IO adapter as stopped. After this, timer AfterFunc
// callbacks will not write to eventCh, preventing a send-to-closed-channel
// panic on the forced-shutdown path.
func (p *ProdIO) SetStopped() {
	p.stopped.Store(true)
}

// Dispatch implements io.IO[engine.Req, engine.Resp]. It is called by the
// gocoro scheduler whenever a coroutine yields a request. The callback
// resolves the coroutine's promise when the operation completes.
func (p *ProdIO) Dispatch(req engine.Req, callback func(engine.Resp, error)) {
	switch req.Kind {
	case engine.SendRPC:
		// Async: launch a goroutine that calls the user-provided SendRPC function
		// and pushes the completion result to completeCh.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), p.conf.NetworkTimeout)
			defer cancel()
			rpcResp, err := p.conf.SendRPC(ctx, req.Peer, req.RPCReq)
			var resp engine.Resp
			if err != nil {
				resp = engine.Resp{Kind: engine.SendRPC, Err: err}
			} else {
				resp = engine.Resp{Kind: engine.SendRPC, RPCResp: rpcResp}
			}
			p.completeCh <- Completion{callback: callback, resp: resp}
		}()

	case engine.Recv:
		// Async: launch a goroutine that reads the next event from eventCh
		// and pushes the completion to completeCh.
		go func() {
			event, ok := <-p.eventCh
			if !ok {
				// eventCh was closed (forced shutdown path) — deliver shutdown
				p.completeCh <- Completion{
					callback: callback,
					resp: engine.Resp{
						Kind:  engine.Recv,
						Event: engine.Event{Kind: engine.EventShutdown},
					},
				}
				return
			}
			p.completeCh <- Completion{
				callback: callback,
				resp:     engine.Resp{Kind: engine.Recv, Event: event},
			}
		}()

	case engine.After:
		// Sync: assign a TimerID, register a time.AfterFunc, and resolve
		// the callback immediately with the TimerID. The timer fires later
		// by pushing an EventTimer to eventCh.
		p.nextTimer++
		timerID := p.nextTimer
		timer := time.AfterFunc(req.Delay, func() {
			if p.stopped.Load() {
				return
			}
			p.eventCh <- engine.Event{
				Kind:    engine.EventTimer,
				TimerID: timerID,
			}
		})
		p.timers[timerID] = timer
		callback(engine.Resp{Kind: engine.After, TimerID: timerID}, nil)

	case engine.Cancel:
		// Sync: stop the timer and remove it from the map. If it already
		// fired or was already cancelled, this is a no-op.
		if t, ok := p.timers[req.TimerID]; ok {
			t.Stop()
			delete(p.timers, req.TimerID)
		}
		callback(engine.Resp{Kind: engine.Cancel}, nil)

	case engine.Now:
		// Sync: resolve with the current wall-clock time.
		callback(engine.Resp{Kind: engine.Now, Time: time.Now()}, nil)
	}
}
