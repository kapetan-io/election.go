package prod

import (
	"time"

	gocoro "github.com/resonatehq/gocoro"
	"github.com/kapetan-io/election.go/internal/engine"
)

// Run drives the gocoro scheduler until all coroutines have finished.
// When the scheduler empties, doneCh is closed to signal the caller.
//
// The main loop:
//  1. Call RunUntilBlocked — runs coroutines until all are blocked on IO
//  2. If no coroutines remain, exit
//  3. Wait for at least one completion on completeCh
//  4. Invoke all ready completion callbacks (which resolve promises)
//  5. Repeat
func Run(sched gocoro.Scheduler[engine.Req, engine.Resp], completeCh chan Completion, doneCh chan struct{}) {
	defer close(doneCh)

	for sched.Size() > 0 {
		sched.RunUntilBlocked(time.Now().UnixNano())

		// After RunUntilBlocked all coroutines are either done or blocked on
		// async IO. If none remain, the loop is done.
		if sched.Size() == 0 {
			return
		}

		// Wait for at least one completion, then drain any additional ones
		// that are already ready (batch processing for efficiency).
		c, ok := <-completeCh
		if !ok {
			return
		}
		c.callback(c.resp, nil)

		// Drain any additional completions that are immediately available.
		for {
			select {
			case c, ok := <-completeCh:
				if !ok {
					return
				}
				c.callback(c.resp, nil)
			default:
				goto drained
			}
		}
	drained:
	}
}
