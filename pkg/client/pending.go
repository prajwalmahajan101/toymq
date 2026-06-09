package client

import "sync"

// pendingReq is one in-flight request awaiting OK/DUP/ERR.
type pendingReq struct {
	resp      chan frame
	cancelled bool
}

// pendingQueue is a FIFO of in-flight requests. The wire protocol
// guarantees responses arrive in request order, so head-of-queue
// routing is sufficient.
type pendingQueue struct {
	mu    sync.Mutex
	queue []*pendingReq
}

func newPendingQueue() *pendingQueue {
	return &pendingQueue{}
}

func (q *pendingQueue) push() *pendingReq {
	p := &pendingReq{resp: make(chan frame, 1)}
	q.mu.Lock()
	q.queue = append(q.queue, p)
	q.mu.Unlock()
	return p
}

// deliver hands the next live request the frame. Skips entries that
// have been cancelled. Returns false if the queue had no live entry.
func (q *pendingQueue) deliver(f frame) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.queue) > 0 {
		head := q.queue[0]
		q.queue = q.queue[1:]
		if !head.cancelled {
			head.resp <- f
			return true
		}
	}
	return false
}

// cancel marks p as cancelled so deliver will skip it.
func (q *pendingQueue) cancel(p *pendingReq) {
	q.mu.Lock()
	p.cancelled = true
	q.mu.Unlock()
}

// drainErr resolves every live request with the given error frame.
// Used on transport failure / Close.
func (q *pendingQueue) drainErr(f frame) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, p := range q.queue {
		if !p.cancelled {
			p.resp <- f
		}
	}
	q.queue = nil
}
