package replication

import (
	"sync"
	"sync/atomic"
)

// ApplyResult is what a mutating command's StateMachine.Apply produces that the
// proposing request handler needs back. Today only PUB needs it — the
// WAL-assigned MsgID and whether the write was a dedupe hit — because
// raft.Node.Propose returns (Index, Term, error) and discards Apply's any
// result (confirmed against toyraft rc.2; its reference kvsm exposes a
// separate Get for the same reason). ACK/NACK/CREATE need nothing back beyond
// Propose's own error, so they run with Nonce 0 and never register a waiter.
type ApplyResult struct {
	MsgID uint64
	Dup   bool
}

// ResultRegistry bridges a value produced inside StateMachine.Apply back to
// the leader's request handler waiting on Propose. The handler registers a
// nonce, Proposes an envelope stamped with it, and — because Propose blocks
// until Apply has run on this node — reads the result channel the instant
// Propose returns. Apply calls resolve with the same nonce.
//
// It is a no-op on any node that is not the proposing leader: a follower (or
// the leader replaying its log at startup) applies the same entry, calls
// resolve, finds no registered waiter, and drops the result. So Apply stays
// oblivious to whether a caller is listening.
type ResultRegistry struct {
	nonce   atomic.Uint64
	waiters sync.Map // uint64 -> chan ApplyResult
}

// NewResultRegistry returns an empty registry ready for use.
func NewResultRegistry() *ResultRegistry {
	return &ResultRegistry{}
}

// next returns a fresh, process-unique nonce. It starts at 1 so that 0 stays
// reserved as the "no waiter" sentinel carried by commands that don't need a
// result back (ACK/NACK/CREATE) — resolve(0, …) must never match a waiter.
func (r *ResultRegistry) next() uint64 {
	return r.nonce.Add(1)
}

// Register allocates a fresh nonce, installs a buffered (cap 1) result channel
// for it, and returns both. The buffer of 1 guarantees resolve never blocks
// even if the handler has not yet reached its receive — the value is parked in
// the channel until the handler reads it. The caller must Propose an envelope
// carrying this nonce and then receive from the channel; on a Propose error it
// must Forget the nonce instead (Apply never ran, so nothing will resolve it).
func (r *ResultRegistry) Register() (uint64, <-chan ApplyResult) {
	nonce := r.next()
	ch := make(chan ApplyResult, 1)
	r.waiters.Store(nonce, ch)
	return nonce, ch
}

// Forget drops a registered waiter without delivering a result. The leader's
// request handler calls it when Propose returns an error: the entry was never
// applied, so resolve will never fire for this nonce, and leaving it in the map
// would leak. Safe to call for an already-resolved or never-registered nonce.
func (r *ResultRegistry) Forget(nonce uint64) {
	r.waiters.Delete(nonce)
}

// resolve delivers res to the waiter registered under nonce and removes it. It
// is a no-op when nonce is 0 (a command that wants no result) or when no
// waiter is registered (a follower, or the leader replaying its log). Called
// once per applied entry from inside StateMachine.Apply.
func (r *ResultRegistry) resolve(nonce uint64, res ApplyResult) {
	if nonce == 0 {
		return
	}
	v, ok := r.waiters.LoadAndDelete(nonce)
	if !ok {
		return
	}
	// Send cannot block: the channel is buffered (cap 1) and a nonce is
	// resolved at most once (LoadAndDelete removed it).
	v.(chan ApplyResult) <- res
}
