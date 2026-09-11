package replication

import (
	"context"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	httptransport "github.com/prajwalmahajan101/toyraft/pkg/transport/http"
)

// singleNodeTransport is a no-op raft.Transport for a self-only (single-node)
// cluster. Such a cluster has no peers, so the raft core never calls Send and
// no inbound message ever arrives to drive the registered step callback — the
// node reaches majority (of 1) and commits locally.
//
// It exists because neither shipped toyraft transport can build a single-node
// embedded cluster (v3 M1 finding, docs/TOYRAFT-MIGRATION-REPORT.md):
//   - pkg/transport/inproc: HubConfig.Clock is typed against toyraft's
//     internal/clock, which an external module cannot construct, and NewHub
//     hard-errors on a nil Clock.
//   - pkg/transport/http: Config.Validate rejects an empty PeerURLs and also
//     rejects this node's own ID appearing in PeerURLs — so a peers=[self]
//     cluster (PeerURLs excludes self → empty) cannot be expressed.
//
// M2 (multi-node) swaps this for pkg/transport/http with real peer URLs.
type singleNodeTransport struct{}

// NewSingleNodeTransport returns a raft.Transport suitable only for a
// single-node cluster (Peers == [self]).
func NewSingleNodeTransport() raft.Transport {
	return singleNodeTransport{}
}

// Send is never called in a single-node cluster (no peers); it drops the
// message and reports success, satisfying the best-effort contract.
func (singleNodeTransport) Send(context.Context, raft.Message) error {
	return nil
}

// Register records the inbound callback but never invokes it — no peer ever
// sends this node a message.
func (singleNodeTransport) Register(func(ctx context.Context, msg raft.Message) error) {
}

// Close is a no-op; there are no listeners or connections to release.
func (singleNodeTransport) Close() error {
	return nil
}

// Compile-time assertion that singleNodeTransport satisfies the interface.
var _ raft.Transport = singleNodeTransport{}

// asyncSendQueueDepth bounds each peer's pending-send buffer.
//
// ponytail: fixed per-peer depth with drop-on-full. raft's Send contract is
// best-effort (a dropped heartbeat/append is retransmitted on the next tick),
// so overflow is safe; bump this only if elections observably flap under load.
const asyncSendQueueDepth = 256

// asyncTransport decorates a raft.Transport so Send never blocks the caller.
//
// It exists because toyraft's driver calls Transport.Send synchronously in its
// single driver goroutine (pkg/raft/driver.go), and the http transport's Send
// is a blocking POST (retries + SendTimeout). Without this wrapper one dead or
// slow peer stalls the whole driver — no ticks, no commits, no election — a
// liveness failure. Each peer gets its own buffered queue + pump goroutine, so
// a stuck peer only fills (then drops from) its own queue and never blocks the
// driver or the other peers' delivery. (v3 M2 finding, TOYRAFT-MIGRATION-REPORT.)
type asyncTransport struct {
	inner  raft.Transport
	queues map[raft.NodeID]chan raft.Message
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newAsyncTransport(inner raft.Transport, peers []raft.NodeID) *asyncTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &asyncTransport{
		inner:  inner,
		queues: make(map[raft.NodeID]chan raft.Message, len(peers)),
		ctx:    ctx,
		cancel: cancel,
	}
	for _, p := range peers {
		ch := make(chan raft.Message, asyncSendQueueDepth)
		t.queues[p] = ch
		t.wg.Add(1)
		go t.pump(ch)
	}
	return t
}

// pump drains one peer's queue, performing the real (blocking) inner Send. Send
// errors are the inner transport's to log (best-effort); we discard them.
func (t *asyncTransport) pump(ch chan raft.Message) {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg := <-ch:
			_ = t.inner.Send(t.ctx, msg)
		}
	}
}

// Send enqueues msg on its peer's queue and returns immediately. A full queue
// drops the message (best-effort contract). An unknown peer falls through to
// the inner transport, which reports it.
func (t *asyncTransport) Send(_ context.Context, msg raft.Message) error {
	ch, ok := t.queues[msg.To]
	if !ok {
		return t.inner.Send(t.ctx, msg)
	}
	select {
	case ch <- msg:
	default:
		// queue full — drop; raft retransmits on the next heartbeat.
	}
	return nil
}

func (t *asyncTransport) Register(step func(ctx context.Context, msg raft.Message) error) {
	t.inner.Register(step)
}

// Close stops the pumps (cancelling any in-flight inner Send) and closes the
// inner transport.
func (t *asyncTransport) Close() error {
	t.cancel()
	t.wg.Wait()
	return t.inner.Close()
}

var _ raft.Transport = (*asyncTransport)(nil)

// NewHTTPTransport builds the multi-node peer transport: toyraft's http
// transport (this node's ListenAddr + the peer NodeID→baseURL table, which
// must exclude self) wrapped in asyncTransport for driver liveness. peerURLs
// keys are the peer NodeIDs as strings; cmd/config keeps them string-typed to
// stay free of a toyraft import.
func NewHTTPTransport(nodeID, listenAddr string, peerURLs map[string]string) (raft.Transport, error) {
	urls := make(map[raft.NodeID]string, len(peerURLs))
	peerIDs := make([]raft.NodeID, 0, len(peerURLs))
	for id, u := range peerURLs {
		nid := raft.NodeID(id)
		urls[nid] = u
		peerIDs = append(peerIDs, nid)
	}
	inner, err := httptransport.New(httptransport.Config{
		NodeID:     raft.NodeID(nodeID),
		ListenAddr: listenAddr,
		PeerURLs:   urls,
	})
	if err != nil {
		return nil, err
	}
	return newAsyncTransport(inner, peerIDs), nil
}
