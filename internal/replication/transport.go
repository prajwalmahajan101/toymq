package replication

import (
	"context"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
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
