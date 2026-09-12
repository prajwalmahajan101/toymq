package broker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/replication"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	filestorage "github.com/prajwalmahajan101/toyraft/pkg/storage/file"
)

// attachSingleNodeRaft wires b into a single-node (self-only) replicated
// cluster and blocks until it is leader. It uses replication's no-op transport
// because neither shipped toyraft transport can express a peers=[self] cluster
// (see transport.go / the migration report).
func attachSingleNodeRaft(t *testing.T, b *Broker, dir string) raft.Node {
	t.Helper()

	store, err := filestorage.New(filepath.Join(dir, "raft"))
	if err != nil {
		t.Fatalf("filestorage.New: %v", err)
	}
	sm := replication.NewBrokerSM(b)
	node, err := raft.New(raft.Config{
		NodeID:       "n1",
		Peers:        []raft.NodeID{"n1"}, // [self] → majority of 1, trivially leader
		Storage:      store,
		Transport:    replication.NewSingleNodeTransport(),
		StateMachine: sm,
		Seed:         1,
	})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("node.Start: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })

	b.AttachRaft(node, "n1")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if node.Status().Role == raft.Leader {
			return node
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node did not become leader within 3s")
	return nil
}

// TestReplicatedPublishRoundTrip proves the --replicate publish path: a PUB
// flows Propose→Apply and the WAL-assigned MsgID comes back through the nonce
// registry, monotonic and in order. This is the apply-once / MsgID-return
// guarantee at the single-node seam (ADR 0028).
func TestReplicatedPublishRoundTrip(t *testing.T) {
	dir := t.TempDir()
	b, err := NewWithTimings(dir, 16, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWithTimings: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	attachSingleNodeRaft(t, b, dir)

	for i := range 3 {
		want := uint64(i)
		id, _, dup, err := b.Publish("orders", "", "", 0, false, []byte("m"))
		if err != nil {
			t.Fatalf("Publish %d: %v", want, err)
		}
		if dup {
			t.Fatalf("Publish %d: unexpected dup", want)
		}
		if id != want {
			t.Fatalf("Publish %d: MsgID = %d, want %d (WAL-assigned, in order)", want, id, want)
		}
	}
}

// TestReplicatedPublishDedupe proves dedupe survives the replicated path: a
// second PUB with the same key returns the original MsgID and dup=true with no
// new WAL write, exactly as standalone.
func TestReplicatedPublishDedupe(t *testing.T) {
	dir := t.TempDir()
	b, err := NewWithTimings(dir, 16, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWithTimings: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	attachSingleNodeRaft(t, b, dir)

	id1, _, dup1, err := b.Publish("orders", "dk-1", "", 0, false, []byte("a"))
	if err != nil || dup1 {
		t.Fatalf("first publish: id=%d dup=%v err=%v", id1, dup1, err)
	}
	id2, _, dup2, err := b.Publish("orders", "dk-1", "", 0, false, []byte("a"))
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if !dup2 || id2 != id1 {
		t.Fatalf("dedupe replay: id=%d dup=%v, want id=%d dup=true", id2, dup2, id1)
	}
}

// TestReplicatedRestartRecovery is the M1 crash-durability guard for the
// idempotent-replay fix (ADR 0028). On restart toyraft replays its whole
// committed log through Apply (it keeps no persistent applied index and
// snapshots are stubbed), while the broker WAL also recovers independently.
// The RaftIndex stamped in each record makes that replay a no-op: a message
// published before the restart must survive exactly once, never doubled.
//
// toyraft rc.3 restores the in-memory log from Storage on New (B1), so a
// restarted node is writable again, and the RaftIndex guard (ADR 0028 §5) keeps
// the replay of the committed log a no-op. So a post-restart publish must
// succeed AND continue from the correct MsgID — the full crash-durability
// criterion that was blocked on rc.2.
func TestReplicatedRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	b, err := NewWithTimings(dir, 16, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWithTimings: %v", err)
	}
	node := attachSingleNodeRaft(t, b, dir)
	for i := range 3 {
		if _, _, _, err := b.Publish("orders", "", "", 0, false, []byte("m")); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	if err := node.Stop(); err != nil {
		t.Fatalf("node.Stop: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("broker.Close: %v", err)
	}

	// Restart: reopen the broker (recovers its WAL + the applied high-water)
	// and re-attach raft, which restores its log (B1) and replays the 3
	// committed entries through Apply (skipped by the RaftIndex guard).
	b2, err := NewWithTimings(dir, 16, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = b2.Close() })
	attachSingleNodeRaft(t, b2, dir)

	// The node is writable after restart (B1). A fresh publish must get MsgID
	// 3 — proving replay wrote nothing extra (double-apply would push it past
	// 3) and the node accepts new writes (rc.2 dropped them). Propose blocks
	// until applied, so no wait for replay drain is needed.
	id, _, dup, err := b2.Publish("orders", "", "", 0, false, []byte("after-restart"))
	if err != nil {
		t.Fatalf("post-restart Publish: %v", err)
	}
	if dup {
		t.Fatal("post-restart Publish unexpectedly deduped")
	}
	if id != 3 {
		t.Fatalf("post-restart MsgID = %d, want 3 (double-apply would exceed 3)", id)
	}
}

// TestReplicatedAckAdvancesOffset proves ACK is replicated: after a
// Propose→Apply ack, the consumer's durable offset advances just as it would
// inline.
func TestReplicatedAckAdvancesOffset(t *testing.T) {
	dir := t.TempDir()
	b, err := NewWithTimings(dir, 16, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWithTimings: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	attachSingleNodeRaft(t, b, dir)

	id, _, _, err := b.Publish("orders", "", "", 0, false, []byte("m"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Deliver it to the consumer's inflight so the ack has a target, then ack
	// through the replicated path.
	p, err := b.partitionAt("orders", 0)
	if err != nil {
		t.Fatalf("partitionAt: %v", err)
	}
	c := p.getOrCreateConsumer("c-1")
	c.mu.Lock()
	c.inflight[id] = &Inflight{MsgID: id, Topic: "orders", Payload: []byte("m")}
	c.mu.Unlock()
	if err := b.Ack("orders", 0, "c-1", id); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	c.mu.Lock()
	lastAcked := c.lastAcked
	c.mu.Unlock()
	if lastAcked != id {
		t.Fatalf("lastAcked = %d, want %d", lastAcked, id)
	}
}
