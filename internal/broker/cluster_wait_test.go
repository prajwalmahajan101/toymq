package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// waitPublish publishes one message on leader with a WAIT <n> <timeout>
// barrier and returns the error (nil = barrier met). The write itself commits
// via quorum before the barrier is evaluated, so a non-nil error is the
// barrier verdict, not a publish failure.
func waitPublish(leader *clusterNode, waitReplicas int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _, _, err := leader.broker.PublishCtx(ctx, "orders", "", "", 0, false, []byte("m"), 0, waitReplicas, timeout)
	return err
}

// TestPublishWaitSatisfied: on a healthy 3-node cluster a WAIT 2 barrier (both
// followers) is met — both followers replicate the entry, so MatchIndex reaches
// the write's index on both within the timeout.
func TestPublishWaitSatisfied(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitLeader(t, nodes, nil, 15*time.Second)
	if err := waitPublish(leader, 2, 5*time.Second); err != nil {
		t.Fatalf("WAIT 2 on a healthy 3-node cluster should be satisfied, got %v", err)
	}
}

// TestPublishWaitTimeoutPartitionedFollower is the owned-risk test (ADR 0033):
// with one follower partitioned off, the leader + reachable follower still form
// a quorum so the write commits, WAIT 1 is satisfied by the reachable follower,
// but WAIT 2 times out — the barrier does NOT over-count a follower that never
// acked. It returns ErrWaitTimeout, never a false success.
func TestPublishWaitTimeoutPartitionedFollower(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitLeader(t, nodes, nil, 15*time.Second)

	// Split one follower off from everyone; leader + the other follower remain a
	// quorum of 2 (of 3), so the leader keeps leadership and commits.
	var isolated, reachable *clusterNode
	for _, cn := range nodes {
		if cn == leader {
			continue
		}
		if isolated == nil {
			isolated = cn
		} else {
			reachable = cn
		}
	}
	partition([]*clusterNode{isolated}, []*clusterNode{leader, reachable})

	// WAIT 1 is met by the one reachable follower.
	if err := waitPublish(leader, 1, 5*time.Second); err != nil {
		t.Fatalf("WAIT 1 with one reachable follower should be satisfied, got %v", err)
	}

	// WAIT 2 cannot be met — the isolated follower never advances MatchIndex, so
	// the barrier must time out rather than over-count.
	err := waitPublish(leader, 2, 1*time.Second)
	if !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("WAIT 2 with a partitioned follower should time out, got %v", err)
	}
}

// TestReplicationStatusLeader: on a healthy 3-node cluster the leader's
// ReplicationStatus reports role "leader", both followers as peers, and a
// non-negative lag matching live state (v3 M4, ADR 0033).
func TestReplicationStatusLeader(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitLeader(t, nodes, nil, 15*time.Second)
	// Publish and let followers converge so MatchIndex is populated.
	publishOnLeader(t, nodes, nil, []byte("m"), 15*time.Second)
	waitConverged(t, nodes, "orders", 0, 0, 15*time.Second)

	st := leader.broker.ReplicationStatus()
	if st.Role != "leader" {
		t.Fatalf("leader role = %q, want leader", st.Role)
	}
	if len(st.Peers) != 2 {
		t.Fatalf("leader should see 2 peers, got %d: %+v", len(st.Peers), st.Peers)
	}
	if st.LastLogIndex == 0 {
		t.Fatalf("leader LastLogIndex should be non-zero after a publish")
	}
	for _, p := range st.Peers {
		if p.MatchIndex > st.LastLogIndex {
			t.Fatalf("peer %s MatchIndex %d exceeds LastLogIndex %d", p.ID, p.MatchIndex, st.LastLogIndex)
		}
	}

	// A follower reports role "follower" and no peers (MatchIndex is leader-only).
	for _, cn := range nodes {
		if cn == leader {
			continue
		}
		fs := cn.broker.ReplicationStatus()
		if fs.Role != "follower" {
			t.Fatalf("follower %s role = %q, want follower", cn.id, fs.Role)
		}
		if len(fs.Peers) != 0 {
			t.Fatalf("follower %s should report no peers, got %d", cn.id, len(fs.Peers))
		}
		break
	}
}

// TestPublishWaitStandalone: a standalone broker (no raft) has no followers, so
// any WAIT barrier is a no-op — the publish succeeds immediately.
func TestPublishWaitStandalone(t *testing.T) {
	b, err := New(t.TempDir(), 16)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if _, _, _, err := b.PublishCtx(context.Background(), "orders", "", "", 0, false, []byte("m"), 0, 3, time.Second); err != nil {
		t.Fatalf("standalone WAIT should be a no-op, got %v", err)
	}
}
