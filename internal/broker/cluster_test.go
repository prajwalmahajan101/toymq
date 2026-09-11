package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/replication"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	filestorage "github.com/prajwalmahajan101/toyraft/pkg/storage/file"
)

// clusterNode is one member of an in-process test cluster: a broker, its
// BrokerSM-driven raft node, and the node's id. part is the test-only partition
// seam wrapping this node's transport (see cluster_partition_test.go).
type clusterNode struct {
	id     string
	broker *Broker
	node   raft.Node
	part   *partitionableTransport
}

// freeLoopbackAddr returns a currently-free 127.0.0.1 TCP address. Standard
// racy allocation — bind :0, read the addr, release it — because toyraft's http
// transport binds via ListenAndServe and gives no way to read back a :0 port.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// newCluster stands up an n-node in-process cluster: n brokers, each with a
// BrokerSM over the async-wrapped http transport on a distinct loopback port,
// the full membership as Peers, and its own broker + raft dirs. Real election
// runs over loopback. Cleanup stops every node and closes every broker.
func newCluster(t *testing.T, n int) []*clusterNode {
	t.Helper()

	ids := make([]string, n)
	addrs := make(map[string]string, n) // id -> host:port
	urls := make(map[string]string, n)  // id -> http://host:port
	for i := range n {
		id := fmt.Sprintf("n%d", i+1)
		ids[i] = id
		addr := freeLoopbackAddr(t)
		addrs[id] = addr
		urls[id] = "http://" + addr
	}
	peers := make([]raft.NodeID, n)
	for i, id := range ids {
		peers[i] = raft.NodeID(id)
	}

	nodes := make([]*clusterNode, 0, n)
	for i, id := range ids {
		base := t.TempDir()
		b, err := NewWithTimings(base, 16, 100*time.Millisecond, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("broker %s: %v", id, err)
		}

		peerURLs := make(map[string]string, n-1) // exclude self
		for pid, u := range urls {
			if pid != id {
				peerURLs[pid] = u
			}
		}
		inner, err := replication.NewHTTPTransport(id, addrs[id], peerURLs)
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		transport := newPartitionableTransport(inner)
		store, err := filestorage.New(filepath.Join(base, "raft"))
		if err != nil {
			t.Fatalf("storage %s: %v", id, err)
		}
		node, err := raft.New(raft.Config{
			NodeID:       raft.NodeID(id),
			Peers:        peers,
			Storage:      store,
			Transport:    transport,
			StateMachine: replication.NewBrokerSM(b),
			Seed:         int64(i + 1),
			// Real-time election over http loopback runs under -race on shared
			// CI runners where goroutine starvation easily exceeds the 150ms
			// default election timeout, causing spurious re-elections and
			// leadership churn. Widen the timeouts (keeping HeartbeatInterval*3
			// <= ElectionTimeoutMin) so leadership stays stable under load.
			HeartbeatInterval:  200 * time.Millisecond,
			ElectionTimeoutMin: 1 * time.Second,
			ElectionTimeoutMax: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("raft.New %s: %v", id, err)
		}
		if err := node.Start(context.Background()); err != nil {
			t.Fatalf("node.Start %s: %v", id, err)
		}
		b.AttachRaft(node)

		cn := &clusterNode{id: id, broker: b, node: node, part: transport}
		nodes = append(nodes, cn)
		t.Cleanup(func() {
			_ = node.Stop()
			_ = b.Close()
		})
	}
	return nodes
}

// waitLeader polls until exactly one reachable node reports raft.Leader,
// skipping any node in exclude (a killed leader). Returns that node.
func waitLeader(t *testing.T, nodes []*clusterNode, exclude *clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, cn := range nodes {
			if cn == exclude {
				continue
			}
			if cn.node.Status().Role == raft.Leader {
				return cn
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

// waitConverged polls until every node in want holds the same, non-zero
// partition head for topic/partition — i.e. every follower has applied up to
// wantHead. Reads go through Head(), which is mutex-guarded, so probing races
// the applier goroutine safely under -race.
func waitConverged(t *testing.T, want []*clusterNode, topic string, partition int, wantHead uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		allMatch := true
		last = ""
		for _, cn := range want {
			p, err := cn.broker.partitionAt(topic, partition)
			if err != nil {
				t.Fatalf("partitionAt on %s: %v", cn.id, err)
			}
			h := p.head()
			last += fmt.Sprintf("%s=%d ", cn.id, h)
			if h != wantHead {
				allMatch = false
			}
		}
		if allMatch {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge to head=%d within %s (last: %s)", wantHead, timeout, last)
}

// publishOnLeader publishes one message, re-resolving the current leader and
// retrying until it succeeds or the deadline passes. It tolerates transient
// leadership churn: a Publish that returns a not-leader rejection means the
// entry was not applied (toyraft's Propose applies before returning), so
// re-resolving the leader and retrying is safe and cannot double-publish.
func publishOnLeader(t *testing.T, nodes []*clusterNode, exclude *clusterNode, payload []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := waitLeader(t, nodes, exclude, 15*time.Second)
		_, _, _, err := leader.broker.Publish("orders", "", "", 0, false, payload)
		if err == nil {
			return
		}
		if _, isNotLeader := leader.broker.NotLeaderHint(err); !isNotLeader {
			t.Fatalf("publish on leader %s: %v", leader.id, err)
		}
		time.Sleep(50 * time.Millisecond) // leadership churned mid-write; retry
	}
	t.Fatalf("could not publish via a stable leader within %s", timeout)
}

// isClusterMember reports whether id names a node in the cluster.
func isClusterMember(nodes []*clusterNode, id string) bool {
	for _, cn := range nodes {
		if cn.id == id {
			return true
		}
	}
	return false
}

// TestClusterReplicatesToAllFollowers proves the distributed core: a
// leader-acked PUB lands on every follower's broker state via Propose→Apply.
func TestClusterReplicatesToAllFollowers(t *testing.T) {
	nodes := newCluster(t, 3)

	const k = 5
	for range k {
		publishOnLeader(t, nodes, nil, []byte("m"), 15*time.Second)
	}

	// k successful publishes → highest assigned id k-1 on every node once all
	// are applied. (MsgID contiguity itself is the M1 single-node guarantee;
	// M2a owns replication: the same head reaching every follower.)
	waitConverged(t, nodes, "orders", 0, k-1, 15*time.Second)
}

// TestClusterFollowerRejectsWrite proves the leader-gate: a write proposed on a
// follower returns *raft.ErrNotLeader carrying a leader hint, which the server
// surfaces as NOTLEADER (mapped in session.go).
func TestClusterFollowerRejectsWrite(t *testing.T) {
	nodes := newCluster(t, 3)

	// Poll until a follower rejects a write as not-leader with a resolved hint
	// naming a cluster member. Re-resolves the leader/follower each attempt so a
	// rare mid-test re-election cannot wedge the test.
	deadline := time.Now().Add(15 * time.Second)
	for {
		leader := waitLeader(t, nodes, nil, 15*time.Second)
		var follower *clusterNode
		for _, cn := range nodes {
			if cn != leader {
				follower = cn
				break
			}
		}

		_, _, _, err := follower.broker.Publish("orders", "", "", 0, false, []byte("m"))
		if err == nil {
			// follower just became leader in a churn; retry the resolution.
			if time.Now().After(deadline) {
				t.Fatal("follower kept accepting writes; no stable follower to reject")
			}
			continue
		}
		var nl *raft.ErrNotLeader
		if !errors.As(err, &nl) {
			t.Fatalf("follower error = %v; want *raft.ErrNotLeader", err)
		}
		hint, ok := follower.broker.NotLeaderHint(err)
		if !ok {
			t.Fatalf("NotLeaderHint did not recognize %v as a not-leader rejection", err)
		}
		if hint != "" {
			// Hint is best-effort; when set it must name a real cluster member.
			if !isClusterMember(nodes, hint) {
				t.Fatalf("leader hint = %q; not a cluster member", hint)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never resolved a leader hint within 15s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestClusterSurvivesLeaderKill proves failover with zero acked-write loss:
// after replicating writes, killing the leader elects a new one among the
// survivors, the acked writes remain, and the new leader accepts fresh writes
// that replicate to the other survivor.
func TestClusterSurvivesLeaderKill(t *testing.T) {
	nodes := newCluster(t, 3)

	const k = 3
	for range k {
		publishOnLeader(t, nodes, nil, []byte("m"), 15*time.Second)
	}
	waitConverged(t, nodes, "orders", 0, k-1, 15*time.Second)

	// Kill the current leader.
	leader := waitLeader(t, nodes, nil, 15*time.Second)
	if err := leader.node.Stop(); err != nil {
		t.Fatalf("stop leader %s: %v", leader.id, err)
	}
	survivors := make([]*clusterNode, 0, 2)
	for _, cn := range nodes {
		if cn != leader {
			survivors = append(survivors, cn)
		}
	}

	// A new leader must emerge among the two survivors (quorum of 3 = 2), and
	// the pre-kill acked writes survive on both.
	waitLeader(t, nodes, leader, 15*time.Second)
	waitConverged(t, survivors, "orders", 0, k-1, 15*time.Second)

	// The new leader accepts a fresh write that replicates to the other
	// survivor; head advances to k.
	publishOnLeader(t, nodes, leader, []byte("after-kill"), 15*time.Second)
	waitConverged(t, survivors, "orders", 0, k, 15*time.Second)
}
