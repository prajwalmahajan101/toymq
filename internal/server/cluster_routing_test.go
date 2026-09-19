package server_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/replication"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/pkg/client"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	filestorage "github.com/prajwalmahajan101/toyraft/pkg/storage/file"
)

// routeNode is one member of the in-process routing cluster: a broker, its
// raft node, and a client-facing TCP server. member is the "id@clientAddr"
// seed form a ClusterClient consumes.
type routeNode struct {
	id         string
	broker     *broker.Broker
	node       raft.Node
	srv        *server.Server
	clientAddr string
}

func (n *routeNode) member() string { return n.id + "@" + n.clientAddr }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// newRoutedCluster stands up an n-node cluster (broker + raft over http
// loopback + client TCP server per node) and returns the nodes once every
// client listener is accepting. Mirrors the internal/broker cluster harness
// plus a server.New front door (v3 M3, ADR 0032).
func newRoutedCluster(t *testing.T, n int) []*routeNode {
	t.Helper()

	ids := make([]string, n)
	raftAddrs := make(map[string]string, n)
	urls := make(map[string]string, n)
	for i := range n {
		id := fmt.Sprintf("n%d", i+1)
		ids[i] = id
		a := freeAddr(t)
		raftAddrs[id] = a
		urls[id] = "http://" + a
	}
	peers := make([]raft.NodeID, n)
	for i, id := range ids {
		peers[i] = raft.NodeID(id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	nodes := make([]*routeNode, 0, n)
	for i, id := range ids {
		base := t.TempDir()
		b, err := broker.NewWithTimings(base, 16, 100*time.Millisecond, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("broker %s: %v", id, err)
		}
		peerURLs := make(map[string]string, n-1)
		for pid, u := range urls {
			if pid != id {
				peerURLs[pid] = u
			}
		}
		transport, err := replication.NewHTTPTransport(id, raftAddrs[id], peerURLs)
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		store, err := filestorage.New(filepath.Join(base, "raft"))
		if err != nil {
			t.Fatalf("storage %s: %v", id, err)
		}
		node, err := raft.New(raft.Config{
			NodeID:             raft.NodeID(id),
			Peers:              peers,
			Storage:            store,
			Transport:          transport,
			StateMachine:       replication.NewBrokerSM(b),
			Seed:               int64(i + 1),
			HeartbeatInterval:  200 * time.Millisecond,
			ElectionTimeoutMin: 1 * time.Second,
			ElectionTimeoutMax: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("raft.New %s: %v", id, err)
		}
		if err := node.Start(ctx); err != nil {
			t.Fatalf("node.Start %s: %v", id, err)
		}
		b.AttachRaft(node)

		srv := server.New("127.0.0.1:0", b)
		go func() { _ = srv.Serve(ctx) }()

		rn := &routeNode{id: id, broker: b, node: node, srv: srv}
		nodes = append(nodes, rn)
		t.Cleanup(func() {
			sctx, sc := context.WithTimeout(context.Background(), 2*time.Second)
			_ = srv.Shutdown(sctx)
			sc()
			_ = node.Stop()
			_ = b.Close()
		})
	}

	// Wait until every client listener has a bound address.
	for _, rn := range nodes {
		waitAddr(t, rn)
	}
	return nodes
}

func waitAddr(t *testing.T, rn *routeNode) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a := rn.srv.Addr(); a != "" {
			rn.clientAddr = a
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s server never bound", rn.id)
}

// waitLeader polls until exactly one node reports raft.Leader, excluding any id
// in exclude (a killed node). Returns the leader.
func waitLeader(t *testing.T, nodes []*routeNode, exclude map[string]bool) *routeNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leader *routeNode
		count := 0
		for _, n := range nodes {
			if exclude[n.id] {
				continue
			}
			if n.node.Status().Role == raft.Leader {
				leader = n
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no stable leader elected")
	return nil
}

func firstFollower(nodes []*routeNode, leaderID string) *routeNode {
	for _, n := range nodes {
		if n.id != leaderID {
			return n
		}
	}
	return nil
}

func TestClusterClientRedirectsWritesAndReads(t *testing.T) {
	nodes := newRoutedCluster(t, 3)
	leader := waitLeader(t, nodes, nil)

	// Seed the ClusterClient with a FOLLOWER first, so the very first op must
	// redirect to the leader.
	follower := firstFollower(nodes, leader.id)
	members := []string{follower.member()}
	for _, n := range nodes {
		if n.id != follower.id {
			members = append(members, n.member())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cc, err := client.DialCluster(ctx, members)
	if err != nil {
		t.Fatalf("DialCluster: %v", err)
	}
	defer cc.Close()

	if err := cc.Create(ctx, "orders", 1); err != nil {
		t.Fatalf("Create via redirect: %v", err)
	}

	// Sub must land on the leader (a default SUB on a follower returns
	// NOTLEADER); establish before publishing so the message is delivered.
	ch, err := cc.Sub(ctx, "orders", "cg")
	if err != nil {
		t.Fatalf("Sub via redirect: %v", err)
	}

	id, dup, err := cc.Pub(ctx, "orders", "", "", []byte("hello"))
	if err != nil {
		t.Fatalf("Pub via redirect: %v", err)
	}
	if dup {
		t.Fatal("first Pub reported dup")
	}

	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatalf("delivery channel closed before message: %v", cc.Err())
		}
		if string(d.Payload) != "hello" || d.MsgID != id {
			t.Fatalf("got payload=%q id=%d, want hello id=%d", d.Payload, d.MsgID, id)
		}
		if err := d.Ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery within 5s")
	}
}

func TestClusterClientStaleReadOnFollower(t *testing.T) {
	nodes := newRoutedCluster(t, 3)
	leader := waitLeader(t, nodes, nil)
	follower := firstFollower(nodes, leader.id)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Publish through the leader and wait for replication to reach the follower.
	lc, err := client.Dial(ctx, leader.clientAddr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer lc.Close()
	if err := lc.Create(ctx, "orders", 1); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := lc.Pub(ctx, "orders", "", "", []byte("stale-me")); err != nil {
		t.Fatalf("pub: %v", err)
	}

	// A bare client to the follower: default Sub is redirected (NOTLEADER),
	// SubStale reads follower-local state without redirect.
	fc, err := client.Dial(ctx, follower.clientAddr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer fc.Close()

	if _, err := fc.Sub(ctx, "orders", "cg-redirect"); err == nil {
		t.Fatal("default Sub on follower succeeded, want NOTLEADER")
	} else {
		var nl *client.NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("default Sub error = %v, want *NotLeaderError", err)
		}
	}

	// SubStale must succeed against the follower and eventually deliver the
	// replicated message from its local state.
	deadline := time.Now().Add(8 * time.Second)
	for {
		ch, err := fc.SubStale(ctx, "orders", "cg-stale")
		if err != nil {
			t.Fatalf("SubStale on follower: %v", err)
		}
		select {
		case d, ok := <-ch:
			if ok && string(d.Payload) == "stale-me" {
				return // follower served its local copy
			}
		case <-time.After(500 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("follower-local stale read never returned the message")
		}
		// Re-subscribe on the same connection for the next poll.
		fc.Close()
		fc, err = client.Dial(ctx, follower.clientAddr)
		if err != nil {
			t.Fatalf("re-dial follower: %v", err)
		}
	}
}

func TestClusterClientConvergesAfterLeaderKill(t *testing.T) {
	nodes := newRoutedCluster(t, 3)
	leader := waitLeader(t, nodes, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	members := make([]string, 0, len(nodes))
	for _, n := range nodes {
		members = append(members, n.member())
	}
	cc, err := client.DialCluster(ctx, members)
	if err != nil {
		t.Fatalf("DialCluster: %v", err)
	}
	defer cc.Close()

	if err := cc.Create(ctx, "orders", 1); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := cc.Pub(ctx, "orders", "", "", []byte("pre-kill")); err != nil {
		t.Fatalf("pre-kill pub: %v", err)
	}

	// Kill the leader: stop its raft node and shut its client server.
	sctx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	_ = leader.srv.Shutdown(sctx)
	sc()
	_ = leader.node.Stop()

	waitLeader(t, nodes, map[string]bool{leader.id: true})

	// Writes converge under bounded retry: each Pub call terminates (no spin);
	// re-issue until the new leader accepts, within the test deadline.
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, _, lastErr = cc.Pub(ctx, "orders", "", "", []byte("post-kill"))
		if lastErr == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("write never converged after leader kill: %v", lastErr)
}
