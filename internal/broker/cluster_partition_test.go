package broker

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// partitionableTransport decorates a raft.Transport with a test-controllable
// blocked-peer set, dropping BOTH outbound Sends to and inbound Steps from any
// blocked peer — a bidirectional network partition whose two sides both stay
// alive. This is the piece node.Stop() cannot express: a kill removes one side,
// so it never exercises split-brain (two live sides, one without quorum). The
// production embed never needs fault injection, so the seam lives in the test
// package rather than internal/replication (ADR 0031).
type partitionableTransport struct {
	inner raft.Transport

	mu      sync.RWMutex
	blocked map[raft.NodeID]bool
}

func newPartitionableTransport(inner raft.Transport) *partitionableTransport {
	return &partitionableTransport{
		inner:   inner,
		blocked: make(map[raft.NodeID]bool),
	}
}

func (t *partitionableTransport) isBlocked(id raft.NodeID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.blocked[id]
}

// Send drops the message when its destination is blocked (best-effort contract,
// exactly like a dropped heartbeat — raft retransmits next tick); otherwise it
// forwards to the inner transport.
func (t *partitionableTransport) Send(ctx context.Context, msg raft.Message) error {
	if t.isBlocked(msg.To) {
		return nil
	}
	return t.inner.Send(ctx, msg)
}

// Register wraps the real step callback so an inbound message From a blocked
// peer is dropped too — that makes the partition bidirectional (a one-way drop
// would still let the isolated node hear the majority and never campaign).
func (t *partitionableTransport) Register(step func(ctx context.Context, msg raft.Message) error) {
	t.inner.Register(func(ctx context.Context, msg raft.Message) error {
		if t.isBlocked(msg.From) {
			return nil
		}
		return step(ctx, msg)
	})
}

func (t *partitionableTransport) Close() error { return t.inner.Close() }

// block marks the given peers unreachable in both directions.
func (t *partitionableTransport) block(ids ...raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range ids {
		t.blocked[id] = true
	}
}

// heal clears every blocked peer on this node.
func (t *partitionableTransport) heal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked = make(map[raft.NodeID]bool)
}

var _ raft.Transport = (*partitionableTransport)(nil)

// partition severs every node in groupA from every node in groupB (both
// directions). Both groups keep running; only cross-group raft traffic drops.
func partition(groupA, groupB []*clusterNode) {
	for _, a := range groupA {
		for _, b := range groupB {
			a.part.block(raft.NodeID(b.id))
			b.part.block(raft.NodeID(a.id))
		}
	}
}

// healAll reconnects the whole cluster.
func healAll(nodes []*clusterNode) {
	for _, cn := range nodes {
		cn.part.heal()
	}
}

// minorityLeaderPublishBlocked publishes on the isolated old leader with a
// bounded context and asserts it does NOT succeed. A partitioned leader's
// Propose appends locally but never reaches quorum, so it blocks until the ctx
// fires and returns ctx.Err() (toyraft node_public.go: Propose selects on
// <-ctx.Done()). A success here would be split-brain.
func minorityLeaderPublishBlocked(t *testing.T, leader *clusterNode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, _, err := leader.broker.PublishCtx(ctx, "orders", "", "", 0, false, []byte("split"), 0); err == nil {
		t.Fatalf("isolated minority leader %s accepted a write — split-brain", leader.id)
	}
}

// TestClusterPartitionHealNoLoss proves partition-heal with zero acked-write
// loss and no split-brain: isolate the leader alone (minority of 1), the
// majority of 2 keeps serving, the isolated leader cannot ack a write, and on
// heal the minority discards its uncommitted tail and converges on the majority
// head — every acked PUB intact.
func TestClusterPartitionHealNoLoss(t *testing.T) {
	nodes := newCluster(t, 3)

	const k = 3
	for range k {
		publishOnLeader(t, nodes, nil, []byte("pre"), 15*time.Second)
	}
	waitConverged(t, nodes, "orders", 0, k-1, 15*time.Second)

	// Isolate the current leader alone; the other two are the majority (quorum
	// of 3 = 2). Splitting the leader off is the worst case for split-brain.
	leader := waitLeader(t, nodes, nil, 15*time.Second)
	minority := []*clusterNode{leader}
	majority := make([]*clusterNode, 0, 2)
	for _, cn := range nodes {
		if cn != leader {
			majority = append(majority, cn)
		}
	}
	partition(minority, majority)

	// No split-brain: the isolated old leader cannot commit, so a write on it
	// must never succeed.
	minorityLeaderPublishBlocked(t, leader)

	// The majority elects a new leader and accepts writes that converge on both.
	waitLeader(t, majority, leader, 15*time.Second)
	const extra = 2
	for range extra {
		publishOnLeader(t, majority, leader, []byte("maj"), 15*time.Second)
	}
	waitConverged(t, majority, "orders", 0, k-1+extra, 15*time.Second)

	// Heal: the minority replays the divergent tail and all three converge to
	// the majority head with zero acked-write loss (the minority never acked a
	// conflicting write — its "split" attempt errored above).
	healAll(nodes)
	waitConverged(t, nodes, "orders", 0, k-1+extra, 30*time.Second)
}

// --- linearizability harness ---------------------------------------------

// linInput is one modelled operation over the single-partition message log.
type linInput struct {
	op string // "pub" | "consume"
	id uint64 // assigned MsgID
}

// linState is the sequential specification's state: the set of published
// (leader-acked) MsgIDs and the set already consumed. A message-queue register
// where each MsgID is published exactly once and consumed exactly once.
type linState struct {
	published map[uint64]bool
	consumed  map[uint64]bool
}

func copySet(m map[uint64]bool) map[uint64]bool {
	out := make(map[uint64]bool, len(m)+1)
	maps.Copy(out, m)
	return out
}

func setsEqual(a, b map[uint64]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// pubConsumeModel is the porcupine sequential model. Step is pure (copies before
// mutating). A pub of an already-published id (duplicate MsgID assignment) or a
// consume of an unpublished / already-consumed id makes the history
// non-linearizable — exactly the "no double-ack as a unique consume" property.
var pubConsumeModel = porcupine.Model{
	Init: func() any {
		return linState{published: map[uint64]bool{}, consumed: map[uint64]bool{}}
	},
	Step: func(state, input, _ any) (bool, any) {
		s := state.(linState)
		in := input.(linInput)
		switch in.op {
		case "pub":
			if s.published[in.id] {
				return false, s // duplicate MsgID assignment
			}
			ns := linState{published: copySet(s.published), consumed: s.consumed}
			ns.published[in.id] = true
			return true, ns
		case "consume":
			if !s.published[in.id] || s.consumed[in.id] {
				return false, s // consumed something unpublished or twice
			}
			ns := linState{published: s.published, consumed: copySet(s.consumed)}
			ns.consumed[in.id] = true
			return true, ns
		default:
			return false, s
		}
	},
	Equal: func(a, b any) bool {
		s1, s2 := a.(linState), b.(linState)
		return setsEqual(s1.published, s2.published) && setsEqual(s1.consumed, s2.consumed)
	},
}

// harnessPublish finds a currently self-reported leader and publishes one
// message with a 1s bounded context. Under partition churn the old leader may
// still report Leader while isolated; its Propose blocks and the ctx fires, so
// the caller simply retries and re-resolves to the majority leader. Returns the
// assigned MsgID on a leader-acked write.
func harnessPublish(nodes []*clusterNode, payload []byte, deadline time.Time) (uint64, bool) {
	for time.Now().Before(deadline) {
		var leader *clusterNode
		for _, cn := range nodes {
			if cn.node.Status().Role == raft.Leader {
				leader = cn
				break
			}
		}
		if leader == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		id, _, _, err := leader.broker.PublishCtx(ctx, "orders", "", "", 0, false, payload, 0)
		cancel()
		if err == nil {
			return id, true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, false
}

// TestClusterLinearizablePubConsume is the Jepsen/Porcupine-style harness
// (ROADMAP v3 M2 exit): concurrent PUBs run while a churn goroutine repeatedly
// partitions one node off and heals it. Only a single node is ever isolated, so
// a majority always makes progress. After the churn the cluster converges; the
// acked-PUB history plus a post-heal drain is checked for linearizability. Pass:
// no acked PUB lost, no double-consume. Run under -race.
func TestClusterLinearizablePubConsume(t *testing.T) {
	nodes := newCluster(t, 3)

	const (
		writers      = 3
		perWriter    = 8
		globalBudget = 60 * time.Second
	)
	deadline := time.Now().Add(globalBudget)

	var (
		mu    sync.Mutex
		pubs  []porcupine.Operation
		acked []uint64
	)

	// Churn: isolate one node at a time, heal, repeat — until publishers finish.
	churnStop := make(chan struct{})
	var churnWG sync.WaitGroup
	churnWG.Add(1)
	go func() {
		defer churnWG.Done()
		i := 0
		for {
			select {
			case <-churnStop:
				return
			default:
			}
			victim := nodes[i%len(nodes)]
			others := make([]*clusterNode, 0, len(nodes)-1)
			for _, cn := range nodes {
				if cn != victim {
					others = append(others, cn)
				}
			}
			partition([]*clusterNode{victim}, others)
			time.Sleep(500 * time.Millisecond)
			healAll(nodes)
			time.Sleep(700 * time.Millisecond)
			i++
		}
	}()

	// Publishers: each lands perWriter acked writes (or runs out the budget).
	var pubWG sync.WaitGroup
	for w := range writers {
		pubWG.Add(1)
		go func(clientID int) {
			defer pubWG.Done()
			got := 0
			for got < perWriter && time.Now().Before(deadline) {
				call := time.Now().UnixNano()
				id, ok := harnessPublish(nodes, []byte("m"), deadline)
				if !ok {
					return
				}
				ret := time.Now().UnixNano()
				mu.Lock()
				pubs = append(pubs, porcupine.Operation{
					ClientId: clientID,
					Input:    linInput{op: "pub", id: id},
					Call:     call,
					Output:   true,
					Return:   ret,
				})
				acked = append(acked, id)
				mu.Unlock()
				got++
			}
		}(w)
	}
	pubWG.Wait()
	close(churnStop)
	churnWG.Wait()
	healAll(nodes)

	if len(acked) == 0 {
		t.Fatal("no writes were acked under partition churn")
	}

	// Every acked MsgID is contiguous from 0 (single-writer log, only our
	// clients publish); the highest acked id is the converged head.
	slices.Sort(acked)
	maxID := acked[len(acked)-1]

	// No acked PUB lost: every node must hold the log up to maxID once healed.
	waitConverged(t, nodes, "orders", 0, maxID, 30*time.Second)

	// Drain the converged log as the consume sequence (post-heal, sequential):
	// one consume per acked MsgID, timestamped strictly after every PUB return.
	base := time.Now().UnixNano()
	history := make([]porcupine.Operation, 0, len(pubs)+len(acked))
	history = append(history, pubs...)
	for i, id := range acked {
		t0 := base + int64(2*i)
		history = append(history, porcupine.Operation{
			ClientId: writers, // a distinct "consumer" client
			Input:    linInput{op: "consume", id: id},
			Call:     t0,
			Output:   id,
			Return:   t0 + 1,
		})
	}

	if !porcupine.CheckOperations(pubConsumeModel, history) {
		t.Fatalf("history not linearizable: %d acked PUBs, head=%d", len(acked), maxID)
	}
}
