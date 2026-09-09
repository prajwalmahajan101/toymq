package broker

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/prajwalmahajan101/toymq/internal/replication"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// TestApplyDeterminism is the M1 owned-risk test: feeding the same
// mutating-command stream through StateMachine.Apply on two independent
// brokers must produce byte-identical state — the same WAL segments, the same
// dedupe index, and the same consumer offsets. Any hidden wall-clock or
// map-iteration non-determinism in the apply path would diverge the replicas
// and this test would catch it (ADR 0028).
func TestApplyDeterminism(t *testing.T) {
	// A fixed stream: creates, publishes (some delayed, some deduped incl. a
	// replayed key), across two topics and, for the multi-partition topic,
	// two partitions. Every non-deterministic value is pinned in the
	// envelope, exactly as the leader would stamp it before Propose.
	stream := []replication.Envelope{
		{Kind: replication.KindCreateTopic, Topic: "events", Partitions: 2},
		{Kind: replication.KindPublish, Topic: "orders", Partition: 0, TsNs: 1000, Payload: []byte("a")},
		{Kind: replication.KindPublish, Topic: "orders", Partition: 0, TsNs: 2000, DedupeKey: "dk", Payload: []byte("b")},
		{Kind: replication.KindPublish, Topic: "orders", Partition: 0, TsNs: 3000, DedupeKey: "dk", Payload: []byte("dup-ignored")},
		{Kind: replication.KindPublish, Topic: "events", Partition: 1, TsNs: 4000, VisibleAtNs: 9000, Payload: []byte("c")},
		{Kind: replication.KindPublish, Topic: "events", Partition: 0, TsNs: 5000, Payload: []byte("d")},
	}

	dirA, brokerA := feedStream(t, stream)
	dirB, brokerB := feedStream(t, stream)

	// WAL bytes must match for every topic/partition written.
	for _, tp := range []struct {
		topic     string
		partition int
	}{
		{"orders", 0}, {"events", 0}, {"events", 1},
	} {
		bytesA := readSegment(t, dirA, tp.topic, tp.partition, brokerA)
		bytesB := readSegment(t, dirB, tp.topic, tp.partition, brokerB)
		if !bytes.Equal(bytesA, bytesB) {
			t.Errorf("%s/%d: WAL segment bytes diverge (%d vs %d bytes)", tp.topic, tp.partition, len(bytesA), len(bytesB))
		}
	}

	// Dedupe index must resolve the replayed key to the same MsgID on both.
	pA, _ := brokerA.partitionAt("orders", 0)
	pB, _ := brokerB.partitionAt("orders", 0)
	idA, okA := pA.dedupe.Lookup("dk")
	idB, okB := pB.dedupe.Lookup("dk")
	if !okA || !okB || idA != idB {
		t.Errorf("dedupe lookup diverges: A=(%d,%v) B=(%d,%v)", idA, okA, idB, okB)
	}
}

// feedStream builds a fresh broker, wraps it in a BrokerSM, and applies every
// envelope in index order — exactly what a replica does when it replays the
// committed log. Returns the data dir and the broker (still open, so its WAL is
// flushed and readable via its own segment handles).
func feedStream(t *testing.T, stream []replication.Envelope) (string, *Broker) {
	t.Helper()
	dir := t.TempDir()
	b, err := New(dir, testDedupeCap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	sm := replication.NewBrokerSM(b)
	for i, env := range stream {
		if _, err := sm.Apply(raft.Entry{Index: raft.Index(i + 1), Term: 1, Data: replication.Encode(env)}); err != nil {
			t.Fatalf("Apply entry %d (%v): %v", i, env.Kind, err)
		}
	}
	return dir, b
}

// readSegment returns the raw bytes of a partition's single WAL segment. A
// 1-partition topic stores it flat at topics/<name>/000000.log; a
// multi-partition topic nests under topics/<name>/<id>/000000.log (ADR 0021).
func readSegment(t *testing.T, dir, topic string, partition int, b *Broker) []byte {
	t.Helper()
	count, err := b.TopicPartitions(topic)
	if err != nil {
		t.Fatalf("TopicPartitions(%q): %v", topic, err)
	}
	path := filepath.Join(dir, "topics", topic)
	if count > 1 {
		path = filepath.Join(path, strconv.Itoa(partition))
	}
	path = filepath.Join(path, "000000.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment %s: %v", path, err)
	}
	return data
}
