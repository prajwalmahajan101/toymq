package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Long visibility so the manual read/ack loop in these tests is never
// racing the redelivery ticker.
func partitionHarness(t *testing.T, defaultPartitions int) *harness {
	return startBroker(t,
		withVisibility(5*time.Second),
		withDefaultPartitions(defaultPartitions),
	)
}

// TestExplicitPartitionRouting: PUB <topic>#<n> lands on exactly partition
// n, and a partition-scoped SUB sees only its own partition (no
// cross-partition leakage).
func TestExplicitPartitionRouting(t *testing.T) {
	h := partitionHarness(t, 1)
	prod := dial(t, h.addr)
	prod.create(t, "orders", 3)
	prod.expectOK(t)

	for p := 0; p < 3; p++ {
		prod.pubRouted(t, fmt.Sprintf("orders#%d", p), "", "", []byte(fmt.Sprintf("m%d", p)))
		prod.expectOK(t)
	}

	// A consumer on partition 1 sees only m1.
	c1 := dial(t, h.addr)
	c1.sub(t, "orders#1", "c1")
	c1.expectOK(t)
	msg := c1.expectMsg(t)
	if msg.partition != 1 || string(msg.payload) != "m1" {
		t.Fatalf("partition-1 sub got partition=%d payload=%q, want 1/m1", msg.partition, msg.payload)
	}
	c1.expectNoMsg(t, 200*time.Millisecond)
}

// TestAllPartitionsFanIn is the owned risk test: N producers fan messages
// across partitions while one #* consumer reads all of them. It asserts
// per-partition MsgID monotonicity, exact per-partition counts, and zero
// cross-partition leakage. Runs under -race in CI.
func TestAllPartitionsFanIn(t *testing.T) {
	const (
		parts   = 4
		perPart = 25
	)
	h := partitionHarness(t, 1)
	prod := dial(t, h.addr)
	prod.create(t, "orders", parts)
	prod.expectOK(t)

	// Publish perPart messages to each partition explicitly; payload encodes
	// (partition, sequence) so the consumer can verify routing and order.
	for seq := 0; seq < perPart; seq++ {
		for p := 0; p < parts; p++ {
			prod.pubRouted(t, fmt.Sprintf("orders#%d", p), "", "",
				[]byte(fmt.Sprintf("%d:%d", p, seq)))
			prod.expectOK(t)
		}
	}

	sub := dial(t, h.addr)
	sub.sub(t, "orders#*", "c-all")
	sub.expectOK(t)

	lastID := make(map[int]int64) // partition -> last delivered msgID
	nextSeq := make(map[int]int)  // partition -> expected next sequence
	count := make(map[int]int)    // partition -> messages seen
	for p := 0; p < parts; p++ {
		lastID[p] = -1
	}

	total := parts * perPart
	for i := 0; i < total; i++ {
		m := sub.expectMsg(t)
		var gotP, gotSeq int
		if _, err := fmt.Sscanf(string(m.payload), "%d:%d", &gotP, &gotSeq); err != nil {
			t.Fatalf("bad payload %q: %v", m.payload, err)
		}
		// No cross-partition leakage: the MSG's partition field must match
		// the partition encoded in the payload at publish time.
		if gotP != m.partition {
			t.Fatalf("cross-partition leak: payload partition %d on MSG partition %d", gotP, m.partition)
		}
		// Per-partition MsgID monotonicity.
		if int64(m.msgID) <= lastID[m.partition] {
			t.Fatalf("partition %d msgID not increasing: got %d after %d", m.partition, m.msgID, lastID[m.partition])
		}
		lastID[m.partition] = int64(m.msgID)
		// Per-partition delivery order matches publish order.
		if gotSeq != nextSeq[m.partition] {
			t.Fatalf("partition %d out of order: got seq %d want %d", m.partition, gotSeq, nextSeq[m.partition])
		}
		nextSeq[m.partition]++
		count[m.partition]++
		// No ACK here: visibility is 5s so nothing redelivers during the
		// read, and not acking keeps ACK OKs out of the raw MSG stream.
	}

	for p := 0; p < parts; p++ {
		if count[p] != perPart {
			t.Fatalf("partition %d: got %d messages, want %d", p, count[p], perPart)
		}
	}
	sub.expectNoMsg(t, 200*time.Millisecond)
}

// TestRoutingKeyDeterministicAcrossRestart: the same routing key always
// hashes to the same partition, including after a restart (fnv is stable).
func TestRoutingKeyDeterministicAcrossRestart(t *testing.T) {
	h := partitionHarness(t, 1)
	prod := dial(t, h.addr)
	prod.create(t, "orders", 4)
	prod.expectOK(t)

	prod.close()

	p := dial(t, h.addr)
	p.pubRouted(t, "orders", "", "user-42", []byte("hello"))
	p.expectOK(t)
	s := dial(t, h.addr)
	s.sub(t, "orders#*", "probe1")
	s.expectOK(t)
	before := s.expectMsg(t).partition
	p.close()
	s.close()

	h.restart(t)

	prod2 := dial(t, h.addr)
	prod2.pubRouted(t, "orders", "", "user-42", []byte("hello2"))
	prod2.expectOK(t)
	s2 := dial(t, h.addr)
	s2.sub(t, "orders#*", "probe2")
	s2.expectOK(t)
	// Drain until we see the second publish of user-42; no acks so ACK OKs
	// don't pollute the raw MSG stream.
	after := -1
	for i := 0; i < 2; i++ {
		m := s2.expectMsg(t)
		if string(m.payload) == "hello2" {
			after = m.partition
		}
	}
	prod2.close()
	s2.close()
	if after == -1 {
		t.Fatal("did not observe hello2")
	}
	if before != after {
		t.Fatalf("routing key landed on partition %d then %d after restart", before, after)
	}
}

// TestPartitionOffsetsSurviveRestart: per-partition ack state persists, so
// a re-subscribe after restart delivers nothing already acked.
func TestPartitionOffsetsSurviveRestart(t *testing.T) {
	h := partitionHarness(t, 1)
	prod := dial(t, h.addr)
	prod.create(t, "orders", 2)
	prod.expectOK(t)
	for p := 0; p < 2; p++ {
		prod.pubRouted(t, fmt.Sprintf("orders#%d", p), "", "", []byte("x"))
		prod.expectOK(t)
	}

	sub := dial(t, h.addr)
	sub.sub(t, "orders#*", "c1")
	sub.expectOK(t)
	for i := 0; i < 2; i++ {
		m := sub.expectMsg(t)
		sub.ackP(t, "c1", m.partition, m.msgID)
		sub.expectOK(t) // consume the ACK's OK response
	}
	prod.close()
	sub.close()

	h.restart(t)

	sub2 := dial(t, h.addr)
	sub2.sub(t, "orders#*", "c1")
	sub2.expectOK(t)
	// Everything was acked before the restart, on both partitions.
	sub2.expectNoMsg(t, 300*time.Millisecond)
	sub2.close()
}

// TestSinglePartitionFlatLayout: a 1-partition topic keeps the pre-M4 flat
// on-disk layout (000000.log at the topic root, no meta.json, no subdirs).
func TestSinglePartitionFlatLayout(t *testing.T) {
	h := partitionHarness(t, 1)
	prod := dial(t, h.addr)
	prod.pubRouted(t, "flat", "", "", []byte("x"))
	prod.expectOK(t)

	topicDir := filepath.Join(h.dataDir, "topics", "flat")
	if _, err := os.Stat(filepath.Join(topicDir, "000000.log")); err != nil {
		t.Fatalf("expected flat WAL at topic root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(topicDir, "meta.json")); !os.IsNotExist(err) {
		t.Fatalf("1-partition topic should have no meta.json, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(topicDir, "0")); !os.IsNotExist(err) {
		t.Fatalf("1-partition topic should have no partition subdir, stat err=%v", err)
	}
}

// TestMultiPartitionLayout: an N>1 topic writes meta.json and per-partition
// subdirectories.
func TestMultiPartitionLayout(t *testing.T) {
	h := partitionHarness(t, 1)
	prod := dial(t, h.addr)
	prod.create(t, "orders", 3)
	prod.expectOK(t)

	topicDir := filepath.Join(h.dataDir, "topics", "orders")
	if _, err := os.Stat(filepath.Join(topicDir, "meta.json")); err != nil {
		t.Fatalf("expected meta.json for multi-partition topic: %v", err)
	}
	for p := 0; p < 3; p++ {
		wal := filepath.Join(topicDir, fmt.Sprintf("%d", p), "000000.log")
		if _, err := os.Stat(wal); err != nil {
			t.Fatalf("expected partition WAL %s: %v", wal, err)
		}
	}
}

// TestCreateIdempotentAndMismatch: CREATE with the same count is OK; a
// different count is an error.
func TestCreateIdempotentAndMismatch(t *testing.T) {
	h := partitionHarness(t, 1)
	c := dial(t, h.addr)

	c.create(t, "orders", 3)
	c.expectOK(t)
	c.create(t, "orders", 3) // idempotent
	c.expectOK(t)

	c.create(t, "orders", 4) // mismatch
	line := c.readResponseLine(t)
	if line[:3] != "ERR" {
		t.Fatalf("re-create with different count = %q, want ERR", line)
	}
}

// TestSubPartitionOutOfRange: subscribing to a partition >= count fails.
func TestSubPartitionOutOfRange(t *testing.T) {
	h := partitionHarness(t, 1)
	c := dial(t, h.addr)
	c.create(t, "orders", 2)
	c.expectOK(t)

	c.sub(t, "orders#5", "c1")
	line := c.readResponseLine(t)
	if line[:3] != "ERR" {
		t.Fatalf("out-of-range SUB = %q, want ERR", line)
	}
}
