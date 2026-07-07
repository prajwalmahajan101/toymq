package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

const testDedupeCap = 100

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	b, err := New(t.TempDir(), testDedupeCap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func mustPublish(t *testing.T, b *Broker, topic, key string, payload []byte) uint64 {
	t.Helper()
	id, dup, err := bpub(b, topic, key, payload)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if dup {
		t.Fatalf("Publish restured dedp=true unexpectedly")
	}
	return id
}

func recvInflight(t *testing.T, ch <-chan *Inflight, timeout time.Duration) *Inflight {
	t.Helper()
	select {
	case inf := <-ch:
		return inf
	case <-time.After(timeout):
		t.Fatal("timed out waiting for inflight")
		return nil
	}
}

func TestSubscribeReceivesNewPublish(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	id := mustPublish(t, b, "orders", "", []byte("hello"))

	inf := recvInflight(t, ch, time.Second)

	if inf.MsgID != id {
		t.Errorf("MsgID = %d, want %d", inf.MsgID, id)
	}

	if string(inf.Payload) != "hello" {
		t.Errorf("Payload =%q, want %q", inf.Payload, "hello")
	}
}

func TestSubscribeReadsBacklog(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	id0 := mustPublish(t, b, "orders", "", []byte("first"))
	id1 := mustPublish(t, b, "orders", "", []byte("second"))

	ch := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	got0 := recvInflight(t, ch, time.Second)
	got1 := recvInflight(t, ch, time.Second)
	if got0.MsgID != id0 || got1.MsgID != id1 {
		t.Errorf("got msg ids (%d, %d), want (%d, %d)", got0.MsgID, got1.MsgID, id0, id1)
	}
}

func TestAckAdvancesLastAcked(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	mustPublish(t, b, "orders", "", []byte("a"))
	mustPublish(t, b, "orders", "", []byte("b"))
	mustPublish(t, b, "orders", "", []byte("c"))

	for i := range uint64(3) {
		inf := recvInflight(t, ch, time.Second)
		if inf.MsgID != i {
			t.Fatalf("got msg %d, want %d", inf.MsgID, i)
		}
		if err := back(b, "orders", "c1", inf.MsgID); err != nil {
			t.Fatalf("Ack %d: %v", inf.MsgID, err)
		}
	}

	// Reach into the consumer to verify lastAcked.
	topic, _ := b.getOrCreateTopic("orders")
	c := part0(topic).getOrCreateConsumer("c1")
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastAcked != 2 {
		t.Errorf("lastAcked = %d, want 2", c.lastAcked)
	}
	if len(c.inflight) != 0 {
		t.Errorf("inflight not empty: %d entries", len(c.inflight))
	}
	if len(c.aboveLast) != 0 {
		t.Errorf("aboveLast not empty: %d entries", len(c.aboveLast))
	}
}

func TestAckOutOfOrderDrains(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for range 5 {
		mustPublish(t, b, "orders", "", []byte("x"))
	}
	for range 5 {
		recvInflight(t, ch, time.Second)
	}

	// Ack out of order: 2, 0, 1 — then 4, 3.
	for _, id := range []uint64{2, 0, 1} {
		if err := back(b, "orders", "c1", id); err != nil {
			t.Fatalf("Ack %d: %v", id, err)
		}
	}
	topic, _ := b.getOrCreateTopic("orders")
	c := part0(topic).getOrCreateConsumer("c1")
	c.mu.Lock()
	got := c.lastAcked
	c.mu.Unlock()
	if got != 2 {
		t.Fatalf("after acking 2,0,1: lastAcked = %d, want 2", got)
	}

	for _, id := range []uint64{4, 3} {
		if err := back(b, "orders", "c1", id); err != nil {
			t.Fatalf("Ack %d: %v", id, err)
		}
	}
	c.mu.Lock()
	got = c.lastAcked
	drained := len(c.aboveLast)
	c.mu.Unlock()
	if got != 4 {
		t.Errorf("after all acks: lastAcked = %d, want 4", got)
	}
	if drained != 0 {
		t.Errorf("aboveLast = %d entries, want 0", drained)
	}
}

func TestNackRedeliversImmediately(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 8)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	id := mustPublish(t, b, "orders", "", []byte("hi"))

	first := recvInflight(t, ch, time.Second)
	if first.MsgID != id {
		t.Fatalf("first MsgID = %d, want %d", first.MsgID, id)
	}
	if first.Attempts != 1 {
		t.Fatalf("first Attempts = %d, want 1", first.Attempts)
	}

	if err := bnack(b, "orders", "c1", id, ch); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	second := recvInflight(t, ch, time.Second)
	if second.MsgID != id {
		t.Fatalf("second MsgID = %d, want %d", second.MsgID, id)
	}
	if second.Attempts != 2 {
		t.Fatalf("second Attempts = %d, want 2", second.Attempts)
	}
}

func TestSubscribeCtxCancelRollsBackInflight(t *testing.T) {
	b := newTestBroker(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Unbuffered channel + no reader → runDelivery blocks at sendCh <- inf.
	ch := make(chan *Inflight)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	mustPublish(t, b, "orders", "", []byte("blocked"))

	// Wait until runDelivery has marked the message inflight, then
	// cancel — the ctx.Done branch must roll the entry back out.
	deadline := time.Now().Add(time.Second)
	for {
		topic, _ := b.getOrCreateTopic("orders")
		c := part0(topic).getOrCreateConsumer("c1")
		c.mu.Lock()
		inflightCount := len(c.inflight)
		c.mu.Unlock()
		if inflightCount > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runDelivery never marked inflight")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	deadline = time.Now().Add(time.Second)
	for {
		topic, _ := b.getOrCreateTopic("orders")
		c := part0(topic).getOrCreateConsumer("c1")
		c.mu.Lock()
		inflightCount := len(c.inflight)
		c.mu.Unlock()
		if inflightCount == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inflight not rolled back after ctx cancel: %d entries", inflightCount)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNackUnknownMsg(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()

	ch := make(chan *Inflight, 1)
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bnack(b, "orders", "c1", 999, ch); err == nil {
		t.Fatal("Nack of unknown msg: expected error, got nil")
	}
}

func TestNewBrokerLoadOffsetsFails(t *testing.T) {
	dir := t.TempDir()
	// Pre-create a topic dir with a corrupt offsets.json. broker.New's
	// startup loop opens the WAL then calls loadOffsets — the latter
	// must propagate the decode error.
	topicDir := filepath.Join(dir, "topics", "orders")
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(topicDir, "offsets.json"), []byte("not json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := New(dir, testDedupeCap); err == nil {
		t.Fatal("New: expected error from corrupt offsets, got nil")
	}
}

func TestNewBrokerTopicsDirFails(t *testing.T) {
	dir := t.TempDir()
	// Make "topics" a regular file so ReadDir fails.
	if err := os.WriteFile(filepath.Join(dir, "topics"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := New(dir, testDedupeCap); err == nil {
		t.Fatal("New: expected error from non-dir topics path, got nil")
	}
}

func TestPublishOnBlockedTopicFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "topics"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Pre-create a file at topics/blocked so wal.Open's MkdirAll
	// fails when the broker tries to open that topic on first publish.
	if err := os.WriteFile(filepath.Join(dir, "topics", "blocked"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	b, err := New(dir, testDedupeCap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	if _, _, err := bpub(b, "blocked", "", []byte("nope")); err == nil {
		t.Fatal("Publish to blocked topic: expected error, got nil")
	}
	if err := back(b, "blocked", "c1", 0); err == nil {
		t.Fatal("Ack on blocked topic: expected error, got nil")
	}
	if err := bnack(b, "blocked", "c1", 0, make(chan *Inflight, 1)); err == nil {
		t.Fatal("Nack on blocked topic: expected error, got nil")
	}
	if _, err := bsub(b, context.Background(), "blocked", "c1", make(chan *Inflight, 1)); err == nil {
		t.Fatal("Subscribe on blocked topic: expected error, got nil")
	}
}

func TestPublishDedupeReturnsOriginalID(t *testing.T) {
	b := newTestBroker(t)

	id1, dup1, err := bpub(b, "orders", "key-A", []byte("first"))
	if err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	if dup1 {
		t.Errorf("first publish: dup = true, want false")
	}

	id2, dup2, err := bpub(b, "orders", "key-A", []byte("second"))
	if err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if !dup2 {
		t.Errorf("second publish: dup = false, want true")
	}
	if id2 != id1 {
		t.Errorf("second publish: id = %d, want %d (original)", id2, id1)
	}
}

func TestOffsetsPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// First broker: publish 3, subscribe, ack all 3, close.
	{
		b, err := New(dir, testDedupeCap)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan *Inflight, 8)
		if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		for range 3 {
			mustPublish(t, b, "orders", "", []byte("x"))
		}
		for range 3 {
			inf := recvInflight(t, ch, time.Second)
			if err := back(b, "orders", "c1", inf.MsgID); err != nil {
				t.Fatalf("Ack %d: %v", inf.MsgID, err)
			}
		}

		cancel()
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Second broker: same dir, fresh subscribe. No messages should
	// be redelivered — they were all acked before the close.
	{
		b, err := New(dir, testDedupeCap)
		if err != nil {
			t.Fatalf("Reopen: %v", err)
		}
		t.Cleanup(func() { b.Close() })

		ctx := t.Context()
		ch := make(chan *Inflight, 8)
		if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
			t.Fatalf("Subscribe after reopen: %v", err)
		}

		select {
		case inf := <-ch:
			t.Errorf("unexpected redelivery after reopen: msg %d", inf.MsgID)
		case <-time.After(200 * time.Millisecond):
			// good — no redelivery
		}
	}
}

func TestOffsetsPersistAboveLast(t *testing.T) {
	dir := t.TempDir()

	{
		b, err := New(dir, testDedupeCap)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan *Inflight, 8)
		if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		// Publish 5, receive all 5 so they're all inflight.
		for range 5 {
			mustPublish(t, b, "orders", "", []byte("x"))
		}
		for range 5 {
			recvInflight(t, ch, time.Second)
		}

		// Ack only 0 and 2 — lastAcked should be 0, aboveLast = {2}.
		if err := back(b, "orders", "c1", 0); err != nil {
			t.Fatalf("Ack 0: %v", err)
		}
		if err := back(b, "orders", "c1", 2); err != nil {
			t.Fatalf("Ack 2: %v", err)
		}

		cancel()
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen and verify aboveLast was restored.
	{
		b, err := New(dir, testDedupeCap)
		if err != nil {
			t.Fatalf("Reopen: %v", err)
		}
		t.Cleanup(func() { b.Close() })

		topic, _ := b.getOrCreateTopic("orders")
		c := part0(topic).getOrCreateConsumer("c1")
		c.mu.Lock()
		lastAcked := c.lastAcked
		_, hasTwo := c.aboveLast[2]
		size := len(c.aboveLast)
		c.mu.Unlock()

		if lastAcked != 0 {
			t.Errorf("lastAcked after reopen = %d, want 0", lastAcked)
		}
		if !hasTwo || size != 1 {
			t.Errorf("aboveLast = %d entries, want {2}", size)
		}
	}
}

// TestDedupeIndexRebuiltFromWALOnRestart is the core M1 unit guarantee:
// after a broker restart over the same data dir, the dedupe index is
// repopulated from the WAL before any new publish, so a re-published
// key returns the original MsgID with no new WAL append. See ADR 0018.
func TestDedupeIndexRebuiltFromWALOnRestart(t *testing.T) {
	dir := t.TempDir()

	b1, err := New(dir, testDedupeCap)
	if err != nil {
		t.Fatalf("New b1: %v", err)
	}
	id1 := mustPublish(t, b1, "orders", "key-1", []byte("one"))
	id2 := mustPublish(t, b1, "orders", "key-2", []byte("two"))
	mustPublish(t, b1, "orders", "", []byte("no-key")) // unkeyed: no dedupe entry
	if err := b1.Close(); err != nil {
		t.Fatalf("Close b1: %v", err)
	}

	b2, err := New(dir, testDedupeCap)
	if err != nil {
		t.Fatalf("New b2: %v", err)
	}
	t.Cleanup(func() { b2.Close() })

	// Index is populated purely from recovery — no publish has happened
	// on b2 yet.
	top, err := b2.getOrCreateTopic("orders")
	if err != nil {
		t.Fatalf("getOrCreateTopic: %v", err)
	}
	if got, ok := part0(top).dedupe.Lookup("key-1"); !ok || got != id1 {
		t.Errorf("Lookup(key-1) = (%d, %v), want (%d, true)", got, ok, id1)
	}
	if got, ok := part0(top).dedupe.Lookup("key-2"); !ok || got != id2 {
		t.Errorf("Lookup(key-2) = (%d, %v), want (%d, true)", got, ok, id2)
	}

	// Re-publishing key-1 must return the original id as a duplicate,
	// with no new WAL record appended.
	gotID, dup, err := bpub(b2, "orders", "key-1", []byte("one-again"))
	if err != nil {
		t.Fatalf("Publish key-1 again: %v", err)
	}
	if !dup || gotID != id1 {
		t.Errorf("re-publish key-1 = (%d, dup=%v), want (%d, dup=true)", gotID, dup, id1)
	}
}

// TestDedupeRebuildRespectsLRUCap proves eviction-by-construction: the
// WAL is replayed in ascending MsgID order, so rebuilding a cap-N index
// from more than N keyed records retains exactly the most-recent N —
// the same set the live LRU would hold. Keys evicted before the restart
// stay evicted after it.
func TestDedupeRebuildRespectsLRUCap(t *testing.T) {
	dir := t.TempDir()
	const dedupeCap = 3

	b1, err := New(dir, dedupeCap)
	if err != nil {
		t.Fatalf("New b1: %v", err)
	}
	// 4 distinct keys into a cap-3 index: key-0 is evicted by key-3.
	for i := 0; i < 4; i++ {
		mustPublish(t, b1, "orders", "key-"+string(rune('0'+i)), []byte("v"))
	}
	if err := b1.Close(); err != nil {
		t.Fatalf("Close b1: %v", err)
	}

	b2, err := New(dir, dedupeCap)
	if err != nil {
		t.Fatalf("New b2: %v", err)
	}
	t.Cleanup(func() { b2.Close() })

	top, err := b2.getOrCreateTopic("orders")
	if err != nil {
		t.Fatalf("getOrCreateTopic: %v", err)
	}
	if _, ok := part0(top).dedupe.Lookup("key-0"); ok {
		t.Errorf("key-0 present after restart; expected it to be evicted (cap=%d)", dedupeCap)
	}
	for i := 1; i < 4; i++ {
		key := "key-" + string(rune('0'+i))
		if _, ok := part0(top).dedupe.Lookup(key); !ok {
			t.Errorf("%s missing after restart; expected retained (cap=%d)", key, dedupeCap)
		}
	}
}

// TestBatchedSyncDeliversEndToEnd checks the broker wires SyncBatched
// into each topic's WAL: a publish under batched mode still gets a real
// MsgID and is delivered to a subscriber (the group committer fsyncs on
// its interval, then delivery proceeds). Uses a short interval so the
// ticker fires quickly.
func TestBatchedSyncDeliversEndToEnd(t *testing.T) {
	sc := SyncConfig{Mode: wal.SyncBatched, Interval: 2 * time.Millisecond}
	b, err := newBroker(t.TempDir(), testDedupeCap, 1, defaultRecvWindow, defaultVisibilityTimeout, 20*time.Millisecond, sc, RetentionConfig{}, 0)
	if err != nil {
		t.Fatalf("newBroker: %v", err)
	}
	t.Cleanup(func() { b.Close() })

	id, dup, err := bpub(b, "orders", "k1", []byte("batched-payload"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if dup {
		t.Fatal("unexpected dup on first publish")
	}

	ch := make(chan *Inflight, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := bsub(b, ctx, "orders", "c1", ch); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	inf := recvInflight(t, ch, 2*time.Second)
	if inf.MsgID != id {
		t.Fatalf("delivered MsgID = %d, want %d", inf.MsgID, id)
	}
	if string(inf.Payload) != "batched-payload" {
		t.Fatalf("payload = %q", inf.Payload)
	}
}
