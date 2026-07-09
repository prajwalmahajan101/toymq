package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// TestM8CrossProductMatrix is the v2 M8 owned risk test: it proves the
// v2 feature surface composes by exercising every cell of
//
//	{per-message, batched} × {plain, TLS} × {auth, no-auth} × {1, 4 partitions}
//
// end-to-end through pkg/client. For each cell it publishes nMsgs keyless
// messages (which round-robin across partitions), fans them back in over a
// "<topic>#*" subscription, and asserts exactly-once delivery, per-partition
// MsgID monotonicity, and the expected total. Runs under -race in CI.
func TestM8CrossProductMatrix(t *testing.T) {
	t.Parallel()

	const (
		goodToken = "m8-secret-token"
		topic     = "orders"
		nMsgs     = 40
	)

	syncModes := []struct {
		name string
		mode wal.SyncMode
	}{
		{"per-message", wal.SyncPerMessage},
		{"batched", wal.SyncBatched},
	}
	tlsModes := []struct {
		name string
		on   bool
	}{{"plain", false}, {"tls", true}}
	authModes := []struct {
		name string
		on   bool
	}{{"no-auth", false}, {"auth", true}}
	partCounts := []int{1, 4}

	for _, sm := range syncModes {
		for _, tm := range tlsModes {
			for _, am := range authModes {
				for _, parts := range partCounts {
					name := fmt.Sprintf("%s/%s/%s/p%d", sm.name, tm.name, am.name, parts)
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						runMatrixCell(t, matrixCell{
							syncMode:   sm.mode,
							useTLS:     tm.on,
							useAuth:    am.on,
							partitions: parts,
							token:      goodToken,
							topic:      topic,
							nMsgs:      nMsgs,
						})
					})
				}
			}
		}
	}
}

type matrixCell struct {
	syncMode   wal.SyncMode
	useTLS     bool
	useAuth    bool
	partitions int
	token      string
	topic      string
	nMsgs      int
}

func runMatrixCell(t *testing.T, c matrixCell) {
	t.Helper()

	// Long visibility so nothing redelivers during the read loop; harness
	// opts assembled from the cell's four axes.
	hopts := []harnessOpt{
		withVisibility(5 * time.Second),
		withDefaultPartitions(c.partitions),
	}
	if c.syncMode == wal.SyncBatched {
		hopts = append(hopts, withSyncMode(wal.SyncBatched, time.Millisecond))
	}
	if c.useAuth {
		hopts = append(hopts, withAuth([]string{c.token}))
	}
	if c.useTLS {
		hopts = append(hopts, withTLS())
	}
	h := startBroker(t, hopts...)

	dialOpts := func() []client.Option {
		var opts []client.Option
		if c.useAuth {
			opts = append(opts, client.WithAuth(c.token))
		}
		if c.useTLS {
			opts = append(opts, client.WithTLS(h.cliTLS))
		}
		return opts
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Produce: nMsgs keyless publishes round-robin across the partitions.
	prod, err := client.Dial(ctx, h.addr, dialOpts()...)
	if err != nil {
		t.Fatalf("dial producer: %v", err)
	}
	for i := 0; i < c.nMsgs; i++ {
		if _, _, perr := prod.Pub(ctx, c.topic, "", "", fmt.Appendf(nil, "m%d", i)); perr != nil {
			t.Fatalf("pub %d: %v", i, perr)
		}
	}
	_ = prod.Close()

	// Consume: fan-in every partition and assert exactly-once + monotonicity.
	sub, err := client.Dial(ctx, h.addr, dialOpts()...)
	if err != nil {
		t.Fatalf("dial consumer: %v", err)
	}
	defer sub.Close()

	ch, err := sub.Sub(ctx, c.topic+"#*", "c-all")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}

	type key struct {
		partition int
		msgID     uint64
	}
	seen := make(map[key]struct{}, c.nMsgs)
	lastID := make(map[int]uint64) // partition -> last delivered msgID
	haveLast := make(map[int]bool)

	for received := 0; received < c.nMsgs; received++ {
		select {
		case d, ok := <-ch:
			if !ok {
				t.Fatalf("delivery channel closed after %d/%d messages", received, c.nMsgs)
			}
			if d.Partition < 0 || d.Partition >= c.partitions {
				t.Fatalf("delivery on partition %d, want [0,%d)", d.Partition, c.partitions)
			}
			k := key{d.Partition, d.MsgID}
			if _, dup := seen[k]; dup {
				t.Fatalf("duplicate delivery of partition %d msgID %d", d.Partition, d.MsgID)
			}
			seen[k] = struct{}{}
			if haveLast[d.Partition] && d.MsgID <= lastID[d.Partition] {
				t.Fatalf("partition %d msgID not increasing: got %d after %d", d.Partition, d.MsgID, lastID[d.Partition])
			}
			lastID[d.Partition] = d.MsgID
			haveLast[d.Partition] = true
			if aerr := d.Ack(ctx); aerr != nil {
				t.Fatalf("ack partition %d msgID %d: %v", d.Partition, d.MsgID, aerr)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d/%d messages", received, c.nMsgs)
		}
	}

	if len(seen) != c.nMsgs {
		t.Fatalf("saw %d distinct deliveries, want %d", len(seen), c.nMsgs)
	}
}
