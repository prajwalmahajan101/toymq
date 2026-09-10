package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// fakeBroker records the calls Apply dispatches and lets a test inject an
// error, standing in for the real broker's ApplySurface.
type fakeBroker struct {
	calls   []string
	nextID  uint64
	nextDup bool
	err     error
}

func (f *fakeBroker) ApplyPublish(_ context.Context, topic string, partition int, tsNs, visibleAtNs, raftIndex uint64, dedupeKey string, payload []byte) (uint64, bool, error) {
	f.calls = append(f.calls, "publish:"+topic)
	if f.err != nil {
		return 0, false, f.err
	}
	return f.nextID, f.nextDup, nil
}
func (f *fakeBroker) ApplyAck(topic string, partition int, consumerID string, msgID uint64) error {
	f.calls = append(f.calls, "ack:"+topic)
	return f.err
}
func (f *fakeBroker) ApplyNack(_ context.Context, topic string, partition int, consumerID string, msgID uint64) error {
	f.calls = append(f.calls, "nack:"+topic)
	return f.err
}
func (f *fakeBroker) ApplyCreateTopic(topic string, partitions int) error {
	f.calls = append(f.calls, "create:"+topic)
	return f.err
}

func entryOf(env Envelope) raft.Entry {
	return raft.Entry{Index: 1, Term: 1, Data: Encode(env)}
}

func TestApplyPublishReturnsResult(t *testing.T) {
	f := &fakeBroker{nextID: 42, nextDup: true}
	sm := NewBrokerSM(f)

	// Apply's return value is what toyraft rc.3 hands back through Propose.
	res, err := sm.Apply(entryOf(Envelope{Kind: KindPublish, Topic: "orders", Partition: 2}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := res.(ApplyResult); got.MsgID != 42 || !got.Dup {
		t.Fatalf("Apply result = %+v, want {42 true}", got)
	}
	if len(f.calls) != 1 || f.calls[0] != "publish:orders" {
		t.Fatalf("calls = %v, want [publish:orders]", f.calls)
	}
}

func TestApplyDispatchesEachKind(t *testing.T) {
	f := &fakeBroker{}
	sm := NewBrokerSM(f)
	// The result-less commands return a nil Apply result.
	for _, env := range []Envelope{
		{Kind: KindAck, Topic: "t", ConsumerID: "c", MsgID: 1},
		{Kind: KindNack, Topic: "t", ConsumerID: "c", MsgID: 2},
		{Kind: KindCreateTopic, Topic: "t", Partitions: 4},
	} {
		res, err := sm.Apply(entryOf(env))
		if err != nil {
			t.Fatalf("Apply(%v): %v", env.Kind, err)
		}
		if res != nil {
			t.Fatalf("Apply(%v) result = %v, want nil", env.Kind, res)
		}
	}
	want := []string{"ack:t", "nack:t", "create:t"}
	if len(f.calls) != 3 || f.calls[0] != want[0] || f.calls[1] != want[1] || f.calls[2] != want[2] {
		t.Fatalf("calls = %v, want %v", f.calls, want)
	}
}

func TestApplyReturnsBrokerError(t *testing.T) {
	sentinel := errors.New("wal append failed")
	f := &fakeBroker{err: sentinel}
	sm := NewBrokerSM(f)

	res, err := sm.Apply(entryOf(Envelope{Kind: KindPublish, Topic: "orders"}))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if res != nil {
		t.Fatalf("result = %v on error, want nil", res)
	}
}

func TestApplyPanicsOnUndecodableEntry(t *testing.T) {
	sm := NewBrokerSM(&fakeBroker{})
	defer func() {
		if recover() == nil {
			t.Fatal("Apply did not panic on undecodable entry")
		}
	}()
	sm.Apply(raft.Entry{Index: 5, Term: 1, Data: []byte("garbage")})
}

func TestSnapshotRestoreStubbed(t *testing.T) {
	sm := NewBrokerSM(&fakeBroker{})
	if _, _, err := sm.Snapshot(); !errors.Is(err, raft.ErrSnapshotUnsupported) {
		t.Fatalf("Snapshot err = %v, want ErrSnapshotUnsupported", err)
	}
	if err := sm.Restore(nil); !errors.Is(err, raft.ErrSnapshotUnsupported) {
		t.Fatalf("Restore err = %v, want ErrSnapshotUnsupported", err)
	}
}
