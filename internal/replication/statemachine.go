package replication

import (
	"context"
	"fmt"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// ApplySurface is the minimal set of deterministic broker mutations the state
// machine drives. The broker satisfies it structurally, so this package never
// imports the broker — keeping raft out of the broker's core files and the
// broker out of replication (no import cycle). Every method is wall-clock-free
// and routing-free: the leader resolved all non-determinism into the Envelope
// before Propose, so running these in raft log-index order yields identical
// state on every node.
type ApplySurface interface {
	// ApplyPublish appends one message to topic/partition using the
	// leader-stamped tsNs / visibleAtNs, returning the WAL-assigned MsgID and
	// whether it was a dedupe hit. raftIndex is the committing entry's log
	// index, stamped into the record so restart replay is idempotent.
	ApplyPublish(ctx context.Context, topic string, partition int, tsNs, visibleAtNs, raftIndex uint64, dedupeKey string, payload []byte) (msgID uint64, dup bool, err error)
	// ApplyAck advances the consumer's durable offset.
	ApplyAck(topic string, partition int, consumerID string, msgID uint64) error
	// ApplyNack bumps attempts / dead-letters. The immediate redelivery push
	// is per-session delivery and does NOT happen here — the leader's
	// redelivery ticker re-pushes the still-inflight message (spec §Command
	// classification; ponytail: no cross-node inflight handle in M1, the
	// ticker is the backstop, tighten if redelivery latency matters).
	ApplyNack(ctx context.Context, topic string, partition int, consumerID string, msgID uint64) error
	// ApplyCreateTopic opens a topic at an exact partition count; idempotent
	// on replay when the count matches.
	ApplyCreateTopic(topic string, partitions int) error
}

// BrokerSM adapts the broker to raft.StateMachine. Apply decodes the Envelope
// in each committed entry, dispatches to the matching deterministic broker
// mutation, and resolves the command's result registry nonce so the proposing
// leader's handler can read back a PUB's MsgID (raft.Node.Propose discards
// Apply's result — see registry.go).
type BrokerSM struct {
	broker ApplySurface
	reg    *ResultRegistry
}

// NewBrokerSM wraps b in a state machine with a fresh result registry. Pass the
// same registry (via Registry) to the broker's AttachRaft so the leader handler
// and Apply share it.
func NewBrokerSM(b ApplySurface) *BrokerSM {
	return &BrokerSM{broker: b, reg: NewResultRegistry()}
}

// Registry returns the SM's result registry so the broker can register waiters
// on the same instance Apply resolves.
func (sm *BrokerSM) Registry() *ResultRegistry {
	return sm.reg
}

// Apply runs one committed command. It is called from toyraft's single apply
// goroutine, in strict index order, so the mutations it drives are serialized
// and deterministic (raft.StateMachine contract).
//
// A decode failure panics: the replicated log is the source of truth, so an
// undecodable entry is unrecoverable corruption, not a per-command reject
// (toyraft recovers the panic into node fatal-status and unblocks Propose with
// the error). A broker mutation error is returned instead — toyraft delivers it
// to the proposing Propose caller without poisoning the node — and the nonce is
// left unresolved; the handler cleans it up via Forget on the Propose error.
func (sm *BrokerSM) Apply(entry raft.Entry) (any, error) {
	env, err := Decode(entry.Data)
	if err != nil {
		panic(fmt.Sprintf("replication: undecodable entry at index %d: %v", entry.Index, err))
	}

	// Apply carries no ctx; tracing degrades to the noop provider.
	ctx := context.Background()

	switch env.Kind {
	case KindPublish:
		id, dup, err := sm.broker.ApplyPublish(ctx, env.Topic, int(env.Partition),
			env.TsNs, env.VisibleAtNs, uint64(entry.Index), env.DedupeKey, env.Payload)
		if err != nil {
			return nil, err
		}
		res := ApplyResult{MsgID: id, Dup: dup}
		sm.reg.resolve(env.Nonce, res)
		return res, nil

	case KindAck:
		if err := sm.broker.ApplyAck(env.Topic, int(env.Partition), env.ConsumerID, env.MsgID); err != nil {
			return nil, err
		}
		sm.reg.resolve(env.Nonce, ApplyResult{})
		return nil, nil

	case KindNack:
		if err := sm.broker.ApplyNack(ctx, env.Topic, int(env.Partition), env.ConsumerID, env.MsgID); err != nil {
			return nil, err
		}
		sm.reg.resolve(env.Nonce, ApplyResult{})
		return nil, nil

	case KindCreateTopic:
		if err := sm.broker.ApplyCreateTopic(env.Topic, int(env.Partitions)); err != nil {
			return nil, err
		}
		sm.reg.resolve(env.Nonce, ApplyResult{})
		return nil, nil

	default:
		// Unreachable: Decode already rejects an unknown Kind.
		panic(fmt.Sprintf("replication: unhandled kind %d at index %d", env.Kind, entry.Index))
	}
}

// Snapshot is an M1 stub: snapshotting is unsupported until v4 (toyraft UP-1).
// rebuildIndexes (internal/broker/partition.go) is the recorded reuse point.
func (sm *BrokerSM) Snapshot() ([]byte, raft.Index, error) {
	return nil, 0, raft.ErrSnapshotUnsupported
}

// Restore is an M1 stub; see Snapshot.
func (sm *BrokerSM) Restore([]byte) error {
	return raft.ErrSnapshotUnsupported
}

// Compile-time assertion that *BrokerSM satisfies the frozen StateMachine
// interface.
var _ raft.StateMachine = (*BrokerSM)(nil)
