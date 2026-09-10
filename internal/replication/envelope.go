// Package replication adapts the broker's deterministic mutation methods to
// toyraft's replicated log. It owns the command envelope that travels in a
// raft.Entry, the raft.StateMachine that applies those entries on every node,
// and the nonce registry that returns a PUB's WAL-assigned MsgID to the
// leader's request handler (raft.Node.Propose discards the Apply result, so
// the result has to come back out of band — see registry.go).
//
// The package deliberately keeps raft and broker types apart: the state
// machine consumes a small ApplySurface interface (statemachine.go) that the
// broker satisfies, so importing replication does not drag raft into the
// broker's core files.
package replication

import (
	"encoding/binary"
	"errors"
)

// EnvelopeVersion is the wire version stamped as the first byte of every
// encoded Envelope. Bump it only for an incompatible layout change; Decode
// rejects any other value so a mixed-version cluster fails loudly instead of
// misreading fields.
const EnvelopeVersion uint8 = 1

// maxEnvelopeSize caps one encoded envelope, mirroring wal.MaxRecordSize (4
// MiB). Decode rejects a larger length prefix up front so a corrupt or
// hostile frame cannot drive a giant allocation.
const maxEnvelopeSize = 4 << 20

// Kind discriminates the replicated mutating commands. Values are wire-visible
// and append-only — never renumber.
type Kind uint8

const (
	KindPublish Kind = iota
	KindAck
	KindNack
	KindCreateTopic
)

// Sentinel decode errors. Callers (the state machine) treat any of these as a
// fatal, non-retryable corruption of the replicated log.
var (
	ErrShortRead       = errors.New("replication: short read")
	ErrBadVersion      = errors.New("replication: unknown envelope version")
	ErrBadKind         = errors.New("replication: unknown command kind")
	ErrTooLarge        = errors.New("replication: envelope too large")
	ErrTrailingGarbage = errors.New("replication: trailing bytes after envelope")
)

// Envelope is the deterministic description of one mutating command. The
// leader stamps every non-deterministic value (Partition after routing, TsNs
// from the clock, VisibleAtNs from delayMs) before Propose, so Apply is pure:
// running the same envelope on every node produces byte-identical state.
//
// Not every field is meaningful for every Kind — Payload/VisibleAtNs are
// publish-only, ConsumerID/MsgID are ack/nack-only, Partitions is
// create-only. Unused fields are zero and cost 8 bytes each on the wire; the
// schema stays fixed for simplicity rather than per-kind-packed.
type Envelope struct {
	Kind        Kind
	Topic       string
	Partition   int32 // leader-resolved (replaces Topic.rr non-determinism)
	DedupeKey   string
	TsNs        uint64 // leader-stamped clock
	VisibleAtNs uint64 // leader-resolved from delayMs (publish)
	ConsumerID  string // ack / nack
	MsgID       uint64 // ack / nack target
	Payload     []byte // publish
	Partitions  int32  // create-topic
}

// Encode serializes env to a fresh byte slice: a version byte, the kind byte,
// then every field little-endian with uint32 length prefixes on the three
// strings and the payload. Layout mirrors internal/wal's fixed-field style but
// carries no CRC — a raft.Entry is already checksummed by pkg/storage, and the
// envelope only ever lives inside Entry.Data.
func Encode(env Envelope) []byte {
	var b []byte
	b = append(b, EnvelopeVersion, byte(env.Kind))

	var scratch [8]byte
	putU64 := func(v uint64) {
		binary.LittleEndian.PutUint64(scratch[:], v)
		b = append(b, scratch[:]...)
	}
	putU32 := func(v uint32) {
		binary.LittleEndian.PutUint32(scratch[:4], v)
		b = append(b, scratch[:4]...)
	}
	putStr := func(s string) {
		putU32(uint32(len(s)))
		b = append(b, s...)
	}

	putStr(env.Topic)
	putU32(uint32(env.Partition))
	putStr(env.DedupeKey)
	putU64(env.TsNs)
	putU64(env.VisibleAtNs)
	putStr(env.ConsumerID)
	putU64(env.MsgID)
	putU32(uint32(len(env.Payload)))
	b = append(b, env.Payload...)
	putU32(uint32(env.Partitions))

	return b
}

// Decode parses one Envelope from data, which must be exactly one encoded
// envelope (no framing length prefix — the raft entry already delimits it).
// It rejects an unknown version/kind, a length field that runs past the
// buffer, and any trailing bytes.
func Decode(data []byte) (Envelope, error) {
	if len(data) > maxEnvelopeSize {
		return Envelope{}, ErrTooLarge
	}
	// version(1) + kind(1) + topicLen(4) + partition(4) + dedupeLen(4) +
	// tsNs(8) + visibleAt(8) + consumerLen(4) + msgID(8) + payloadLen(4) +
	// partitions(4) = 50 bytes of fixed framing minimum (all three strings
	// and the payload empty).
	if len(data) < 50 {
		return Envelope{}, ErrShortRead
	}

	off := 0
	if data[off] != EnvelopeVersion {
		return Envelope{}, ErrBadVersion
	}
	off++

	kind := Kind(data[off])
	if kind > KindCreateTopic {
		return Envelope{}, ErrBadKind
	}
	off++

	var env Envelope
	env.Kind = kind

	u64 := func() (uint64, bool) {
		if off+8 > len(data) {
			return 0, false
		}
		v := binary.LittleEndian.Uint64(data[off:])
		off += 8
		return v, true
	}
	u32 := func() (uint32, bool) {
		if off+4 > len(data) {
			return 0, false
		}
		v := binary.LittleEndian.Uint32(data[off:])
		off += 4
		return v, true
	}
	str := func() (string, bool) {
		n, ok := u32()
		if !ok || off+int(n) > len(data) {
			return "", false
		}
		s := string(data[off : off+int(n)])
		off += int(n)
		return s, true
	}
	bytesField := func() ([]byte, bool) {
		n, ok := u32()
		if !ok || off+int(n) > len(data) {
			return nil, false
		}
		p := append([]byte(nil), data[off:off+int(n)]...)
		off += int(n)
		return p, true
	}

	var ok bool
	var p uint32
	if env.Topic, ok = str(); !ok {
		return Envelope{}, ErrShortRead
	}
	if p, ok = u32(); !ok {
		return Envelope{}, ErrShortRead
	}
	env.Partition = int32(p)
	if env.DedupeKey, ok = str(); !ok {
		return Envelope{}, ErrShortRead
	}
	if env.TsNs, ok = u64(); !ok {
		return Envelope{}, ErrShortRead
	}
	if env.VisibleAtNs, ok = u64(); !ok {
		return Envelope{}, ErrShortRead
	}
	if env.ConsumerID, ok = str(); !ok {
		return Envelope{}, ErrShortRead
	}
	if env.MsgID, ok = u64(); !ok {
		return Envelope{}, ErrShortRead
	}
	if env.Payload, ok = bytesField(); !ok {
		return Envelope{}, ErrShortRead
	}
	if p, ok = u32(); !ok {
		return Envelope{}, ErrShortRead
	}
	env.Partitions = int32(p)

	if off != len(data) {
		return Envelope{}, ErrTrailingGarbage
	}

	return env, nil
}
