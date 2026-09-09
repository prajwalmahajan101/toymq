package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// MaxRecordSize caps one wire record at 4 MiB including framing and
// CRC. Decode rejects anything larger up front to avoid a hostile
// length-field allocating gigabytes.
const MaxRecordSize = 4 << 20 // 4 MiB

// Sentinel errors returned by Decode. Distinct types so callers
// (chiefly the recovery loop) can branch on torn-tail vs corruption
// vs adversarial input.
var (
	ErrShortRead = errors.New("wal: short read")
	ErrBadCRC    = errors.New("wal: crc mismatch")
	ErrTooLarge  = errors.New("wal: record too large")
)

// Record is one durable message: a monotonic MsgID, a producer
// timestamp, an optional dedupe key, an opaque payload, and an optional
// visible-at time for delayed delivery. The wire format is documented in
// ADR 0001; VisibleAtNs is an append-only trailing field (ADR 0025).
type Record struct {
	MsgID     uint64
	TsNs      uint64
	DedupeKey string
	Payload   []byte

	// VisibleAtNs is the wall-clock time (unix ns) before which the
	// message must not be delivered — set by the producer/proposer to
	// now+delay for a DELAYed PUB (ADR 0025). 0 means "visible
	// immediately", so every pre-M6 record and every non-delayed PUB is
	// unchanged. It is encoded as an append-only 8-byte field after the
	// payload: records written before M6 carry no trailing bytes and
	// decode to 0.
	VisibleAtNs uint64

	// RaftIndex is the toyraft log index of the entry that produced this
	// record, stamped only on the replicated path (v3 M1, ADR 0028); it is
	// the applied-state durability marker that makes restart replay
	// idempotent — on recovery the broker skips re-applying any PUB whose
	// RaftIndex is at or below the highest recovered value. It is an
	// append-only 8-byte field written ONLY when non-zero: raft indices are
	// 1-based, so a standalone record (RaftIndex 0) omits it entirely and
	// stays byte-for-byte identical to v2. A replicated record carries both
	// trailing 8-byte fields (VisibleAtNs then RaftIndex).
	RaftIndex uint64
}

// Encode writes one framed Record to dst. Returns ErrTooLarge if the
// resulting frame would exceed MaxRecordSize or the dedupe key
// overflows the uint16 length prefix.
func Encode(rec Record, dst *bytes.Buffer) error {
	if len(rec.DedupeKey) > 65535 {
		return ErrTooLarge
	}
	var inner bytes.Buffer
	var scratch [8]byte

	binary.LittleEndian.PutUint64(scratch[:], rec.MsgID)
	inner.Write(scratch[:])

	binary.LittleEndian.PutUint64(scratch[:], rec.TsNs)
	inner.Write(scratch[:])

	binary.LittleEndian.PutUint16(scratch[:2], uint16(len(rec.DedupeKey)))
	inner.Write(scratch[:2])

	inner.WriteString(rec.DedupeKey)

	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(rec.Payload)))
	inner.Write(scratch[:4])

	inner.Write(rec.Payload)

	// Append-only VisibleAtNs (ADR 0025): always written for records
	// encoded at M6+, so new records carry 8 trailing bytes; pre-M6
	// records have none and decode to VisibleAtNs == 0.
	binary.LittleEndian.PutUint64(scratch[:], rec.VisibleAtNs)
	inner.Write(scratch[:])

	// Append-only RaftIndex (ADR 0028): written ONLY when non-zero, after
	// VisibleAtNs. Standalone records (RaftIndex 0) omit it and stay
	// byte-identical to v2; replicated records carry a second 8-byte field.
	if rec.RaftIndex != 0 {
		binary.LittleEndian.PutUint64(scratch[:], rec.RaftIndex)
		inner.Write(scratch[:])
	}

	if inner.Len()+4 > MaxRecordSize {
		return ErrTooLarge
	}

	crc := crc32.ChecksumIEEE(inner.Bytes())

	length := uint32(inner.Len() + 4)
	binary.LittleEndian.PutUint32(scratch[:4], length)
	dst.Write(scratch[:4])

	dst.Write(inner.Bytes())

	binary.LittleEndian.PutUint32(scratch[:4], crc)
	dst.Write(scratch[:4])

	return nil
}

// Decode reads one framed Record from r and returns the Record, the
// total number of bytes consumed, and any error. A clean io.EOF means
// the stream ended on a frame boundary; ErrShortRead means a torn
// trailing write; ErrBadCRC means corruption.
func Decode(r *bufio.Reader) (Record, int, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, 0, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, 0, ErrShortRead
		}
		return Record{}, 0, err
	}

	length := binary.LittleEndian.Uint32(lenBuf[:])

	if length > MaxRecordSize {
		return Record{}, 0, ErrTooLarge
	}

	if length < 4 {
		return Record{}, 0, ErrShortRead
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Record{}, 0, ErrShortRead
	}

	inner := body[:len(body)-4]
	gotCRC := binary.LittleEndian.Uint32(body[len(body)-4:])
	wantCRC := crc32.ChecksumIEEE(inner)

	if gotCRC != wantCRC {
		return Record{}, 0, ErrBadCRC
	}

	if len(inner) < 8+8+2+4 {
		return Record{}, 0, ErrShortRead
	}

	var rec Record

	off := 0

	rec.MsgID = binary.LittleEndian.Uint64(inner[off:])
	off += 8

	rec.TsNs = binary.LittleEndian.Uint64(inner[off:])
	off += 8

	keyLen := int(binary.LittleEndian.Uint16(inner[off:]))
	off += 2

	if off+keyLen > len(inner) {
		return Record{}, 0, ErrShortRead
	}

	rec.DedupeKey = string(inner[off : off+keyLen])

	off += keyLen

	if off+4 > len(inner) {
		return Record{}, 0, ErrShortRead
	}

	payloadLen := int(binary.LittleEndian.Uint32(inner[off:]))
	off += 4

	if off+payloadLen > len(inner) {
		return Record{}, 0, ErrShortRead
	}

	rec.Payload = append([]byte(nil), inner[off:off+payloadLen]...)
	off += payloadLen

	// Append-only trailing fields:
	//   0 bytes  → pre-M6 record: VisibleAtNs 0, RaftIndex 0.
	//   8 bytes  → M6+ standalone record: VisibleAtNs, RaftIndex 0 (ADR 0025).
	//  16 bytes  → v3 replicated record: VisibleAtNs then RaftIndex (ADR 0028).
	// Anything else is a malformed frame.
	switch len(inner) - off {
	case 0:
		rec.VisibleAtNs = 0
		rec.RaftIndex = 0
	case 8:
		rec.VisibleAtNs = binary.LittleEndian.Uint64(inner[off:])
		rec.RaftIndex = 0
	case 16:
		rec.VisibleAtNs = binary.LittleEndian.Uint64(inner[off:])
		rec.RaftIndex = binary.LittleEndian.Uint64(inner[off+8:])
	default:
		return Record{}, 0, ErrShortRead
	}

	n := 4 + int(length)

	return rec, n, nil
}
