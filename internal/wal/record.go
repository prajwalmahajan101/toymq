package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const MaxRecordSize = 4 << 20 // 4 MiB

var (
	ErrShortRead = errors.New("wal: short read")
	ErrBadCRC    = errors.New("wal: crc mismatch")
	ErrTooLarge  = errors.New("wal: record too large")
)

type Record struct {
	MsgID     uint64
	TsNs      uint64
	DedupeKey string
	Payload   []byte
}

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

	if off+payloadLen != len(inner) {
		return Record{}, 0, ErrShortRead
	}

	rec.Payload = append([]byte(nil), inner[off:off+payloadLen]...)

	n := 4 + int(length)

	return rec, n, nil
}
