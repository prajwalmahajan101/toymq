package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
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
