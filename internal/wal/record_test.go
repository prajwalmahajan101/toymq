package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestRoundTripSimple(t *testing.T) {
	rec := Record{
		MsgID:     42,
		TsNs:      1_700_000_000_000_000_000,
		DedupeKey: "order-123",
		Payload:   []byte("Hello World!"),
	}

	var buf bytes.Buffer
	if err := Encode(rec, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	totalSize := buf.Len()

	got, n, err := Decode(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if n != totalSize {
		t.Errorf("n: got %d, want %d", n, totalSize)
	}

	if got.MsgID != rec.MsgID {
		t.Errorf("MsgID: got %d, want %d", got.MsgID, rec.MsgID)
	}

	if got.TsNs != rec.TsNs {
		t.Errorf("TsNs: got %d, want %d", got.TsNs, rec.TsNs)
	}

	if got.DedupeKey != rec.DedupeKey {
		t.Errorf("DedupeKey: got %q, want %q", got.DedupeKey, rec.DedupeKey)
	}

	if !bytes.Equal(got.Payload, rec.Payload) {
		t.Errorf("Payload: got %q, want %q", got.Payload, rec.Payload)
	}
}

func TestRoundTripSizes(t *testing.T) {
	cases := []struct {
		name      string
		keyLen    int
		pyloadLen int
	}{
		{"empty key, empty payload", 0, 0},
		{"empty key, 1 byte", 0, 1},
		{"key, 256 bytes", 8, 256},
		{"key, 1 KiB", 8, 1024},
		{"key, near max", 8, MaxRecordSize - 64},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := Record{
				MsgID:     1,
				TsNs:      2,
				DedupeKey: strings.Repeat("k", tc.keyLen),
				Payload:   bytes.Repeat([]byte{0xAB}, tc.pyloadLen),
			}
			var buf bytes.Buffer
			if err := Encode(rec, &buf); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, _, err := Decode(bufio.NewReader(&buf))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.DedupeKey != rec.DedupeKey {
				t.Errorf("key mismatch")
			}
			if !bytes.Equal(got.Payload, rec.Payload) {
				t.Errorf("payload mismatch (len got=%d want=%d)", len(got.Payload), len(rec.Payload))
			}
		})
	}
}

func TestDecodeBadCRC(t *testing.T) {
	rec := Record{MsgID: 1, Payload: []byte("payload-bytes")}

	var buf bytes.Buffer
	if err := Encode(rec, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	raw := buf.Bytes()
	// Flip one bit somewhere inside the payload — after the 4-byte length
	// header, well clear of the CRC at the end.
	raw[10] ^= 0xFF

	_, _, err := Decode(bufio.NewReader(bytes.NewReader(raw)))
	if !errors.Is(err, ErrBadCRC) {
		t.Fatalf("got err=%v, want ErrBadCRC", err)
	}
}

func TestDecodeShortRead(t *testing.T) {
	rec := Record{MsgID: 1, Payload: []byte("some payload here")}

	var buf bytes.Buffer
	if err := Encode(rec, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	raw := buf.Bytes()
	truncated := raw[:len(raw)-7] // chop last 7 bytes (partial CRC + tail)

	_, _, err := Decode(bufio.NewReader(bytes.NewReader(truncated)))
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("got err=%v, want ErrShortRead", err)
	}
}

func TestDecodeCleanEOF(t *testing.T) {
	_, _, err := Decode(bufio.NewReader(bytes.NewReader(nil)))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("got err=%v, want io.EOF", err)
	}
}

func TestDecodeTooLarge(t *testing.T) {
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], MaxRecordSize+1)

	_, _, err := Decode(bufio.NewReader(bytes.NewReader(hdr[:])))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got err=%v, want ErrTooLarge", err)
	}
}
