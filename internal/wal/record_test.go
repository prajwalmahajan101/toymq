package wal

import (
	"bufio"
	"bytes"
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
